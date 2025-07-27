package workers

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/sony/gobreaker/v2"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
)

type WorkerPools struct {
	ProcessingPool          *ants.Pool
	RetryPool               *ants.Pool
	PaymentTaskChannel      chan types.PaymentTask
	DB                      *sql.DB
	DefaultEndpoint         string
	FallbackEndpoint        string
	DefaultFee              float64
	FallbackFee             float64
	Client                  *http.Client
	DefaultCircuitBreaker   *gobreaker.CircuitBreaker[[]byte]
	FallbackCircuitBreaker  *gobreaker.CircuitBreaker[[]byte]
	wg                      sync.WaitGroup
	ctx                     context.Context
	cancel                  context.CancelFunc
	lastDefaultState        gobreaker.State
	lastFallbackState       gobreaker.State
	stateMutex              sync.RWMutex
	defaultHealth           types.ProcessorHealth
	fallbackHealth          types.ProcessorHealth
	healthMutex             sync.RWMutex
	lastDefaultHealthCheck  time.Time
	lastFallbackHealthCheck time.Time
}

func NewWorkerPools(db *sql.DB, defaultEndpoint, fallbackEndpoint string, defaultFee, fallbackFee float64, httpClient *http.Client, defaultCB, fallbackCB *gobreaker.CircuitBreaker[[]byte]) (*WorkerPools, error) {
	ctx, cancel := context.WithCancel(context.Background())

	processingPool, err := ants.NewPool(150, ants.WithOptions(ants.Options{
		ExpiryDuration: 30 * time.Second,
		Nonblocking:    false,
		PanicHandler: func(i interface{}) {
			log.Printf("Payment processing worker panic: %v", i)
		},
	}))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create processing pool: %v", err)
	}

	retryPool, err := ants.NewPool(0, ants.WithOptions(ants.Options{
		ExpiryDuration: 60 * time.Second,
		Nonblocking:    false,
		PanicHandler: func(i interface{}) {
			log.Printf("Payment retry worker panic: %v", i)
		},
	}))
	if err != nil {
		processingPool.Release()
		cancel()
		return nil, fmt.Errorf("failed to create retry pool: %v", err)
	}

	wp := &WorkerPools{
		ProcessingPool:         processingPool,
		RetryPool:              retryPool,
		PaymentTaskChannel:     make(chan types.PaymentTask, 1000),
		DB:                     db,
		DefaultEndpoint:        defaultEndpoint,
		FallbackEndpoint:       fallbackEndpoint,
		DefaultFee:             defaultFee,
		FallbackFee:            fallbackFee,
		Client:                 httpClient,
		DefaultCircuitBreaker:  defaultCB,
		FallbackCircuitBreaker: fallbackCB,
		ctx:                    ctx,
		cancel:                 cancel,
		lastDefaultState:       defaultCB.State(),
		lastFallbackState:      fallbackCB.State(),
	}

	return wp, nil
}

func (wp *WorkerPools) Shutdown(timeout time.Duration) error {
	log.Println("Starting graceful shutdown of worker pools...")

	wp.cancel()
	close(wp.PaymentTaskChannel)

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All worker tasks completed successfully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown")
	}

	wp.ProcessingPool.Release()
	wp.RetryPool.Release()

	log.Println("Worker pools shutdown complete")
	return nil
}

func (wp *WorkerPools) StartPaymentConsumers() {
	for i := 0; i < 150; i++ {
		go func() {
			for {
				select {
				case <-wp.ctx.Done():
					return
				case task, ok := <-wp.PaymentTaskChannel:
					if !ok {
						return
					}
					wp.wg.Add(1)
					func() {
						defer wp.wg.Done()
						wp.ProcessPaymentDirect(task)
					}()
				}
			}
		}()
	}
}

func (wp *WorkerPools) StartRetryWorker() {
	go func() {
		instanceID := os.Getenv("INSTANCE_ID")
		var staggerDelay time.Duration
		if instanceID == "2" {
			staggerDelay = 1 * time.Second
		}

		if staggerDelay > 0 {
			time.Sleep(staggerDelay)
		}

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-wp.ctx.Done():
				return
			case <-ticker.C:
				wp.checkCircuitBreakerStates()
				wp.processFailedPayments()
			}
		}
	}()
}

func (wp *WorkerPools) updateProcessorHealth(processorType string, health types.ProcessorHealth) {
	wp.healthMutex.Lock()
	defer wp.healthMutex.Unlock()

	if processorType == "default" {
		wp.defaultHealth = health
	} else {
		wp.fallbackHealth = health
	}
}

func (wp *WorkerPools) getProcessorHealth(processorType string) types.ProcessorHealth {
	wp.healthMutex.RLock()
	defer wp.healthMutex.RUnlock()

	if processorType == "default" {
		return wp.defaultHealth
	}
	return wp.fallbackHealth
}

func (wp *WorkerPools) checkCircuitBreakerStates() {
	wp.stateMutex.Lock()
	defer wp.stateMutex.Unlock()

	currentDefaultState := wp.DefaultCircuitBreaker.State()
	currentFallbackState := wp.FallbackCircuitBreaker.State()

	defaultStateChanged := currentDefaultState != wp.lastDefaultState
	fallbackStateChanged := currentFallbackState != wp.lastFallbackState

	defaultBecameAvailable := defaultStateChanged &&
		(wp.lastDefaultState == gobreaker.StateOpen || wp.lastDefaultState == gobreaker.StateHalfOpen) &&
		currentDefaultState == gobreaker.StateClosed

	fallbackBecameAvailable := fallbackStateChanged &&
		(wp.lastFallbackState == gobreaker.StateOpen || wp.lastFallbackState == gobreaker.StateHalfOpen) &&
		currentFallbackState == gobreaker.StateClosed

	if defaultBecameAvailable || fallbackBecameAvailable {
		log.Printf("Circuit breaker became available - triggering immediate retry burst")
		go wp.processFailedPayments()
	}

	wp.lastDefaultState = currentDefaultState
	wp.lastFallbackState = currentFallbackState
}

