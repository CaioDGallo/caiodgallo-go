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
	RetryCircuitBreaker     *gobreaker.CircuitBreaker[[]byte]
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

func NewWorkerPools(db *sql.DB, defaultEndpoint, fallbackEndpoint string, defaultFee, fallbackFee float64, httpClient *http.Client, defaultCB, fallbackCB, retryCB *gobreaker.CircuitBreaker[[]byte]) (*WorkerPools, error) {
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

	retryPool, err := ants.NewPool(150, ants.WithOptions(ants.Options{
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
		PaymentTaskChannel:     make(chan types.PaymentTask, 1200),
		DB:                     db,
		DefaultEndpoint:        defaultEndpoint,
		FallbackEndpoint:       fallbackEndpoint,
		DefaultFee:             defaultFee,
		FallbackFee:            fallbackFee,
		Client:                 httpClient,
		DefaultCircuitBreaker:  defaultCB,
		FallbackCircuitBreaker: fallbackCB,
		RetryCircuitBreaker:    retryCB,
		ctx:                    ctx,
		cancel:                 cancel,
	}

	return wp, nil
}

func (wp *WorkerPools) SetCircuitBreakers(defaultCB, fallbackCB, retryCB *gobreaker.CircuitBreaker[[]byte]) {
	wp.DefaultCircuitBreaker = defaultCB
	wp.FallbackCircuitBreaker = fallbackCB
	wp.RetryCircuitBreaker = retryCB
	wp.lastDefaultState = defaultCB.State()
	wp.lastFallbackState = fallbackCB.State()
}

func (wp *WorkerPools) TriggerRetries() {
	if wp.DefaultCircuitBreaker == nil || wp.FallbackCircuitBreaker == nil {
		return
	}
	log.Printf("Circuit breaker became available - triggering immediate retry burst")
	wp.processFailedPayments()
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
	for range 150 {
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
						wp.ProcessPaymentDirect(task, false)
					}()
				}
			}
		}()
	}
}

func (wp *WorkerPools) StartRetryWorker() {
	go func() {
		instanceID := os.Getenv("INSTANCE_ID")
		if instanceID == "2" {
			return
		}

		ticker := time.NewTicker(3000 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-wp.ctx.Done():
				return
			case <-ticker.C:
				wp.TriggerRetries()
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
