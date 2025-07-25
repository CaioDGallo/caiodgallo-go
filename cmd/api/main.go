package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"

	"github.com/gofiber/fiber/v3"
	"github.com/sony/gobreaker/v2"
)

const (
	insertPayment = "INSERT INTO payment_log (idempotency_key, payment_processor, amount, fee, requested_at, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING"
	updatePayment = "UPDATE payment_log SET status = $1 WHERE idempotency_key = $2"
	statsBoth     = "SELECT COALESCE(payment_processor, 'default') as processor, COUNT(*) as count, COALESCE(SUM(amount), 0) as total_amount FROM payment_log WHERE status = 'success' AND requested_at >= $1 AND requested_at <= $2 GROUP BY payment_processor"
	statsFrom     = "SELECT COALESCE(payment_processor, 'default') as processor, COUNT(*) as count, COALESCE(SUM(amount), 0) as total_amount FROM payment_log WHERE status = 'success' AND requested_at >= $1 GROUP BY payment_processor"
	statsTo       = "SELECT COALESCE(payment_processor, 'default') as processor, COUNT(*) as count, COALESCE(SUM(amount), 0) as total_amount FROM payment_log WHERE status = 'success' AND requested_at <= $1 GROUP BY payment_processor"
	statsAll      = "SELECT COALESCE(payment_processor, 'default') as processor, COUNT(*) as count, COALESCE(SUM(amount), 0) as total_amount FROM payment_log WHERE status = 'success' GROUP BY payment_processor"
)

type AdminTransactionSummary struct {
	TotalRequests     int             `json:"totalRequests"`
	TotalAmount       decimal.Decimal `json:"totalAmount"`
	TotalFee          decimal.Decimal `json:"totalFee"`
	FeePerTransaction decimal.Decimal `json:"feePerTransaction"`
}

type PaymentsSummaryResponse struct {
	Default  PaymentProcessorStats `json:"default"`
	Fallback PaymentProcessorStats `json:"fallback"`
}

type PaymentProcessorStats struct {
	TotalRequests int         `json:"totalRequests"`
	TotalAmount   JSONDecimal `json:"totalAmount"`
}

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentProcessorPaymentRequest struct {
	PaymentRequest
	RequestedAt string `json:"requestedAt"`
}

type JSONDecimal struct {
	decimal.Decimal
}

func (d JSONDecimal) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *JSONDecimal) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)
	dec, err := decimal.NewFromString(str)
	if err != nil {
		return err
	}
	d.Decimal = dec
	return nil
}

func NewJSONDecimal(d decimal.Decimal) JSONDecimal {
	return JSONDecimal{Decimal: d}
}

type PaymentTask struct {
	Request     PaymentRequest
	RequestedAt time.Time
	Fee         float64
	Processor   string
}

type ProcessorHealth struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
	LastChecked     time.Time
	IsValid         bool
}

type WorkerPools struct {
	CreationPool            *ants.Pool
	ProcessingPool          *ants.Pool
	RetryPool               *ants.Pool
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
	defaultHealth           ProcessorHealth
	fallbackHealth          ProcessorHealth
	healthMutex             sync.RWMutex
	lastDefaultHealthCheck  time.Time
	lastFallbackHealthCheck time.Time
}

var (
	client      http.Client
	defaultCB   *gobreaker.CircuitBreaker[[]byte]
	fallbackCB  *gobreaker.CircuitBreaker[[]byte]
	workerPools *WorkerPools
	defaultFee  float64
	fallbackFee float64
)

func NewWorkerPools(db *sql.DB, defaultEndpoint, fallbackEndpoint string, defaultFee, fallbackFee float64, httpClient *http.Client, defaultCB, fallbackCB *gobreaker.CircuitBreaker[[]byte]) (*WorkerPools, error) {
	ctx, cancel := context.WithCancel(context.Background())

	creationPool, err := ants.NewPool(300, ants.WithOptions(ants.Options{
		ExpiryDuration: 15 * time.Second,
		Nonblocking:    true,
		PanicHandler: func(i interface{}) {
			// log.Printf("Payment creation worker panic: %v", i)
		},
	}))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create creation pool: %v", err)
	}

	processingPool, err := ants.NewPool(150, ants.WithOptions(ants.Options{
		ExpiryDuration: 30 * time.Second,
		Nonblocking:    false,
		PanicHandler: func(i interface{}) {
			// log.Printf("Payment processing worker panic: %v", i)
		},
	}))
	if err != nil {
		creationPool.Release()
		cancel()
		return nil, fmt.Errorf("failed to create processing pool: %v", err)
	}

	retryPool, err := ants.NewPool(0, ants.WithOptions(ants.Options{
		ExpiryDuration: 60 * time.Second,
		Nonblocking:    false,
		PanicHandler: func(i interface{}) {
			// log.Printf("Payment retry worker panic: %v", i)
		},
	}))
	if err != nil {
		creationPool.Release()
		processingPool.Release()
		cancel()
		return nil, fmt.Errorf("failed to create retry pool: %v", err)
	}

	wp := &WorkerPools{
		CreationPool:           creationPool,
		ProcessingPool:         processingPool,
		RetryPool:              retryPool,
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
	// log.Println("Starting graceful shutdown of worker pools...")

	wp.cancel()

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// log.Println("All worker tasks completed successfully")
	case <-time.After(timeout):
		// log.Println("Timeout reached, forcing shutdown")
	}

	wp.CreationPool.Release()
	wp.ProcessingPool.Release()
	wp.RetryPool.Release()

	// log.Println("Worker pools shutdown complete")
	return nil
}

func (wp *WorkerPools) ProcessPaymentCreation(task PaymentTask) {
	defer func() {
		if r := recover(); r != nil {
			// log.Printf("Payment creation worker panic recovered: %v", r)
		}
	}()

	var processor string
	var fee float64

	defaultHealth := wp.getProcessorHealth("default")
	fallbackHealth := wp.getProcessorHealth("fallback")

	if (defaultHealth.IsValid && !defaultHealth.Failing) || wp.DefaultCircuitBreaker.State() == gobreaker.StateClosed {
		processor = "default"
		fee = wp.DefaultFee
	} else if (fallbackHealth.IsValid && !fallbackHealth.Failing) || wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		processor = "fallback"
		fee = wp.FallbackFee
	} else {
		processor = "default"
		fee = wp.DefaultFee
	}

	tx, err := wp.DB.Begin()
	if err != nil {
		// log.Printf("Failed to begin transaction for payment %s: %v", task.Request.CorrelationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO payment_log (idempotency_key, payment_processor, amount, fee, requested_at, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING",
		task.Request.CorrelationID, processor, task.Request.Amount, fee, task.RequestedAt, "pending",
	)
	if err != nil {
		// log.Printf("Failed to insert payment %s: %v", task.Request.CorrelationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		// log.Printf("Failed to commit payment creation for %s: %v", task.Request.CorrelationID, err)
		return
	}

	task.Processor = processor
	task.Fee = fee

	wp.wg.Add(1)
	if err := wp.ProcessingPool.Submit(func() {
		defer wp.wg.Done()
		wp.ProcessPaymentDownstream(task)
	}); err != nil {
		wp.wg.Done()
		// log.Printf("Failed to submit payment %s to processing pool: %v", task.Request.CorrelationID, err)
	}
}

func (wp *WorkerPools) ProcessPaymentDownstream(task PaymentTask) {
	defer func() {
		if r := recover(); r != nil {
			// log.Printf("Payment processing worker panic recovered: %v", r)
		}
	}()

	correlationID := task.Request.CorrelationID
	requestedAtString := task.RequestedAt.Format("2006-01-02T15:04:05.000Z")

	tx, err := wp.DB.Begin()
	if err != nil {
		// log.Printf("Failed to begin processing transaction for payment %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"UPDATE payment_log SET status = $1, processing_started_at = $2 WHERE idempotency_key = $3 AND status = 'pending'",
		"processing", time.Now(), correlationID,
	)
	if err != nil {
		// log.Printf("Failed to update payment %s to processing: %v", correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		// log.Printf("Failed to commit processing status for %s: %v", correlationID, err)
		return
	}

	ppPaymentRequest := &PaymentProcessorPaymentRequest{
		PaymentRequest: task.Request,
		RequestedAt:    requestedAtString,
	}

	ppPayload, err := json.Marshal(ppPaymentRequest)
	if err != nil {
		// log.Printf("Error marshaling request for %s: %v", correlationID, err)
		wp.updatePaymentStatus(correlationID, "failed")
		return
	}

	var endpoint string
	var circuitBreaker *gobreaker.CircuitBreaker[[]byte]
	var processor string

	defaultHealth := wp.getProcessorHealth("default")
	fallbackHealth := wp.getProcessorHealth("fallback")

	if (defaultHealth.IsValid && !defaultHealth.Failing) || wp.DefaultCircuitBreaker.State() == gobreaker.StateClosed {
		endpoint = wp.DefaultEndpoint
		circuitBreaker = wp.DefaultCircuitBreaker
		processor = "default"
	} else if (fallbackHealth.IsValid && !fallbackHealth.Failing) || wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		endpoint = wp.FallbackEndpoint
		circuitBreaker = wp.FallbackCircuitBreaker
		processor = "fallback"
	} else {
		endpoint = wp.DefaultEndpoint
		circuitBreaker = wp.DefaultCircuitBreaker
		processor = "default"
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", endpoint), bytes.NewReader(ppPayload))
	if err != nil {
		// log.Printf("Error creating request for %s: %v", correlationID, err)
		wp.updatePaymentStatus(correlationID, "failed")
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(ppPayload)))

	_, err = circuitBreaker.Execute(func() ([]byte, error) {
		resp, err := wp.Client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("downstream error: %d", resp.StatusCode)
		}

		return []byte{}, nil
	})

	if err != nil && processor == "default" && wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		// log.Printf("Default processor failed for %s, trying fallback: %v", correlationID, err)

		req, err = http.NewRequest("POST", fmt.Sprintf("%s/payments", wp.FallbackEndpoint), bytes.NewReader(ppPayload))
		if err != nil {
			// log.Printf("Error creating fallback request for %s: %v", correlationID, err)
			wp.updatePaymentStatus(correlationID, "failed")
			return
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Length", strconv.Itoa(len(ppPayload)))

		_, err = wp.FallbackCircuitBreaker.Execute(func() ([]byte, error) {
			resp, err := wp.Client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				return nil, fmt.Errorf("downstream error: %d", resp.StatusCode)
			}

			return []byte{}, nil
		})

		if err == nil {
			wp.updatePaymentProcessorAndFee(correlationID, "fallback", wp.FallbackFee)
		}
	}

	if err != nil {
		// log.Printf("Payment processing failed for %s: %v", correlationID, err)
		wp.updatePaymentStatus(correlationID, "failed")
		return
	}

	wp.updatePaymentStatus(correlationID, "success")
}

func (wp *WorkerPools) updatePaymentStatus(correlationID, status string) {
	tx, err := wp.DB.Begin()
	if err != nil {
		// log.Printf("CRITICAL: Failed to begin status update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET status = $1 WHERE idempotency_key = $2", status, correlationID)
	if err != nil {
		// log.Printf("CRITICAL: Failed to update payment status to %s for %s: %v", status, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		// log.Printf("CRITICAL: Failed to commit status update to %s for %s: %v", status, correlationID, err)
		return
	}
}

func (wp *WorkerPools) updatePaymentProcessor(correlationID, processor string) {
	tx, err := wp.DB.Begin()
	if err != nil {
		// log.Printf("Failed to begin processor update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET payment_processor = $1 WHERE idempotency_key = $2", processor, correlationID)
	if err != nil {
		// log.Printf("Failed to update payment processor to %s for %s: %v", processor, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		// log.Printf("Failed to commit processor update to %s for %s: %v", processor, correlationID, err)
		return
	}
}

func (wp *WorkerPools) updatePaymentProcessorAndFee(correlationID, processor string, fee float64) {
	tx, err := wp.DB.Begin()
	if err != nil {
		// log.Printf("Failed to begin processor/fee update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET payment_processor = $1, fee = $2 WHERE idempotency_key = $3", processor, fee, correlationID)
	if err != nil {
		// log.Printf("Failed to update payment processor to %s and fee to %f for %s: %v", processor, fee, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		// log.Printf("Failed to commit processor/fee update to %s/%f for %s: %v", processor, fee, correlationID, err)
		return
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

func (wp *WorkerPools) StartHealthCheckWorker() {
	go func() {
		instanceID := os.Getenv("INSTANCE_ID")
		var staggerDelay time.Duration
		if instanceID == "2" {
			staggerDelay = 2500 * time.Millisecond
		}

		if staggerDelay > 0 {
			time.Sleep(staggerDelay)
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-wp.ctx.Done():
				return
			case <-ticker.C:
				wp.checkProcessorHealth()
			}
		}
	}()
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
		// log.Printf("Circuit breaker became available - triggering immediate retry burst")
		go wp.processFailedPayments()
	}

	wp.lastDefaultState = currentDefaultState
	wp.lastFallbackState = currentFallbackState
}

func (wp *WorkerPools) checkProcessorHealth() {
	now := time.Now()

	if now.Sub(wp.lastDefaultHealthCheck) >= 5*time.Second {
		go wp.checkSingleProcessorHealth(wp.DefaultEndpoint, "default")
		wp.lastDefaultHealthCheck = now
	}

	if now.Sub(wp.lastFallbackHealthCheck) >= 5*time.Second {
		time.Sleep(2500 * time.Millisecond)
		go wp.checkSingleProcessorHealth(wp.FallbackEndpoint, "fallback")
		wp.lastFallbackHealthCheck = now.Add(2500 * time.Millisecond)
	}
}

func (wp *WorkerPools) checkSingleProcessorHealth(endpoint, processorType string) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/payments/service-health", endpoint), nil)
	if err != nil {
		// log.Printf("Error creating health check request for %s: %v", processorType, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := wp.Client.Do(req)
	if err != nil {
		// log.Printf("Health check failed for %s processor: %v", processorType, err)
		wp.updateProcessorHealth(processorType, ProcessorHealth{IsValid: false})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		// log.Printf("Health check rate limited for %s processor", processorType)
		return
	}

	if resp.StatusCode != 200 {
		// log.Printf("Health check returned status %d for %s processor", resp.StatusCode, processorType)
		wp.updateProcessorHealth(processorType, ProcessorHealth{IsValid: false})
		return
	}

	var health ProcessorHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		// log.Printf("Error decoding health response for %s processor: %v", processorType, err)
		wp.updateProcessorHealth(processorType, ProcessorHealth{IsValid: false})
		return
	}

	health.LastChecked = time.Now()
	health.IsValid = true

	wp.updateProcessorHealth(processorType, health)

	if processorType == "default" && !health.Failing {
		previousHealth := wp.getProcessorHealth("default")
		if !previousHealth.IsValid || previousHealth.Failing {
			// log.Printf("Default processor became healthy - triggering priority retry burst")
			go func() {
				instanceID := os.Getenv("INSTANCE_ID")
				if instanceID == "2" {
					time.Sleep(500 * time.Millisecond)
				}
				wp.processFailedPayments()
			}()
		}
	}
}

func (wp *WorkerPools) updateProcessorHealth(processorType string, health ProcessorHealth) {
	wp.healthMutex.Lock()
	defer wp.healthMutex.Unlock()

	if processorType == "default" {
		wp.defaultHealth = health
	} else {
		wp.fallbackHealth = health
	}
}

func (wp *WorkerPools) getProcessorHealth(processorType string) ProcessorHealth {
	wp.healthMutex.RLock()
	defer wp.healthMutex.RUnlock()

	if processorType == "default" {
		return wp.defaultHealth
	}
	return wp.fallbackHealth
}

func (wp *WorkerPools) processFailedPayments() {
	rows, err := wp.DB.Query(`
		SELECT idempotency_key, amount, fee, requested_at 
		FROM payment_log 
		WHERE status = 'failed' 
		ORDER BY requested_at ASC 
		LIMIT 100`)
	if err != nil {
		// log.Printf("Failed to query failed payments: %v", err)
		return
	}
	defer rows.Close()

	// log.Default().Println("starting retry")

	processedCount := 0
	for rows.Next() {
		var correlationID string
		var amount decimal.Decimal
		var fee float64
		var requestedAt time.Time

		if err := rows.Scan(&correlationID, &amount, &fee, &requestedAt); err != nil {
			// log.Printf("Failed to scan failed payment row: %v", err)
			continue
		}

		task := PaymentTask{
			Request: PaymentRequest{
				CorrelationID: correlationID,
				Amount:        amount,
			},
			RequestedAt: requestedAt,
			Fee:         fee,
		}

		wp.wg.Add(1)
		// log.Default().Println("retrying")
		if err := wp.RetryPool.Submit(func() {
			defer wp.wg.Done()
			wp.ProcessPaymentDownstream(task)
			// log.Default().Println("finished retrying attempt")
		}); err != nil {
			wp.wg.Done()
			// log.Printf("Failed to submit retry for payment %s: %v", correlationID, err)
		}

		processedCount++
		if processedCount%10 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func main() {
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	connStr := "postgresql://dev:secret123@postgres/onecent?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		// log.Default().Fatal("failed to open db connection", err)
	}

	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	_, err = db.Exec(`
				CREATE TABLE IF NOT EXISTS payment_log(
						id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
						amount DECIMAL NOT NULL,
						fee DECIMAL NOT NULL,
						payment_processor TEXT,
						requested_at TIMESTAMP,
						processing_started_at TIMESTAMP,
						status TEXT DEFAULT 'success',
						idempotency_key TEXT UNIQUE
				);
				CREATE INDEX IF NOT EXISTS idx_requested_at ON payment_log(requested_at);
				CREATE INDEX IF NOT EXISTS idx_status ON payment_log(status);
				CREATE INDEX IF NOT EXISTS idx_status_requested_at ON payment_log(status, requested_at);
				CREATE INDEX IF NOT EXISTS idx_pending_failed_processing ON payment_log(status, processing_started_at) WHERE status IN ('pending', 'failed');
    `)
	if err != nil {
		// log.Default().Fatal("failed to create tables", err)
	}

	insertPreparedStmt, err := db.Prepare(insertPayment)
	if err != nil {
		// log.Default().Fatal("failed to prepare insert statement", err)
	}
	defer insertPreparedStmt.Close()

	updatePreparedStmt, err := db.Prepare(updatePayment)
	if err != nil {
		// log.Default().Fatal("failed to prepare update statement", err)
	}
	defer updatePreparedStmt.Close()

	statsAllPreparedStmt, err := db.Prepare(statsAll)
	if err != nil {
		// log.Default().Fatal("failed to prepare stats all statement", err)
	}
	defer statsAllPreparedStmt.Close()

	statsFromPreparedStmt, err := db.Prepare(statsFrom)
	if err != nil {
		// log.Default().Fatal("failed to prepare stats from statement", err)
	}
	defer statsFromPreparedStmt.Close()

	statsToPreparedStmt, err := db.Prepare(statsTo)
	if err != nil {
		// log.Default().Fatal("failed to prepare stats to statement", err)
	}
	defer statsToPreparedStmt.Close()

	statsBothPreparedStmt, err := db.Prepare(statsBoth)
	if err != nil {
		// log.Default().Fatal("failed to prepare stats both statement", err)
	}
	defer statsBothPreparedStmt.Close()

	transport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 20,
		MaxConnsPerHost:     40,
		IdleConnTimeout:     90 * time.Second,

		ResponseHeaderTimeout: 5500 * time.Millisecond,

		ExpectContinueTimeout: 0,

		DisableCompression: true,
		DisableKeepAlives:  false,

		DialContext: (&net.Dialer{
			Timeout:   5000 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: false,
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   6000 * time.Millisecond,

		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var defaultSettings gobreaker.Settings
	defaultSettings.Name = "Default Payments Breaker"
	defaultSettings.MaxRequests = 20
	defaultSettings.Interval = time.Duration(3 * time.Second)
	defaultSettings.Timeout = time.Duration(1500 * time.Millisecond)
	defaultSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 10 && failureRatio >= 0.6
	}

	defaultCB = gobreaker.NewCircuitBreaker[[]byte](defaultSettings)

	var fallbackSettings gobreaker.Settings
	fallbackSettings.Name = "Fallback Payments Breaker"
	fallbackSettings.MaxRequests = 20
	fallbackSettings.Interval = time.Duration(3 * time.Second)
	fallbackSettings.Timeout = time.Duration(1500 * time.Millisecond)
	fallbackSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 10 && failureRatio >= 0.6
	}

	fallbackCB = gobreaker.NewCircuitBreaker[[]byte](fallbackSettings)

	defaultPPEndpoint := "http://payment-processor-default:8080"
	fallbackPPEndpoint := "http://payment-processor-fallback:8080"

	defaultFee, err = GetPPFee(defaultPPEndpoint)
	if err != nil {
		// log.Default().Fatal("failed getting the default processor transaction fee ", err)
	}

	fallbackFee, err = GetPPFee(fallbackPPEndpoint)
	if err != nil {
		// log.Default().Fatal("failed getting the fallback processor transaction fee ", err)
	}

	workerPools, err = NewWorkerPools(db, defaultPPEndpoint, fallbackPPEndpoint, defaultFee, fallbackFee, client, defaultCB, fallbackCB)
	if err != nil {
		// log.Default().Fatal("failed to initialize worker pools: ", err)
	}

	// workerPools.StartRetryWorker()
	workerPools.StartHealthCheckWorker()

	app := fiber.New()

	app.Post("/payments", func(c fiber.Ctx) error {
		var paymentRequest PaymentRequest
		requestedAt := time.Now()

		body := c.Body()

		if err := json.Unmarshal(body, &paymentRequest); err != nil {
			return c.Status(400).JSON(fiber.Map{
				"error": "Invalid JSON",
			})
		}

		task := PaymentTask{
			Request:     paymentRequest,
			RequestedAt: requestedAt,
			Fee:         defaultFee,
			Processor:   "default",
		}

		_ = workerPools.CreationPool.Submit(func() {
			workerPools.ProcessPaymentCreation(task)
		})

		return c.Send(nil)
	})

	app.Get("/payments-summary", func(c fiber.Ctx) error {
		fromParam := c.Query("from")
		toParam := c.Query("to")

		var fromTime, toTime time.Time
		var err error

		if fromParam != "" {
			fromTime, err = time.Parse("2006-01-02T15:04:05.000Z", fromParam)
			if err != nil {
				// log.Default().Fatal("failed parsing from", err.Error())
			}
		}

		if toParam != "" {
			toTime, err = time.Parse("2006-01-02T15:04:05.000Z", toParam)
			if err != nil {
				// log.Default().Fatal("failed parsing to", err.Error())
			}
		}

		hasFrom := !fromTime.IsZero()
		hasTo := !toTime.IsZero()

		var rows *sql.Rows

		switch {
		case hasFrom && hasTo:
			rows, err = statsBothPreparedStmt.Query(fromTime, toTime)
		case hasFrom:
			rows, err = statsFromPreparedStmt.Query(fromTime)
		case hasTo:
			rows, err = statsToPreparedStmt.Query(toTime)
		default:
			rows, err = statsAllPreparedStmt.Query()
		}

		if err != nil {
			// log.Default().Printf("failed querying payment stats: %v", err)
			return c.Status(500).Send([]byte("Internal server error"))
		}
		defer rows.Close()

		defaultStats := &PaymentProcessorStats{
			TotalRequests: 0,
			TotalAmount:   NewJSONDecimal(decimal.NewFromInt(0)),
		}
		fallbackStats := &PaymentProcessorStats{
			TotalRequests: 0,
			TotalAmount:   NewJSONDecimal(decimal.NewFromInt(0)),
		}

		for rows.Next() {
			var processor string
			var count int64
			var totalAmountString string

			err = rows.Scan(&processor, &count, &totalAmountString)
			if err != nil {
				// log.Default().Printf("failed scanning payment stats row: %v", err)
				continue
			}

			totalAmount, err := decimal.NewFromString(totalAmountString)
			if err != nil {
				// log.Default().Printf("failed parsing total amount: %v", err)
				totalAmount = decimal.NewFromInt(0)
			}

			stats := &PaymentProcessorStats{
				TotalRequests: int(count),
				TotalAmount:   NewJSONDecimal(totalAmount),
			}

			if processor == "default" {
				defaultStats = stats
			} else if processor == "fallback" {
				fallbackStats = stats
			}
		}

		paymentsSummaryResp := &PaymentsSummaryResponse{
			Default:  *defaultStats,
			Fallback: *fallbackStats,
		}

		resp, err := json.Marshal(paymentsSummaryResp)
		if err != nil {
			// log.Default().Fatal("failed marshaling response", err.Error())
		}

		return c.Send(resp)
	})

	go func() {
		if err := app.Listen(":8080"); err != nil {
			// log.Printf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// log.Println("Shutting down server...")

	if err := app.Shutdown(); err != nil {
		// log.Printf("Server forced to shutdown: %v", err)
	}

	if err := workerPools.Shutdown(30 * time.Second); err != nil {
		// log.Printf("Worker pools shutdown failed: %v", err)
	}

	// log.Println("Server exiting")
}

func GetPPFee(processorURL string) (float64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/admin/payments-summary", processorURL), nil)
	if err != nil {
		// log.Default().Println("error creating request GetPPFee", err.Error())
	}

	req.Header.Set("X-Rinha-Token", "123")

	resp, err := client.Do(req)
	if err != nil {
		// log.Default().Println("error doing request GetPPFee", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("request failed with status: %s\n", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// log.Default().Println("error reading body GetPPFee", err.Error())
	}

	var summary AdminTransactionSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		// log.Default().Println("error unmarshaling request GetPPFee", err.Error())
	}

	floatFee, exact := summary.FeePerTransaction.Float64()
	if !exact {
		// log.Default().Println("fee conversion not exact to float64 GetPPFee ", exact)
	}

	return floatFee, nil
}
