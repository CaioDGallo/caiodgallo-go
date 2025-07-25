package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// FIXME: float64 might be imprecise
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

type WorkerPools struct {
	CreationPool           *ants.Pool
	ProcessingPool         *ants.Pool
	RetryPool              *ants.Pool
	DB                     *sql.DB
	DefaultEndpoint        string
	FallbackEndpoint       string
	DefaultFee             float64
	FallbackFee            float64
	Client                 *http.Client
	DefaultCircuitBreaker  *gobreaker.CircuitBreaker[[]byte]
	FallbackCircuitBreaker *gobreaker.CircuitBreaker[[]byte]
	wg                     sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc
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
			log.Printf("Payment creation worker panic: %v", i)
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
			log.Printf("Payment processing worker panic: %v", i)
		},
	}))
	if err != nil {
		creationPool.Release()
		cancel()
		return nil, fmt.Errorf("failed to create processing pool: %v", err)
	}

	retryPool, err := ants.NewPool(40, ants.WithOptions(ants.Options{
		ExpiryDuration: 60 * time.Second,
		Nonblocking:    false,
		PanicHandler: func(i interface{}) {
			log.Printf("Payment retry worker panic: %v", i)
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
	}

	return wp, nil
}

func (wp *WorkerPools) Shutdown(timeout time.Duration) error {
	log.Println("Starting graceful shutdown of worker pools...")

	wp.cancel()

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

	wp.CreationPool.Release()
	wp.ProcessingPool.Release()
	wp.RetryPool.Release()

	log.Println("Worker pools shutdown complete")
	return nil
}

func (wp *WorkerPools) ProcessPaymentCreation(task PaymentTask) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Payment creation worker panic recovered: %v", r)
		}
	}()

	// Determine which processor and fee to use
	var processor string
	var fee float64

	if wp.DefaultCircuitBreaker.State() == gobreaker.StateClosed {
		processor = "default"
		fee = wp.DefaultFee
	} else if wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		processor = "fallback"
		fee = wp.FallbackFee
	} else {
		// Both circuit breakers are open, use default (better fees) and let downstream handle the failure
		processor = "default"
		fee = wp.DefaultFee
	}

	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction for payment %s: %v", task.Request.CorrelationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO payment_log (idempotency_key, payment_processor, amount, fee, requested_at, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING",
		task.Request.CorrelationID, processor, task.Request.Amount, fee, task.RequestedAt, "pending",
	)
	if err != nil {
		log.Printf("Failed to insert payment %s: %v", task.Request.CorrelationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit payment creation for %s: %v", task.Request.CorrelationID, err)
		return
	}

	// Update task with the processor and fee that will be used
	task.Processor = processor
	task.Fee = fee

	wp.wg.Add(1)
	if err := wp.ProcessingPool.Submit(func() {
		defer wp.wg.Done()
		wp.ProcessPaymentDownstream(task)
	}); err != nil {
		wp.wg.Done()
		log.Printf("Failed to submit payment %s to processing pool: %v", task.Request.CorrelationID, err)
	}
}

func (wp *WorkerPools) ProcessPaymentDownstream(task PaymentTask) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Payment processing worker panic recovered: %v", r)
		}
	}()

	correlationID := task.Request.CorrelationID
	requestedAtString := task.RequestedAt.Format("2006-01-02T15:04:05.000Z")

	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("Failed to begin processing transaction for payment %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"UPDATE payment_log SET status = $1, processing_started_at = $2 WHERE idempotency_key = $3 AND status = 'pending'",
		"processing", time.Now(), correlationID,
	)
	if err != nil {
		log.Printf("Failed to update payment %s to processing: %v", correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit processing status for %s: %v", correlationID, err)
		return
	}

	ppPaymentRequest := &PaymentProcessorPaymentRequest{
		PaymentRequest: task.Request,
		RequestedAt:    requestedAtString,
	}

	ppPayload, err := json.Marshal(ppPaymentRequest)
	if err != nil {
		log.Printf("Error marshaling request for %s: %v", correlationID, err)
		wp.updatePaymentStatus(correlationID, "failed")
		return
	}

	// Determine which processor to use based on circuit breaker state
	var endpoint string
	var circuitBreaker *gobreaker.CircuitBreaker[[]byte]
	var processor string

	// Prefer default processor if circuit breaker is closed
	if wp.DefaultCircuitBreaker.State() == gobreaker.StateClosed {
		endpoint = wp.DefaultEndpoint
		circuitBreaker = wp.DefaultCircuitBreaker
		processor = "default"
	} else if wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		endpoint = wp.FallbackEndpoint
		circuitBreaker = wp.FallbackCircuitBreaker
		processor = "fallback"
	} else {
		// Both circuit breakers are open, try default first (it has better fees)
		endpoint = wp.DefaultEndpoint
		circuitBreaker = wp.DefaultCircuitBreaker
		processor = "default"
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", endpoint), bytes.NewReader(ppPayload))
	if err != nil {
		log.Printf("Error creating request for %s: %v", correlationID, err)
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

	// If default processor failed and we haven't tried fallback yet, try fallback
	if err != nil && processor == "default" && wp.FallbackCircuitBreaker.State() == gobreaker.StateClosed {
		log.Printf("Default processor failed for %s, trying fallback: %v", correlationID, err)

		// Try fallback processor
		req, err = http.NewRequest("POST", fmt.Sprintf("%s/payments", wp.FallbackEndpoint), bytes.NewReader(ppPayload))
		if err != nil {
			log.Printf("Error creating fallback request for %s: %v", correlationID, err)
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

		// If fallback succeeded, update the processor and fee in database to reflect actual processor used
		if err == nil {
			wp.updatePaymentProcessorAndFee(correlationID, "fallback", wp.FallbackFee)
		}
	}

	if err != nil {
		log.Printf("Payment processing failed for %s: %v", correlationID, err)
		wp.updatePaymentStatus(correlationID, "failed")
		return
	}

	wp.updatePaymentStatus(correlationID, "success")
}

func (wp *WorkerPools) updatePaymentStatus(correlationID, status string) {
	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("CRITICAL: Failed to begin status update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET status = $1 WHERE idempotency_key = $2", status, correlationID)
	if err != nil {
		log.Printf("CRITICAL: Failed to update payment status to %s for %s: %v", status, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("CRITICAL: Failed to commit status update to %s for %s: %v", status, correlationID, err)
		return
	}
}

func (wp *WorkerPools) updatePaymentProcessor(correlationID, processor string) {
	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("Failed to begin processor update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET payment_processor = $1 WHERE idempotency_key = $2", processor, correlationID)
	if err != nil {
		log.Printf("Failed to update payment processor to %s for %s: %v", processor, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit processor update to %s for %s: %v", processor, correlationID, err)
		return
	}
}

func (wp *WorkerPools) updatePaymentProcessorAndFee(correlationID, processor string, fee float64) {
	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("Failed to begin processor/fee update transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE payment_log SET payment_processor = $1, fee = $2 WHERE idempotency_key = $3", processor, fee, correlationID)
	if err != nil {
		log.Printf("Failed to update payment processor to %s and fee to %f for %s: %v", processor, fee, correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit processor/fee update to %s/%f for %s: %v", processor, fee, correlationID, err)
		return
	}
}

func (wp *WorkerPools) StartRetryWorker() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-wp.ctx.Done():
				return
			case <-ticker.C:
				wp.processFailedPayments()
			}
		}
	}()
}

func (wp *WorkerPools) processFailedPayments() {
	cutoffTime := time.Now().Add(-5 * time.Minute)

	rows, err := wp.DB.Query(`
		SELECT idempotency_key, amount, fee, requested_at 
		FROM payment_log 
		WHERE status = 'failed' 
		AND (processing_started_at IS NULL OR processing_started_at < $1)
		ORDER BY requested_at ASC 
		LIMIT 10
	`, cutoffTime)
	if err != nil {
		log.Printf("Failed to query failed payments: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var correlationID string
		var amount decimal.Decimal
		var fee float64
		var requestedAt time.Time

		if err := rows.Scan(&correlationID, &amount, &fee, &requestedAt); err != nil {
			log.Printf("Failed to scan failed payment row: %v", err)
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
		if err := wp.RetryPool.Submit(func() {
			defer wp.wg.Done()
			wp.ProcessPaymentDownstream(task)
		}); err != nil {
			wp.wg.Done()
			log.Printf("Failed to submit retry for payment %s: %v", correlationID, err)
		}
	}
}

func main() {
	connStr := "postgresql://dev:secret123@postgres/onecent?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Default().Fatal("failed to open db connection", err)
	}

	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)

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
		log.Default().Fatal("failed to create tables", err)
	}

	insertPreparedStmt, err := db.Prepare(insertPayment)
	if err != nil {
		log.Default().Fatal("failed to prepare insert statement", err)
	}
	defer insertPreparedStmt.Close()

	updatePreparedStmt, err := db.Prepare(updatePayment)
	if err != nil {
		log.Default().Fatal("failed to prepare update statement", err)
	}
	defer updatePreparedStmt.Close()

	statsAllPreparedStmt, err := db.Prepare(statsAll)
	if err != nil {
		log.Default().Fatal("failed to prepare stats all statement", err)
	}
	defer statsAllPreparedStmt.Close()

	statsFromPreparedStmt, err := db.Prepare(statsFrom)
	if err != nil {
		log.Default().Fatal("failed to prepare stats from statement", err)
	}
	defer statsFromPreparedStmt.Close()

	statsToPreparedStmt, err := db.Prepare(statsTo)
	if err != nil {
		log.Default().Fatal("failed to prepare stats to statement", err)
	}
	defer statsToPreparedStmt.Close()

	statsBothPreparedStmt, err := db.Prepare(statsBoth)
	if err != nil {
		log.Default().Fatal("failed to prepare stats both statement", err)
	}
	defer statsBothPreparedStmt.Close()

	transport := &http.Transport{
		MaxIdleConns:        20,               // High idle connections
		MaxIdleConnsPerHost: 20,               // All to same host
		MaxConnsPerHost:     0,                // No limit
		IdleConnTimeout:     90 * time.Second, // Keep connections alive

		ResponseHeaderTimeout: 1500 * time.Millisecond, // Was 2s

		ExpectContinueTimeout: 0, // Disable Expect: 100-continue

		DisableCompression: true,  // No compression overhead
		DisableKeepAlives:  false, // Keep connections alive

		DialContext: (&net.Dialer{
			Timeout:   1000 * time.Millisecond, // Faster connection timeout
			KeepAlive: 30 * time.Second,        // TCP keepalive
			DualStack: false,                   // IPv4 only if possible
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   2000 * time.Millisecond,

		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Default processor circuit breaker
	var defaultSettings gobreaker.Settings
	defaultSettings.Name = "Default Payments Breaker"
	defaultSettings.MaxRequests = 10
	defaultSettings.Interval = time.Duration(3 * time.Second)
	defaultSettings.Timeout = time.Duration(3 * time.Second)
	defaultSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 10 && failureRatio >= 0.6
	}

	defaultCB = gobreaker.NewCircuitBreaker[[]byte](defaultSettings)

	// Fallback processor circuit breaker
	var fallbackSettings gobreaker.Settings
	fallbackSettings.Name = "Fallback Payments Breaker"
	fallbackSettings.MaxRequests = 10
	fallbackSettings.Interval = time.Duration(3 * time.Second)
	fallbackSettings.Timeout = time.Duration(3 * time.Second)
	fallbackSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 10 && failureRatio >= 0.6
	}

	fallbackCB = gobreaker.NewCircuitBreaker[[]byte](fallbackSettings)

	// Get fees from both processors
	defaultPPEndpoint := "http://payment-processor-default:8080"
	fallbackPPEndpoint := "http://payment-processor-fallback:8080"

	defaultFee, err = GetPPFee(defaultPPEndpoint)
	if err != nil {
		log.Default().Fatal("failed getting the default processor transaction fee ", err)
	}

	fallbackFee, err = GetPPFee(fallbackPPEndpoint)
	if err != nil {
		log.Default().Fatal("failed getting the fallback processor transaction fee ", err)
	}

	workerPools, err = NewWorkerPools(db, defaultPPEndpoint, fallbackPPEndpoint, defaultFee, fallbackFee, client, defaultCB, fallbackCB)
	if err != nil {
		log.Default().Fatal("failed to initialize worker pools: ", err)
	}

	workerPools.StartRetryWorker()

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

		err := workerPools.CreationPool.Submit(func() {
			workerPools.ProcessPaymentCreation(task)
		})
		if err != nil {
			// Pool overwhelmed - drop request silently to maintain low latency
		}

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
				log.Default().Fatal("failed parsing from", err.Error())
			}
		}

		if toParam != "" {
			toTime, err = time.Parse("2006-01-02T15:04:05.000Z", toParam)
			if err != nil {
				log.Default().Fatal("failed parsing to", err.Error())
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
			log.Default().Printf("failed querying payment stats: %v", err)
			return c.Status(500).Send([]byte("Internal server error"))
		}
		defer rows.Close()

		// Initialize stats for both processors
		defaultStats := &PaymentProcessorStats{
			TotalRequests: 0,
			TotalAmount:   NewJSONDecimal(decimal.NewFromInt(0)),
		}
		fallbackStats := &PaymentProcessorStats{
			TotalRequests: 0,
			TotalAmount:   NewJSONDecimal(decimal.NewFromInt(0)),
		}

		// Process results and populate stats
		for rows.Next() {
			var processor string
			var count int64
			var totalAmountString string

			err = rows.Scan(&processor, &count, &totalAmountString)
			if err != nil {
				log.Default().Printf("failed scanning payment stats row: %v", err)
				continue
			}

			totalAmount, err := decimal.NewFromString(totalAmountString)
			if err != nil {
				log.Default().Printf("failed parsing total amount: %v", err)
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
			log.Default().Fatal("failed marshaling response", err.Error())
		}

		return c.Send(resp)
	})

	go func() {
		if err := app.Listen(":8080"); err != nil {
			log.Printf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	if err := app.Shutdown(); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	if err := workerPools.Shutdown(30 * time.Second); err != nil {
		log.Printf("Worker pools shutdown failed: %v", err)
	}

	log.Println("Server exiting")
}

func GetPPFee(processorURL string) (float64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/admin/payments-summary", processorURL), nil)
	if err != nil {
		log.Default().Println("error creating request GetPPFee", err.Error())
	}

	req.Header.Set("X-Rinha-Token", "123")

	resp, err := client.Do(req)
	if err != nil {
		log.Default().Println("error doing request GetPPFee", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("request failed with status: %s\n", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Default().Println("error reading body GetPPFee", err.Error())
	}

	var summary AdminTransactionSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		log.Default().Println("error unmarshaling request GetPPFee", err.Error())
	}

	floatFee, exact := summary.FeePerTransaction.Float64()
	if !exact {
		log.Default().Println("fee conversion not exact to float64 GetPPFee ", exact)
	}

	return floatFee, nil
}
