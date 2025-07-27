package workers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/sony/gobreaker/v2"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
)

func (wp *WorkerPools) ProcessPaymentDirect(task types.PaymentTask) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Payment processing worker panic recovered: %v", r)
		}
	}()

	correlationID := task.Request.CorrelationID
	requestedAtString := task.RequestedAt.Format("2006-01-02T15:04:05.000Z")

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

	ppPaymentRequest := &types.PaymentProcessorPaymentRequest{
		PaymentRequest: task.Request,
		RequestedAt:    requestedAtString,
	}

	ppPayload, err := json.Marshal(ppPaymentRequest)
	if err != nil {
		log.Printf("Error marshaling request for %s: %v", correlationID, err)
		wp.createPaymentRecord(correlationID, task.Request.Amount, fee, processor, task.RequestedAt, "failed")
		return
	}

	var endpoint string
	var circuitBreaker *gobreaker.CircuitBreaker[[]byte]

	if processor == "default" {
		endpoint = wp.DefaultEndpoint
		circuitBreaker = wp.DefaultCircuitBreaker
	} else {
		endpoint = wp.FallbackEndpoint
		circuitBreaker = wp.FallbackCircuitBreaker
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", endpoint), bytes.NewReader(ppPayload))
	if err != nil {
		log.Printf("Error creating request for %s: %v", correlationID, err)
		wp.createPaymentRecord(correlationID, task.Request.Amount, fee, processor, task.RequestedAt, "failed")
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
		log.Printf("Default processor failed for %s, trying fallback: %v", correlationID, err)

		req, err = http.NewRequest("POST", fmt.Sprintf("%s/payments", wp.FallbackEndpoint), bytes.NewReader(ppPayload))
		if err != nil {
			log.Printf("Error creating fallback request for %s: %v", correlationID, err)
			wp.createPaymentRecord(correlationID, task.Request.Amount, fee, processor, task.RequestedAt, "failed")
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
			processor = "fallback"
			fee = wp.FallbackFee
		}
	}

	if err != nil {
		log.Printf("Payment processing failed for %s: %v", correlationID, err)
		wp.createPaymentRecord(correlationID, task.Request.Amount, fee, processor, task.RequestedAt, "failed")
		return
	}

	wp.createPaymentRecord(correlationID, task.Request.Amount, fee, processor, task.RequestedAt, "success")
}

func (wp *WorkerPools) createPaymentRecord(correlationID string, amount decimal.Decimal, fee float64, processor string, requestedAt time.Time, status string) {
	tx, err := wp.DB.Begin()
	if err != nil {
		log.Printf("CRITICAL: Failed to begin payment creation transaction for %s: %v", correlationID, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO payment_log (idempotency_key, payment_processor, amount, fee, requested_at, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING",
		correlationID, processor, amount, fee, requestedAt, status,
	)
	if err != nil {
		log.Printf("CRITICAL: Failed to insert payment %s: %v", correlationID, err)
		return
	}

	if err = tx.Commit(); err != nil {
		log.Printf("CRITICAL: Failed to commit payment creation for %s: %v", correlationID, err)
		return
	}
}




func (wp *WorkerPools) processFailedPayments() {
	rows, err := wp.DB.Query(`
		SELECT idempotency_key, amount, fee, requested_at 
		FROM payment_log 
		WHERE status = 'failed' 
		ORDER BY requested_at ASC 
		LIMIT 100`)
	if err != nil {
		log.Printf("Failed to query failed payments: %v", err)
		return
	}
	defer rows.Close()

	processedCount := 0
	for rows.Next() {
		var correlationID string
		var amount decimal.Decimal
		var fee float64
		var requestedAt time.Time

		if err := rows.Scan(&correlationID, &amount, &fee, &requestedAt); err != nil {
			log.Printf("Failed to scan failed payment row: %v", err)
			continue
		}

		task := types.PaymentTask{
			Request: types.PaymentRequest{
				CorrelationID: correlationID,
				Amount:        amount,
			},
			RequestedAt: requestedAt,
			Fee:         fee,
		}

		wp.wg.Add(1)
		if err := wp.RetryPool.Submit(func() {
			defer wp.wg.Done()
			wp.ProcessPaymentDirect(task)
		}); err != nil {
			wp.wg.Done()
			log.Printf("Failed to submit retry for payment %s: %v", correlationID, err)
		}

		processedCount++
		if processedCount%10 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

