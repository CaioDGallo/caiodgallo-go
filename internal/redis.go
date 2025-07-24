package internal

// Redis keys structure
// payments:log:<processor> - sorted set for payment log (score=timestamp, member=JSON with correlationId+requestedAt)
// retry:queue - sorted set for retry queue (score=retryTime, member=JSON with payload+metadata)

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/panjf2000/ants"
	"github.com/redis/go-redis/v9"
)

type RedisPaymentHandler struct {
	client *redis.Client
	pool   *ants.Pool
	pf     *PaymentForwarder
}

func NewRedisPaymentHandler(pf *PaymentForwarder) *RedisPaymentHandler {
	client := redis.NewClient(&redis.Options{
		Addr:         "redis:6379",
		PoolSize:     10,
		MinIdleConns: 5,
		MaxRetries:   2,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
	})

	pool, _ := ants.NewPool(50, ants.WithPreAlloc(true))

	return &RedisPaymentHandler{
		client: client,
		pool:   pool,
		pf:     pf,
	}
}

func (r *RedisPaymentHandler) SetPaymentForwarder(pf *PaymentForwarder) {
	r.pf = pf
}

// The complete processRetry implementation
func (r *RedisPaymentHandler) processRetry(retryMember string) bool {
	ctx := context.Background()

	// Parse retry data
	var retryData RetryPayload
	if err := json.Unmarshal([]byte(retryMember), &retryData); err != nil {
		log.Printf("Failed to parse retry data: %v", err)
		return true // Remove invalid data
	}

	// Increment attempt count
	retryData.Attempts++

	// Stop retrying after 5 attempts
	if retryData.Attempts > 5 {
		log.Printf("Max retries reached for payload")
		return true // Remove from queue
	}

	// Attempt to forward
	requestedAt := time.Now()
	err := r.pf.ForwardPayment([]byte(retryData.Payload), requestedAt)
	if err != nil {
		log.Printf("Retry attempt %d failed: %v", retryData.Attempts, err)

		// Exponential backoff for next retry
		backoffSeconds := math.Pow(2, float64(retryData.Attempts)) * 10
		newRetryTime := time.Now().Add(time.Duration(backoffSeconds) * time.Second).Unix()

		// Update retry data with new attempt count
		updatedRetryJSON, _ := json.Marshal(retryData)

		// Update score in sorted set for next retry
		r.client.ZAdd(ctx, "retry:queue", redis.Z{
			Score:  float64(newRetryTime),
			Member: string(updatedRetryJSON),
		})

		return false // Keep in queue with new retry time
	}

	// Success - register the payment
	var paymentReq PaymentRequest
	if err := json.Unmarshal([]byte(retryData.Payload), &paymentReq); err == nil {
		err := r.RegisterPayment(paymentReq.CorrelationID, retryData.Processor, requestedAt)
		if err != nil {
			log.Printf("Failed to register successful retry: %v", err)
		}
	}

	return true // Remove from retry queue
}

func (r *RedisPaymentHandler) RegisterPayment(correlationID string, processor string, requestedAt time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Create payment record with correlationId and timestamp
	paymentData := fmt.Sprintf(`{"correlationId":"%s","requestedAt":"%s"}`, 
		correlationID, requestedAt.Format(time.RFC3339))

	// Store in processor-specific sorted set
	key := fmt.Sprintf("payments:log:%s", processor)
	return r.client.ZAdd(ctx, key, redis.Z{
		Score:  float64(requestedAt.UnixNano()),
		Member: paymentData,
	}).Err()
}

type RetryPayload struct {
	Payload   string `json:"payload"`
	Processor string `json:"processor"`
	Attempts  int    `json:"attempts"`
}

func (r *RedisPaymentHandler) EnqueueRetry(payload []byte) error {
	ctx := context.Background()

	// Create retry metadata
	retryData := RetryPayload{
		Payload:   string(payload),
		Processor: "default", // TODO: make this configurable
		Attempts:  0,
	}

	retryJSON, err := json.Marshal(retryData)
	if err != nil {
		return err
	}

	// Use sorted set with retry time as score
	retryTime := time.Now().Add(10 * time.Second).Unix()
	return r.client.ZAdd(ctx, "retry:queue", redis.Z{
		Score:  float64(retryTime),
		Member: string(retryJSON),
	}).Err()
}

func (r *RedisPaymentHandler) ProcessRetryQueue() {
	ticker := time.NewTicker(2 * time.Second) // Check more frequently

	for range ticker.C {
		ctx := context.Background()
		now := time.Now().Unix()

		// Get due retries
		payloads, err := r.client.ZRangeByScore(ctx, "retry:queue", &redis.ZRangeBy{
			Min:    "0",
			Max:    fmt.Sprintf("%d", now),
			Offset: 0,
			Count:  100,
		}).Result()

		if err != nil || len(payloads) == 0 {
			continue
		}

		// Process each retry
		for _, retryMember := range payloads {
			r.pool.Submit(func() {
				if r.processRetry(retryMember) {
					// Remove from retry queue
					r.client.ZRem(ctx, "retry:queue", retryMember)
				}
				// Note: processRetry handles updating retry time internally
			})
		}
	}
}

func (r *RedisPaymentHandler) GetPaymentSummary(from, to time.Time, processor string) (int64, error) {
	ctx := context.Background()
	key := fmt.Sprintf("payments:log:%s", processor)

	if from.IsZero() && to.IsZero() {
		// Count all payments for this processor
		return r.client.ZCard(ctx, key).Result()
	}

	minScore := "-inf"
	maxScore := "+inf"

	if !from.IsZero() {
		minScore = fmt.Sprintf("%d", from.UnixNano())
	}
	if !to.IsZero() {
		maxScore = fmt.Sprintf("%d", to.UnixNano())
	}

	return r.client.ZCount(ctx, key, minScore, maxScore).Result()
}
