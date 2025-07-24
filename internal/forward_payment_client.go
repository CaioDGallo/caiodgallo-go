package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

type RetryEnqueuer interface {
	EnqueueRetry(payload []byte) error
}

var (
	requestedAtPrefix = []byte(`,"requestedAt":"`)
	requestedAtSuffix = []byte(`"`)
	emptyObjPrefix    = []byte(`"requestedAt":"`)
)

type AdminTransactionSummary struct {
	TotalRequests     int             `json:"totalRequests"`
	TotalAmount       decimal.Decimal `json:"totalAmount"`
	TotalFee          decimal.Decimal `json:"totalFee"`
	FeePerTransaction decimal.Decimal `json:"feePerTransaction"`
}

type PaymentForwarder struct {
	client         *http.Client
	endpoint       string
	transactionFee int64

	bufferPool sync.Pool

	failureCount uint32
	lastFailTime int64
	circuitOpen  uint32

	totalRequests uint64
	totalFailures uint64

	retryHandler RetryEnqueuer
}

func NewPaymentForwarder(endpoint string, retryHandler RetryEnqueuer) *PaymentForwarder {
	transport := &http.Transport{
		MaxIdleConns:        100,              // High idle connections
		MaxIdleConnsPerHost: 100,              // All to same host
		MaxConnsPerHost:     0,                // No limit
		IdleConnTimeout:     90 * time.Second, // Keep connections alive

		ResponseHeaderTimeout: 400 * time.Millisecond, // Was 2s

		ExpectContinueTimeout: 0, // Disable Expect: 100-continue

		DisableCompression: true,  // No compression overhead
		DisableKeepAlives:  false, // Keep connections alive

		DialContext: (&net.Dialer{
			Timeout:   200 * time.Millisecond, // Faster connection timeout
			KeepAlive: 30 * time.Second,       // TCP keepalive
			DualStack: false,                  // IPv4 only if possible
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	pf := &PaymentForwarder{
		client: &http.Client{
			Transport: transport,
			Timeout:   600 * time.Millisecond,

			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint:       endpoint,
		transactionFee: 0,
		retryHandler:   retryHandler,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 4096)
			},
		},
	}

	// TODO: need to get the fallback fee when handling it
	fee, err := pf.GetDefaultPPFee()
	if err != nil {
		log.Default().Fatal("failed to get fee", err.Error())
	}
	pf.SetTransactionFee(fee)

	return pf
}

func (pf *PaymentForwarder) SetTransactionFee(fee int64) {
	pf.transactionFee = fee
}

func (pf *PaymentForwarder) GetDefaultPPFee() (int64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/admin/payments-summary", pf.endpoint), nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("X-Rinha-Token", "123")
	req.Close = false

	resp, err := pf.client.Do(req)
	if err != nil {
		log.Default().Println("error doing request ", err.Error())
		return 0, err
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Request failed with status: %s\n", resp.Status)
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var summary AdminTransactionSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return 0, err
	}

	return summary.FeePerTransaction.Mul(decimal.NewFromInt(100)).IntPart(), nil
}

func (pf *PaymentForwarder) ForwardPayment(payload []byte, requestedAt time.Time) error {
	if atomic.LoadUint32(&pf.circuitOpen) == 1 {
		if time.Now().Unix()-atomic.LoadInt64(&pf.lastFailTime) < 3 {
			err := pf.retryHandler.EnqueueRetry(payload)
			if err != nil {
				log.Default().Println("failed enqueueing to retry", err)
			}
			return fmt.Errorf("circuit breaker is open")
		}
		atomic.StoreUint32(&pf.circuitOpen, 0)
		atomic.StoreUint32(&pf.failureCount, 0)
	}
	timestampBytes := []byte(requestedAt.Format(time.RFC3339))
	finalPayload := InjectRequestedAt(payload, timestampBytes)

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", pf.endpoint), bytes.NewReader(finalPayload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(payload)))
	req.Close = false

	resp, err := pf.client.Do(req)
	if err != nil {
		log.Default().Println("error doing request ", err.Error())
		err := pf.handleFailure(payload)
		if err != nil {
			log.Default().Println("error enqueueing for retry", err.Error())
		}
		return err
	}
	atomic.AddUint64(&pf.totalRequests, 1)

	buf := pf.bufferPool.Get().([]byte)
	defer pf.bufferPool.Put(buf)

	_, err = io.CopyBuffer(io.Discard, resp.Body, buf)
	if err != nil {
		log.Default().Println(err.Error())
	}
	resp.Body.Close()

	if resp.StatusCode >= 500 {
		err := pf.handleFailure(payload)
		if err != nil {
			log.Default().Println("failed enqueueing for retry", err.Error())
		}
		return fmt.Errorf("server error: %d", resp.StatusCode)
	}

	atomic.StoreUint32(&pf.failureCount, 0)
	return nil
}

func (pf *PaymentForwarder) ForwardBatch(payloads [][]byte) {
	var wg sync.WaitGroup

	sem := make(chan struct{}, 50)

	for _, payload := range payloads {
		wg.Add(1)
		sem <- struct{}{} // Acquire

		go func(data []byte) {
			defer wg.Done()
			defer func() { <-sem }() // Release

			requestedAt := time.Now()
			pf.ForwardPayment(data, requestedAt)
		}(payload)
	}

	wg.Wait()
}

func (pf *PaymentForwarder) handleFailure(payload []byte) error {
	atomic.AddUint64(&pf.totalFailures, 1)
	failures := atomic.AddUint32(&pf.failureCount, 1)

	if failures >= 5 {
		atomic.StoreUint32(&pf.circuitOpen, 1)
		atomic.StoreInt64(&pf.lastFailTime, time.Now().Unix())
	}

	return pf.enqueueRetry(payload)
}

func (pf *PaymentForwarder) enqueueRetry(payload []byte) error {
	if pf.retryHandler != nil {
		return pf.retryHandler.EnqueueRetry(payload)
	}
	return nil
}

func InjectRequestedAt(jsonBody []byte, timestamp []byte) []byte {
	lastBrace := bytes.LastIndexByte(jsonBody, '}')
	if lastBrace == -1 {
		return jsonBody
	}

	result := make([]byte, 0, len(jsonBody)+len(timestamp)+len(requestedAtPrefix)+len(requestedAtSuffix))
	result = append(result, jsonBody[:lastBrace]...)
	result = append(result, requestedAtPrefix...)
	result = append(result, timestamp...)
	result = append(result, requestedAtSuffix...)
	result = append(result, jsonBody[lastBrace:]...)
	return result
}
