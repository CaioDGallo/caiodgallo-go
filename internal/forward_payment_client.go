package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type PaymentForwarder struct {
	client   *http.Client
	endpoint string

	bufferPool sync.Pool

	failureCount uint32
	lastFailTime int64
	circuitOpen  uint32

	totalRequests uint64
	totalFailures uint64

	retryHandler *RetryHandler
}

func NewPaymentForwarder(endpoint string, retryHandler *RetryHandler) *PaymentForwarder {
	transport := &http.Transport{
		MaxIdleConns:        100,              // High idle connections
		MaxIdleConnsPerHost: 100,              // All to same host
		MaxConnsPerHost:     0,                // No limit
		IdleConnTimeout:     90 * time.Second, // Keep connections alive

		ResponseHeaderTimeout: 2 * time.Second,
		ExpectContinueTimeout: 0, // Disable Expect: 100-continue

		DisableCompression: true,  // No compression overhead
		DisableKeepAlives:  false, // Keep connections alive

		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,  // Fast connection timeout
			KeepAlive: 30 * time.Second, // TCP keepalive
			DualStack: false,            // IPv4 only if possible
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	return &PaymentForwarder{
		client: &http.Client{
			Transport: transport,
			Timeout:   3 * time.Second,

			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint:     endpoint,
		retryHandler: retryHandler,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 4096)
			},
		},
	}
}

func (pf *PaymentForwarder) ForwardPayment(payload []byte) error {
	if atomic.LoadUint32(&pf.circuitOpen) == 1 {
		if time.Now().Unix()-atomic.LoadInt64(&pf.lastFailTime) < 30 {
			return pf.retryHandler.EnqueueRetry(payload)
		}
		atomic.StoreUint32(&pf.circuitOpen, 0)
		atomic.StoreUint32(&pf.failureCount, 0)
	}

	req, err := http.NewRequest("POST", pf.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(payload)))
	req.Close = false

	atomic.AddUint64(&pf.totalRequests, 1)
	resp, err := pf.client.Do(req)
	if err != nil {
		log.Default().Println("error doing request ", err.Error())
		return pf.handleFailure(payload, err)
	}

	buf := pf.bufferPool.Get().([]byte)
	defer pf.bufferPool.Put(buf)

	_, err = io.CopyBuffer(io.Discard, resp.Body, buf)
	if err != nil {
		log.Default().Println(err.Error())
	}
	resp.Body.Close()

	if resp.StatusCode >= 500 {
		return pf.handleFailure(payload, fmt.Errorf("server error: %d", resp.StatusCode))
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

			pf.ForwardPayment(data)
		}(payload)
	}

	wg.Wait()
}

func (pf *PaymentForwarder) handleFailure(payload []byte, err error) error {
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

func (pf *PaymentForwarder) ForwardPaymentWithContext(ctx context.Context, payload []byte) error {
	if atomic.LoadUint32(&pf.circuitOpen) == 1 {
		return pf.enqueueRetry(payload)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", pf.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := pf.client.Do(req)
	if err != nil {
		return pf.handleFailure(payload, err)
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode >= 500 {
		return pf.handleFailure(payload, fmt.Errorf("status: %d", resp.StatusCode))
	}

	return nil
}

func (pf *PaymentForwarder) Stats() (requests, failures uint64, circuitOpen bool) {
	return atomic.LoadUint64(&pf.totalRequests),
		atomic.LoadUint64(&pf.totalFailures),
		atomic.LoadUint32(&pf.circuitOpen) == 1
}
