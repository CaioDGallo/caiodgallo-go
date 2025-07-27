package client

import (
	"time"

	"github.com/sony/gobreaker/v2"
)

func NewCircuitBreakers() (*gobreaker.CircuitBreaker[[]byte], *gobreaker.CircuitBreaker[[]byte]) {
	var defaultSettings gobreaker.Settings
	defaultSettings.Name = "Default Payments Breaker"
	defaultSettings.MaxRequests = 100
	defaultSettings.Interval = time.Duration(3 * time.Second)
	defaultSettings.Timeout = time.Duration(1500 * time.Millisecond)
	defaultSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 50 && failureRatio >= 0.6
	}

	defaultCB := gobreaker.NewCircuitBreaker[[]byte](defaultSettings)

	var fallbackSettings gobreaker.Settings
	fallbackSettings.Name = "Fallback Payments Breaker"
	fallbackSettings.MaxRequests = 100
	fallbackSettings.Interval = time.Duration(3 * time.Second)
	fallbackSettings.Timeout = time.Duration(1500 * time.Millisecond)
	fallbackSettings.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 50 && failureRatio >= 0.6
	}

	fallbackCB := gobreaker.NewCircuitBreaker[[]byte](fallbackSettings)

	return defaultCB, fallbackCB
}

