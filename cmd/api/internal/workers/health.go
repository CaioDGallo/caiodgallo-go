package workers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
)

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
		log.Printf("Error creating health check request for %s: %v", processorType, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := wp.Client.Do(req)
	if err != nil {
		log.Printf("Health check failed for %s processor: %v", processorType, err)
		wp.updateProcessorHealth(processorType, types.ProcessorHealth{IsValid: false})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		log.Printf("Health check rate limited for %s processor", processorType)
		return
	}

	if resp.StatusCode != 200 {
		log.Printf("Health check returned status %d for %s processor", resp.StatusCode, processorType)
		wp.updateProcessorHealth(processorType, types.ProcessorHealth{IsValid: false})
		return
	}

	var health types.ProcessorHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		log.Printf("Error decoding health response for %s processor: %v", processorType, err)
		wp.updateProcessorHealth(processorType, types.ProcessorHealth{IsValid: false})
		return
	}

	health.LastChecked = time.Now()
	health.IsValid = true

	wp.updateProcessorHealth(processorType, health)

	if processorType == "default" && !health.Failing {
		previousHealth := wp.getProcessorHealth("default")
		if !previousHealth.IsValid || previousHealth.Failing {
			log.Printf("Default processor became healthy - triggering priority retry burst")
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