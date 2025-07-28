package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/client"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/config"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/database"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/handlers"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/router"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/workers"
)

func main() {
	db, preparedStmts, err := database.SetupDatabase(config.DatabaseConnectionString)
	if err != nil {
		log.Fatal("failed to setup database: ", err)
	}
	defer db.Close()
	defer preparedStmts.Close()

	httpClient := client.NewHTTPClient()

	defaultFee, err := client.GetProcessorFee(httpClient, config.DefaultProcessorEndpoint)
	if err != nil {
		log.Fatal("failed getting the default processor transaction fee: ", err)
	}

	fallbackFee, err := client.GetProcessorFee(httpClient, config.FallbackProcessorEndpoint)
	if err != nil {
		log.Fatal("failed getting the fallback processor transaction fee: ", err)
	}

	workerPools, err := workers.NewWorkerPools(db, config.DefaultProcessorEndpoint, config.FallbackProcessorEndpoint, defaultFee, fallbackFee, httpClient, nil, nil, nil)
	if err != nil {
		log.Fatal("failed to initialize worker pools: ", err)
	}

	defaultCB, fallbackCB, retryCB := client.NewCircuitBreakers(workerPools.TriggerRetries)

	workerPools.SetCircuitBreakers(defaultCB, fallbackCB, retryCB)

	workerPools.StartRetryWorker()
	workerPools.StartHealthCheckWorker()
	workerPools.StartPaymentConsumers()

	paymentHandler := handlers.NewPaymentHandler(workerPools, defaultFee)
	statsHandler := handlers.NewStatsHandler(preparedStmts)

	app := router.SetupRoutes(paymentHandler, statsHandler)

	go func() {
		if err := app.Listen(config.ServerAddress); err != nil {
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
