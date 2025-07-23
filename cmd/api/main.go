package main

import (
	"log"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/CaioDGallo/caiodgallo-go/internal"
	_ "github.com/mattn/go-sqlite3"
	"github.com/panjf2000/gnet/v2"
)

func main() {
	debug.SetGCPercent(100)
	debug.SetMemoryLimit(90 * 1024 * 1024)
	runtime.GOMAXPROCS(1)

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	db, err := internal.InitDatabase("app-" + instanceID)
	if err != nil {
		log.Fatal("Failed to init database:", err)
	}
	defer db.Close()

	rh := internal.NewRetryHandler(db)
	defer rh.Cleanup()

	go rh.ProcessRetryQueue()

	mainPF := internal.NewPaymentForwarder("http://payment-processor-default:8080", rh)
	// fallbackPF := internal.NewPaymentForwarder("http://payment-processor-fallback:8080", rh)

	rh.SetPaymentForwarder(mainPF)

	plh := internal.NewPaymentLogHandler(db)

	rh.SetPaymentLogHandler(plh)

	hs := internal.NewHTTPServer(
		instanceID,
		db,
		mainPF,
		rh,
		make([]byte, 0, 256),
		plh,
	)

	log.Printf("Starting low-latency server instance %s on :8080", instanceID)

	options := []gnet.Option{
		gnet.WithMulticore(false),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithReadBufferCap(2048),
		gnet.WithWriteBufferCap(2048),
	}

	log.Fatal(gnet.Run(hs, "tcp://:8080", options...))
}
