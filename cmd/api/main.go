package main

import (
	"log"
	"runtime"
	"runtime/debug"

	"github.com/CaioDGallo/caiodgallo-go/internal"
	"github.com/panjf2000/gnet/v2"
)

func main() {
	debug.SetGCPercent(100)
	debug.SetMemoryLimit(90 * 1024 * 1024)
	runtime.GOMAXPROCS(1)

	rph := internal.NewRedisPaymentHandler(nil)

	mainPF := internal.NewPaymentForwarder("http://payment-processor-default:8080", rph)
	// fallbackPF := internal.NewPaymentForwarder("http://payment-processor-fallback:8080", rph)

	rph.SetPaymentForwarder(mainPF)

	// go rph.ProcessRetryQueue()

	hs := internal.NewHTTPServer(
		rph,
		mainPF,
		make([]byte, 0, 256),
	)

	log.Printf("Starting low-latency server instance on port :8080")

	options := []gnet.Option{
		gnet.WithMulticore(false),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithReadBufferCap(2048),
		gnet.WithWriteBufferCap(2048),
	}

	log.Fatal(gnet.Run(hs, "tcp://:8080", options...))
}
