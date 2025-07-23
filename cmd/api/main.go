package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/CaioDGallo/caiodgallo-go/internal"
	_ "github.com/mattn/go-sqlite3"
	"github.com/panjf2000/gnet/v2"
)

var (
	http200 = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	http400 = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
	http404 = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")

	httpOK = []byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n")

	totalPayments  uint64
	totalAmount    uint64
	failedPayments uint64
)

type httpServer struct {
	*gnet.BuiltinEventEngine
	instanceID    string
	db            *sql.DB
	summaryBuffer []byte
	pf            *internal.PaymentForwarder
	rh            *internal.RetryHandler

	knownReqSize int32
	reqSizeSet   uint32
}

func (hs *httpServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetNoDelay(true)
	return nil, gnet.None
}

func (hs *httpServer) OnTraffic(c gnet.Conn) gnet.Action {
	data, _ := c.Next(-1)
	if len(data) < 14 {
		c.Write(http400)
		return gnet.None
	}

	switch data[0] {
	case 'P':
		if data[5] == '/' && data[6] == 'p' && data[7] == 'a' && data[14] == ' ' {
			return hs.handlePayment(c, data)
		}
	case 'G':
		if data[4] == '/' && data[5] == 'p' && data[20] == 'y' {
			return hs.handleSummary(c)
		}
	}
	log.Default().Println(string(data), string(data[0]), string(data[4]), string(data[5]), string(data[20]), string(data[21]), string(data[22]))

	c.Write(http404)
	return gnet.None
}

func (hs *httpServer) handlePayment(c gnet.Conn, data []byte) gnet.Action {
	bodyStart := 0

	if atomic.LoadUint32(&hs.reqSizeSet) == 1 {
		headerSize := int(atomic.LoadInt32(&hs.knownReqSize))
		if len(data) > headerSize {
			bodyStart = headerSize
		}
	} else {
		for i := 0; i < len(data)-4 && i < 1024; i++ {
			if data[i] == '\r' && data[i+1] == '\n' && data[i+2] == '\r' && data[i+3] == '\n' {
				bodyStart = i + 4
				atomic.StoreInt32(&hs.knownReqSize, int32(bodyStart))
				atomic.StoreUint32(&hs.reqSizeSet, 1)
				break
			}
		}
	}

	if bodyStart == 0 || bodyStart >= len(data) {
		c.Write(http400)
		atomic.AddUint64(&failedPayments, 1)
		return gnet.None
	}

	byteBody := data[bodyStart:]
	amountStartingIdx := 65
	if byteBody[2] != 'c' {
		amountStartingIdx = 10
	}

	log.Default().Println(string(byteBody[65]), string(byteBody[2]), string(byteBody[10]))

	dotIdx := 0
	amountEndIdx := amountStartingIdx
	for i := amountStartingIdx; i < len(byteBody); i++ {
		b := byteBody[i]

		if b == '.' {
			dotIdx = i
		}

		if b == ' ' || b == ',' || b == '}' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v' {
			amountEndIdx = i - 1
			break
		}
	}

	amountByteArr := append(byteBody[amountStartingIdx:dotIdx], byteBody[dotIdx+1:amountEndIdx+1]...)

	err := hs.pf.ForwardPayment(byteBody)
	if err != nil {
		log.Default().Println("err ForwardPayment ", err.Error())
		err := hs.rh.EnqueueRetry(byteBody)
		if err != nil {
			log.Default().Println("failed enqueing retry: ", err.Error())
		}
	} else {
		atomic.AddUint64(&totalPayments, 1)

		amount, err := strconv.ParseUint(string(amountByteArr), 10, 64)
		if err != nil {
			log.Default().Println("error parsing amount", err.Error())
		}
		atomic.AddUint64(&totalAmount, amount)
	}

	c.Write(http200)
	return gnet.None
}

func (hs *httpServer) handleSummary(c gnet.Conn) gnet.Action {
	// payments := atomic.LoadUint64(&totalPayments)
	amount := atomic.LoadUint64(&totalAmount)
	totalRequests := hs.pf.GetTotalRequests()
	// failed := atomic.LoadUint64(&failedPayments)

	hs.summaryBuffer = hs.summaryBuffer[:0]
	hs.summaryBuffer = append(hs.summaryBuffer, httpOK...)

	json := fmt.Sprintf(
		`{"default":{ "totalRequests": %d,"totalAmount":%d},"fallback":{ "totalRequests": %d,"totalAmount":%d}}`,
		totalRequests, amount, 0, 0,
	)

	hs.summaryBuffer = fmt.Appendf(hs.summaryBuffer, "Content-Length: %d\r\n\r\n%s", len(json), json)
	c.Write(hs.summaryBuffer)
	return gnet.None
}

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

	hs := &httpServer{
		instanceID:    instanceID,
		db:            db,
		summaryBuffer: make([]byte, 0, 256),
		pf:            mainPF,
		rh:            rh,
	}

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
