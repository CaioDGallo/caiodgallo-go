package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/shopspring/decimal"
)

var (
	http200 = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	http400 = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
	http404 = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")

	httpOK = []byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n")
)

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId`
	Amount        decimal.Decimal `json:"amount"`
}

type HTTPServer struct {
	*gnet.BuiltinEventEngine
	instanceID    string
	db            *sql.DB
	summaryBuffer []byte
	pf            *PaymentForwarder
	rh            *RetryHandler
	plh           *PaymentLogHandler

	knownReqSize int32
	reqSizeSet   uint32

	knownReqAmountVal int64
	reqAmountValSet   uint32
}

func NewHTTPServer(instanceID string, db *sql.DB, mainPF *PaymentForwarder, rh *RetryHandler, sb []byte, plh *PaymentLogHandler) *HTTPServer {
	return &HTTPServer{
		instanceID:    instanceID,
		db:            db,
		summaryBuffer: sb,
		pf:            mainPF,
		rh:            rh,
		plh:           plh,
	}
}

func (hs *HTTPServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetNoDelay(true)
	return nil, gnet.None
}

func (hs *HTTPServer) OnTraffic(c gnet.Conn) gnet.Action {
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
			log.Default().Println("handleSummary")
			return hs.handleSummary(c, data)
		}
	}

	c.Write(http404)
	return gnet.None
}

func (hs *HTTPServer) handlePayment(c gnet.Conn, data []byte) gnet.Action {
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
		// TODO: register failed payment?
		return gnet.None
	}

	byteBody := data[bodyStart:]

	if atomic.LoadUint32(&hs.reqAmountValSet) != 1 {
		var paymentBody PaymentRequest
		err := json.Unmarshal(byteBody, &paymentBody)
		if err != nil {
			log.Fatal("failed to decode request body")
		}
		amount := paymentBody.Amount.Mul(decimal.NewFromInt(100)).IntPart()

		atomic.StoreInt64(&hs.knownReqAmountVal, amount)
		atomic.StoreUint32(&hs.reqAmountValSet, 1)
	}

	requestedAt := time.Now()
	err := hs.pf.ForwardPayment(byteBody, requestedAt)
	if err != nil {
		log.Default().Println("err ForwardPayment ", err.Error())
		err := hs.rh.EnqueueRetry(byteBody)
		if err != nil {
			log.Default().Println("failed enqueing retry: ", err.Error())
		}
	} else {
		// FIXME: Will need to get pp dinamically when we start using the fallback
		err := hs.plh.RegisterPayment("default")
		if err != nil {
			log.Default().Println("error registering payment", err.Error())
		}
	}

	c.Write(http200)
	return gnet.None
}

func (hs *HTTPServer) handleSummary(c gnet.Conn, data []byte) gnet.Action {
	// Find the query string start (after '?')
	queryStart := -1
	queryEnd := -1

	// Look for '?' in the first line (before first space or \r\n)
	for i := 4; i < len(data) && i < 200; i++ { // reasonable limit for URL length
		if data[i] == '?' {
			queryStart = i + 1
		} else if data[i] == ' ' || data[i] == '\r' || data[i] == '\n' {
			queryEnd = i
			break
		}
	}

	var fromParam, toParam []byte

	if queryStart > 0 && queryEnd > queryStart {
		queryString := data[queryStart:queryEnd]
		fromParam, toParam = parseQueryParams(queryString)
	}

	var fromTime, toTime time.Time
	var err error

	if len(fromParam) > 0 {
		fromTime, err = time.Parse(time.RFC3339, string(fromParam))
		if err != nil {
			c.Write(http400)
			return gnet.None
		}
	}

	if len(toParam) > 0 {
		toTime, err = time.Parse(time.RFC3339, string(toParam))
		if err != nil {
			c.Write(http400)
			return gnet.None
		}
	}

	paymentCount, err := hs.plh.GetPaymentCount(fromTime, toTime)
	if err != nil {
		log.Default().Println("error getting payment count", err.Error())
	}

	// FIXME: This might still be wrong ...
	totalInCents := paymentCount * int(hs.knownReqAmountVal)
	feePercentage := decimal.NewFromInt(int64(hs.pf.transactionFee)).Div(decimal.NewFromInt(100))
	feesInCents := decimal.NewFromInt(int64(totalInCents)).Mul(feePercentage)
	netAmountInCents := decimal.NewFromInt(int64(totalInCents)).Sub(feesInCents)
	amount := netAmountInCents.Div(decimal.NewFromInt(100))

	hs.summaryBuffer = hs.summaryBuffer[:0]
	hs.summaryBuffer = append(hs.summaryBuffer, httpOK...)

	json := fmt.Sprintf(
		`{"default":{ "totalRequests": %d,"totalAmount":%s},"fallback":{ "totalRequests": %d,"totalAmount":%d}}`,
		paymentCount, amount.StringFixed(2), 0, 0,
	)

	hs.summaryBuffer = fmt.Appendf(hs.summaryBuffer, "Content-Length: %d\r\n\r\n%s", len(json), json)
	c.Write(hs.summaryBuffer)
	return gnet.None
}

func parseQueryParams(query []byte) (from, to []byte) {
	i := 0
	for i < len(query) {
		// Find parameter name
		paramStart := i
		for i < len(query) && query[i] != '=' {
			i++
		}
		if i >= len(query) {
			break
		}

		paramName := query[paramStart:i]
		i++ // skip '='

		// Find parameter value
		valueStart := i
		for i < len(query) && query[i] != '&' {
			i++
		}
		paramValue := query[valueStart:i]

		// Check parameter names using byte comparison
		if len(paramName) == 4 && paramName[0] == 'f' && paramName[1] == 'r' &&
			paramName[2] == 'o' && paramName[3] == 'm' {
			from = paramValue
		} else if len(paramName) == 2 && paramName[0] == 't' && paramName[1] == 'o' {
			to = paramValue
		}

		if i < len(query) {
			i++ // skip '&'
		}
	}
	return
}
