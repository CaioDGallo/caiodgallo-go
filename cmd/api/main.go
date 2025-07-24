package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"

	"github.com/gofiber/fiber/v3"
)

const (
	getAmountAndFee = "SELECT amount, fee FROM payment_log LIMIT 1"
	insertPayment   = "INSERT INTO payment_log (payment_processor, amount, fee, requested_at) VALUES ($1, $2, $3, $4)"
	countBoth       = "SELECT COUNT(*) FROM payment_log WHERE requested_at >= $1 AND requested_at <= $2"
	countFrom       = "SELECT COUNT(*) FROM payment_log WHERE requested_at >= $1"
	countTo         = "SELECT COUNT(*) FROM payment_log WHERE requested_at <= $1"
	countAll        = "SELECT COUNT(*) FROM payment_log"
)

type AdminTransactionSummary struct {
	TotalRequests     int             `json:"totalRequests"`
	TotalAmount       decimal.Decimal `json:"totalAmount"`
	TotalFee          decimal.Decimal `json:"totalFee"`
	FeePerTransaction decimal.Decimal `json:"feePerTransaction"`
}

type PaymentsSummaryResponse struct {
	Default  PaymentProcessorStats `json:"default"`
	Fallback PaymentProcessorStats `json:"fallback"`
}

// FIXME: float64 might be imprecise
type PaymentProcessorStats struct {
	TotalRequests int         `json:"totalRequests"`
	TotalAmount   JSONDecimal `json:"totalAmount"`
}

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentProcessorPaymentRequest struct {
	PaymentRequest
	RequestedAt string `json:"requestedAt"`
}

type JSONDecimal struct {
	decimal.Decimal
}

func (d JSONDecimal) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *JSONDecimal) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)
	dec, err := decimal.NewFromString(str)
	if err != nil {
		return err
	}
	d.Decimal = dec
	return nil
}

func NewJSONDecimal(d decimal.Decimal) JSONDecimal {
	return JSONDecimal{Decimal: d}
}

var client http.Client

func main() {
	connStr := "postgresql://dev:secret123@postgres/onecent?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Default().Fatal("failed to open db connection", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	_, err = db.Exec(`
				CREATE TABLE IF NOT EXISTS payment_log(
						id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
						amount DECIMAL NOT NULL,
						fee DECIMAL NOT NULL,
						payment_processor TEXT,
						requested_at TIMESTAMP
				);
				CREATE INDEX IF NOT EXISTS idx_requested_at ON payment_log(requested_at);
    `)
	if err != nil {
		log.Default().Fatal("failed to create tables", err)
	}

	getAmountAndFeePreparedStmt, err := db.Prepare(getAmountAndFee)
	if err != nil {
		log.Default().Fatal("failed to prepare get amount and fee statement", err)
	}
	defer getAmountAndFeePreparedStmt.Close()

	insertPreparedStmt, err := db.Prepare(insertPayment)
	if err != nil {
		log.Default().Fatal("failed to prepare insert statement", err)
	}
	defer insertPreparedStmt.Close()

	countAllPreparedStmt, err := db.Prepare(countAll)
	if err != nil {
		log.Default().Fatal("failed to prepare count all statement", err)
	}
	defer countAllPreparedStmt.Close()

	countFromPreparedStmt, err := db.Prepare(countFrom)
	if err != nil {
		log.Default().Fatal("failed to prepare count from statement", err)
	}
	defer countFromPreparedStmt.Close()

	countToPreparedStmt, err := db.Prepare(countTo)
	if err != nil {
		log.Default().Fatal("failed to prepare count to statement", err)
	}
	defer countToPreparedStmt.Close()

	countBothPreparedStmt, err := db.Prepare(countBoth)
	if err != nil {
		log.Default().Fatal("failed to prepare count both statement", err)
	}
	defer countBothPreparedStmt.Close()

	transport := &http.Transport{
		MaxIdleConns:        20,               // High idle connections
		MaxIdleConnsPerHost: 20,               // All to same host
		MaxConnsPerHost:     0,                // No limit
		IdleConnTimeout:     90 * time.Second, // Keep connections alive

		ResponseHeaderTimeout: 750 * time.Millisecond, // Was 2s

		ExpectContinueTimeout: 0, // Disable Expect: 100-continue

		DisableCompression: true,  // No compression overhead
		DisableKeepAlives:  false, // Keep connections alive

		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond, // Faster connection timeout
			KeepAlive: 30 * time.Second,       // TCP keepalive
			DualStack: false,                  // IPv4 only if possible
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   1000 * time.Millisecond,

		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	defaultPPEndpoint := "http://payment-processor-default:8080"
	transactionFee, err := GetPPFee(defaultPPEndpoint)
	if err != nil {
		log.Default().Fatal("failed getting the transaction fee ", err)
	}

	app := fiber.New()

	app.Post("/payments", func(c fiber.Ctx) error {
		var paymentRequest PaymentRequest
		requestedAt := time.Now()
		requestedAtString := requestedAt.Format("2006-01-02T15:04:05.000Z")

		body := c.Body()

		// Manually unmarshal
		if err := json.Unmarshal(body, &paymentRequest); err != nil {
			return c.Status(400).JSON(fiber.Map{
				"error": "Invalid JSON",
			})
		}

		ppPaymentRequest := &PaymentProcessorPaymentRequest{
			PaymentRequest: paymentRequest,
			RequestedAt:    requestedAtString,
		}

		ppPayload, err := json.Marshal(ppPaymentRequest)
		if err != nil {
			log.Default().Println("error marshaling request ", err.Error())
		}

		req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", defaultPPEndpoint), bytes.NewReader(ppPayload))
		if err != nil {
			log.Default().Println("error creating request ", err.Error())
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Length", strconv.Itoa(len(ppPayload)))

		resp, err := client.Do(req)
		if err != nil {
			log.Default().Println("error doing request ", err.Error())
		} else {

			defer resp.Body.Close()

			_, err = insertPreparedStmt.Exec("default", paymentRequest.Amount, transactionFee, requestedAt)
			if err != nil {
				log.Printf("Failed to register payment: %v", err)
			}
		}

		return c.Send(nil)
	})

	app.Get("/payments-summary", func(c fiber.Ctx) error {
		fromParam := c.Query("from")
		toParam := c.Query("to")

		var fromTime, toTime time.Time
		var err error

		if fromParam != "" {
			fromTime, err = time.Parse("2006-01-02T15:04:05.000Z", fromParam)
			if err != nil {
				log.Default().Fatal("failed parsing from", err.Error())
			}
		}

		if toParam != "" {
			toTime, err = time.Parse("2006-01-02T15:04:05.000Z", toParam)
			if err != nil {
				log.Default().Fatal("failed parsing to", err.Error())
			}
		}

		hasFrom := !fromTime.IsZero()
		hasTo := !toTime.IsZero()

		var result *sql.Row

		switch {
		case hasFrom && hasTo:
			result = countBothPreparedStmt.QueryRow(fromTime, toTime)
		case hasFrom:
			result = countFromPreparedStmt.QueryRow(fromTime)
		case hasTo:
			result = countToPreparedStmt.QueryRow(toTime)
		default:
			result = countAllPreparedStmt.QueryRow()
		}

		var count int64
		err = result.Scan(&count)
		if err != nil {
			log.Default().Println("failed counting payments", err.Error())
		}

		var paymentAmountString, feeString string
		err = getAmountAndFeePreparedStmt.QueryRow().Scan(&paymentAmountString, &feeString)
		if err != nil {
			log.Default().Println("failed getting amount and fee", err.Error())
		}

		paymentAmount, _ := decimal.NewFromString(paymentAmountString)
		// feePercent, _ := decimal.NewFromString(feeString)
		totalPayments := decimal.NewFromInt(count)

		totalGross := paymentAmount.Mul(totalPayments)

		// feeAmount := totalGross.Mul(feePercent)
		//
		// totalNet := totalGross.Sub(feeAmount)

		defaultStats := &PaymentProcessorStats{
			TotalRequests: int(count),
			TotalAmount:   NewJSONDecimal(totalGross),
		}
		fallbackStats := &PaymentProcessorStats{
			TotalRequests: 0,
			TotalAmount:   NewJSONDecimal(decimal.NewFromInt(0)),
		}

		paymentsSummaryResp := &PaymentsSummaryResponse{
			Default:  *defaultStats,
			Fallback: *fallbackStats,
		}

		resp, err := json.Marshal(paymentsSummaryResp)
		if err != nil {
			log.Default().Fatal("failed marshaling response", err.Error())
		}

		return c.Send(resp)
	})

	log.Fatal(app.Listen(":8080"))
}

func GetPPFee(processorURL string) (float64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/admin/payments-summary", processorURL), nil)
	if err != nil {
		log.Default().Println("error creating request GetPPFee", err.Error())
	}

	req.Header.Set("X-Rinha-Token", "123")

	resp, err := client.Do(req)
	if err != nil {
		log.Default().Println("error doing request GetPPFee", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("request failed with status: %s\n", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Default().Println("error reading body GetPPFee", err.Error())
	}

	var summary AdminTransactionSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		log.Default().Println("error unmarshaling request GetPPFee", err.Error())
	}

	floatFee, exact := summary.FeePerTransaction.Float64()
	if !exact {
		log.Default().Println("fee conversion not exact to float64 GetPPFee ", exact)
	}

	return floatFee, nil
}
