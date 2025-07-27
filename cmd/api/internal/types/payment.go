package types

import (
	"strings"
	"time"

	"github.com/shopspring/decimal"
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

type PaymentTask struct {
	Request     PaymentRequest
	RequestedAt time.Time
	Fee         float64
	Processor   string
}

type ProcessorHealth struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
	LastChecked     time.Time
	IsValid         bool
}