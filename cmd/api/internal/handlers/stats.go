package handlers

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/shopspring/decimal"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/database"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
)

type StatsHandler struct {
	PreparedStmts *database.PreparedStatements
}

func NewStatsHandler(preparedStmts *database.PreparedStatements) *StatsHandler {
	return &StatsHandler{
		PreparedStmts: preparedStmts,
	}
}

func (h *StatsHandler) GetPaymentsSummary(c fiber.Ctx) error {
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

	var rows *sql.Rows

	switch {
	case hasFrom && hasTo:
		rows, err = h.PreparedStmts.StatsBoth.Query(fromTime, toTime)
	case hasFrom:
		rows, err = h.PreparedStmts.StatsFrom.Query(fromTime)
	case hasTo:
		rows, err = h.PreparedStmts.StatsTo.Query(toTime)
	default:
		rows, err = h.PreparedStmts.StatsAll.Query()
	}

	if err != nil {
		log.Default().Printf("failed querying payment stats: %v", err)
		return c.Status(500).Send([]byte("Internal server error"))
	}
	defer rows.Close()

	defaultStats := &types.PaymentProcessorStats{
		TotalRequests: 0,
		TotalAmount:   types.NewJSONDecimal(decimal.NewFromInt(0)),
	}
	fallbackStats := &types.PaymentProcessorStats{
		TotalRequests: 0,
		TotalAmount:   types.NewJSONDecimal(decimal.NewFromInt(0)),
	}

	for rows.Next() {
		var processor string
		var count int64
		var totalAmountString string

		err = rows.Scan(&processor, &count, &totalAmountString)
		if err != nil {
			log.Default().Printf("failed scanning payment stats row: %v", err)
			continue
		}

		totalAmount, err := decimal.NewFromString(totalAmountString)
		if err != nil {
			log.Default().Printf("failed parsing total amount: %v", err)
			totalAmount = decimal.NewFromInt(0)
		}

		stats := &types.PaymentProcessorStats{
			TotalRequests: int(count),
			TotalAmount:   types.NewJSONDecimal(totalAmount),
		}

		if processor == "default" {
			defaultStats = stats
		} else if processor == "fallback" {
			fallbackStats = stats
		}
	}

	paymentsSummaryResp := &types.PaymentsSummaryResponse{
		Default:  *defaultStats,
		Fallback: *fallbackStats,
	}

	resp, err := json.Marshal(paymentsSummaryResp)
	if err != nil {
		log.Default().Fatal("failed marshaling response", err.Error())
	}

	return c.Send(resp)
}