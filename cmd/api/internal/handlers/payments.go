package handlers

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/workers"
)

type PaymentHandler struct {
	WorkerPools *workers.WorkerPools
	DefaultFee  float64
}

func NewPaymentHandler(workerPools *workers.WorkerPools, defaultFee float64) *PaymentHandler {
	return &PaymentHandler{
		WorkerPools: workerPools,
		DefaultFee:  defaultFee,
	}
}

func (h *PaymentHandler) CreatePayment(c fiber.Ctx) error {
	var paymentRequest types.PaymentRequest
	requestedAt := time.Now()

	body := c.Body()

	if err := json.Unmarshal(body, &paymentRequest); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Invalid JSON",
		})
	}

	task := types.PaymentTask{
		Request:     paymentRequest,
		RequestedAt: requestedAt,
		Fee:         h.DefaultFee,
		Processor:   "default",
	}

	select {
	case h.WorkerPools.PaymentTaskChannel <- task:
	default:
		return c.Status(503).JSON(fiber.Map{
			"error": "Payment processing queue is full",
		})
	}

	return c.Send(nil)
}
