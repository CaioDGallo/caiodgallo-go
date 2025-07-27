package main

//
// import (
// 	"encoding/json"
// 	"log"
//
// 	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
// 	"github.com/gofiber/fiber/v3"
// 	"github.com/shopspring/decimal"
// )
//
// func main() {
// 	// Initialize a new Fiber app
// 	app := fiber.New()
//
// 	app.Post("/payments", func(c fiber.Ctx) error {
// 		// Send a string response to the client
// 		return c.Send(nil)
// 	})
// 	// Define a route for the GET method on the root path '/'
// 	app.Get("/payments-summary", func(c fiber.Ctx) error {
// 		// Send a string response to the client
// 		defaultStats := &types.PaymentProcessorStats{
// 			TotalRequests: 0,
// 			TotalAmount:   types.NewJSONDecimal(decimal.NewFromInt(0)),
// 		}
// 		fallbackStats := &types.PaymentProcessorStats{
// 			TotalRequests: 0,
// 			TotalAmount:   types.NewJSONDecimal(decimal.NewFromInt(0)),
// 		}
// 		paymentsSummaryResp := &types.PaymentsSummaryResponse{
// 			Default:  *defaultStats,
// 			Fallback: *fallbackStats,
// 		}
//
// 		resp, err := json.Marshal(paymentsSummaryResp)
// 		if err != nil {
// 			log.Default().Fatal("failed marshaling response", err.Error())
// 		}
// 		return c.Send(resp)
// 	})
//
// 	// Start the server on port 3000
// 	log.Fatal(app.Listen(":8080"))
// }
