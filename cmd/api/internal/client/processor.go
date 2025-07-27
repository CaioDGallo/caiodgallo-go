package client

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/CaioDGallo/caiodgallo-go/cmd/api/internal/types"
)

func GetProcessorFee(client *http.Client, processorURL string) (float64, error) {
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

	var summary types.AdminTransactionSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		log.Default().Println("error unmarshaling request GetPPFee", err.Error())
	}

	floatFee, exact := summary.FeePerTransaction.Float64()
	if !exact {
		log.Default().Println("fee conversion not exact to float64 GetPPFee ", exact)
	}

	return floatFee, nil
}