package config

import "os"

const (
	DefaultProcessorEndpoint  = "http://payment-processor-default:8080"
	FallbackProcessorEndpoint = "http://payment-processor-fallback:8080"
	DatabaseConnectionString  = "postgresql://dev:secret123@postgres/onecent?sslmode=disable"
	ServerAddress             = ":8080"
)

func GetInstanceID() string {
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}
	return instanceID
}