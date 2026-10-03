package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	AppEnv               string
	LogLevel             string
	GRPCPort             string
	BMKGBaseURL          string
	PVMBGBaseURL         string
	BMKGAPIKey           string
	PVMBGBearerToken     string
	ServiceToken         string
	AuthPublicKeyPEM     string
	AuthPublicKeyPath    string
	BMKGPollInterval     time.Duration
	PVMBGPollInterval    time.Duration
	BMKGRequestTimeout   time.Duration
	PVMBGRequestTimeout  time.Duration
	SourceStaleThreshold time.Duration
	CanonicalStoreDSN    string
	AMQPURL              string
	RabbitMQExchange     string
}

func Load() *Config {
	return &Config{
		AppEnv:               getEnv("APP_ENV", "development"),
		LogLevel:             getEnv("LOG_LEVEL", "info"),
		GRPCPort:             getEnv("AGGREGATOR_GRPC_PORT", "9091"),
		BMKGBaseURL:          getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		PVMBGBaseURL:         getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		BMKGAPIKey:           getEnv("BMKG_API_KEY", "REPLACE_BMKG_ONLY"),
		PVMBGBearerToken:     getEnv("PVMBG_BEARER_TOKEN", "REPLACE_PVMBG_ONLY"),
		ServiceToken:         getEnv("AGGREGATOR_SERVICE_TOKEN", "REPLACE_AGGREGATOR_SERVICE_TOKEN"),
		AuthPublicKeyPEM:     getEnv("AUTH_PUBLIC_KEY", ""),
		AuthPublicKeyPath:    getEnv("AUTH_PUBLIC_KEY_PATH", ""),
		BMKGPollInterval:     time.Duration(getEnvInt("BMKG_POLL_INTERVAL_MS", 3000)) * time.Millisecond,
		PVMBGPollInterval:    time.Duration(getEnvInt("PVMBG_POLL_INTERVAL_MS", 3000)) * time.Millisecond,
		BMKGRequestTimeout:   time.Duration(getEnvInt("BMKG_REQUEST_TIMEOUT_MS", 1000)) * time.Millisecond,
		PVMBGRequestTimeout:  time.Duration(getEnvInt("PVMBG_REQUEST_TIMEOUT_MS", 3500)) * time.Millisecond,
		SourceStaleThreshold: time.Duration(getEnvInt("SOURCE_STALE_AFTER_MS", 10000)) * time.Millisecond,
		CanonicalStoreDSN:    getEnv("CANONICAL_STORE_DSN", "postgres://postgres:postgres@localhost:5432/bnpb_canonical?sslmode=disable"),
		AMQPURL:              getEnv("AGGREGATOR_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQExchange:     getEnv("RABBITMQ_EXCHANGE", "bnpb.hazards.v1"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
