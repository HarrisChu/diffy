package pkg

import (
	"strconv"
	"syscall"
)

type config struct {
	address                  string
	user                     string
	password                 string
	statement                string
	concurrency              int
	iterationsPerConcurrency int
}

func NewConfigFromEnv() *config {
	return &config{
		address:                  getEnv("NEBULA_ADDRESS", "localhost:8080"),
		user:                     getEnv("NEBULA_USER", "root"),
		password:                 getEnv("NEBULA_PASSWORD", "nebula"),
		statement:                getEnv("NEBULA_STATEMENT", ""),
		concurrency:              getEnvAsInt("NEBULA_CONCURRENCY", 1),
		iterationsPerConcurrency: getEnvAsInt("NEBULA_ITERATIONS_PER_CONCURRENCY", 10),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(name string, defaultVal int) int {
	if valueStr, exists := LookupEnv(name); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return defaultVal
}

func LookupEnv(key string) (string, bool) {
	return syscall.Getenv(key)
}
