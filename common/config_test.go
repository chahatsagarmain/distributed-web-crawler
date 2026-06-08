package common

import (
	"testing"
)

func TestConfigLoading(t *testing.T) {
	// Check if the auto-loaded AppConfig has the expected values from our .env file
	if AppConfig.MongoURI == "" {
		t.Error("Expected MongoURI to be loaded, but got empty string")
	}
	if AppConfig.RedisAddr == "" {
		t.Error("Expected RedisAddr to be loaded, but got empty string")
	}
	if AppConfig.RabbitMQURI == "" {
		t.Error("Expected RabbitMQURI to be loaded, but got empty string")
	}
}
