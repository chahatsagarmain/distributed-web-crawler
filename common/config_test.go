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

	expectedMongoURI := "mongodb://admin:password@localhost:27017"
	if AppConfig.MongoURI != expectedMongoURI {
		t.Errorf("Expected MongoURI to be %q, but got %q", expectedMongoURI, AppConfig.MongoURI)
	}

	expectedRedisAddr := "localhost:6379"
	if AppConfig.RedisAddr != expectedRedisAddr {
		t.Errorf("Expected RedisAddr to be %q, but got %q", expectedRedisAddr, AppConfig.RedisAddr)
	}

	expectedRabbitMQURI := "amqp://guest:guest@localhost:5672/"
	if AppConfig.RabbitMQURI != expectedRabbitMQURI {
		t.Errorf("Expected RabbitMQURI to be %q, but got %q", expectedRabbitMQURI, AppConfig.RabbitMQURI)
	}
}
