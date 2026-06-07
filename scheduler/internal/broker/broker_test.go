package broker

import (
	"os"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestSetupBroker(t *testing.T) {
	// Retrieve rabbitmq url from env or use default
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(rabbitURI)
	if err != nil {
		t.Skip("Skipping test; RabbitMQ is not running or accessible")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = SetupBroker(ch)
	if err != nil {
		t.Fatalf("SetupBroker failed: %v", err)
	}
}
