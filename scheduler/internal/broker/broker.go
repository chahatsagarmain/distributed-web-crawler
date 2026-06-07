package broker

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ConsistentHashingExchange = "consistent_hashing"
	ResultExchange            = "result_exchange"
	ResultQueue               = "result_queue"
	ResultRoutingKey          = "result"
)

var Queues = []string{"queue_1", "queue_2", "queue_3"}


func StartPublishing(url string , depth int){
	
}

func SetupBroker(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		ConsistentHashingExchange, // name
		"x-consistent-hash",       // type
		true,                      // durable
		false,                     // auto-deleted
		false,                     // internal
		false,                     // no-wait
		nil,                       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare consistent hashing exchange: %w", err)
	}
	log.Printf("Declared consistent hashing exchange: %s", ConsistentHashingExchange)

	for _, qName := range Queues {
		q, err := ch.QueueDeclare(
			qName, // name
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", qName, err)
		}

		err = ch.QueueBind(
			q.Name,                    // queue name
			"1",                      // routing key (weight)
			ConsistentHashingExchange, // exchange name
			false,                     // no-wait
			nil,                       // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to bind queue %s to exchange %s: %w", qName, ConsistentHashingExchange, err)
		}
		log.Printf("Declared and bound queue: %s to %s with weight 10", qName, ConsistentHashingExchange)
	}

	err = ch.ExchangeDeclare(
		ResultExchange, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare result exchange: %w", err)
	}
	log.Printf("Declared result exchange: %s", ResultExchange)

	// 4. Declare result queue
	rq, err := ch.QueueDeclare(
		ResultQueue, // name
		true,        // durable
		false,       // auto-delete
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare result queue %s: %w", ResultQueue, err)
	}

	// 5. Bind result queue to result exchange
	err = ch.QueueBind(
		rq.Name,          // queue name
		ResultRoutingKey, // routing key
		ResultExchange,   // exchange name
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind result queue %s to exchange %s: %w", ResultQueue, ResultExchange, err)
	}
	log.Printf("Declared and bound result queue: %s to %s with routing key %s", ResultQueue, ResultExchange, ResultRoutingKey)

	return nil
}