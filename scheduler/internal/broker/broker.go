package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chahatsagarmain/distributed-web-crawler/common"
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

// SetupBroker declares the consistent hashing exchange, binds 3 queues to it,
// and declares the result exchange with the result queue.
func SetupBroker(ch *amqp.Channel) error {
	// 1. Declare consistent hashing exchange
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

	// 2. Declare and bind 3 queues
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

		// Bind queue to consistent hashing exchange.
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

	// 3. Declare result exchange
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


func ConsumeMessage(ch *amqp.Channel, dbchan chan []byte , depth int) {
	msg, ok, err := ch.Get(ResultQueue, true)
	if !ok || err != nil {
		log.Printf("ERROR: reading message %v", err)
		return
	}
	dbchan <- msg.Body
	var data common.UrlData
	if err := json.Unmarshal(msg.Body , data) ; err != nil{
		log.Printf("ERROR: Marshalling error for %v" , msg.Body)
	}
	if(data.Depth <= depth){
		InsertMessage(ch , data.Url , depth + 1)
	}
}

func InsertMessage(ch *amqp.Channel, urlStr string, depth int) error {
	msg := common.CrawlMessage{
		URL:   urlStr,
		Depth: depth,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal crawl message: %w", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		ConsistentHashingExchange, // exchange
		urlStr,                    // routing key (URL)
		false,                     // mandatory
		false,                     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to consistent hashing exchange: %w", err)
	}

	log.Printf("Published message to consistent hashing exchange: URL=%s, Depth=%d", urlStr, depth)
	return nil
}