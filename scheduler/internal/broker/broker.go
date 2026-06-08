package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/cache"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
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
			"1",                       // routing key (weight)
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

func ConsumeMessage(ch *amqp.Channel, rdb *redis.Client, dbchan chan []byte, maxDepth int) {
	msg, ok, err := ch.Get(ResultQueue, true)
	if !ok || err != nil {
		return
	}
	dbchan <- msg.Body

	var data common.UrlData
	if err := json.Unmarshal(msg.Body, &data); err != nil {
		log.Printf("ERROR unmarshalling result message: %v", err)
		db.DecrementPending(rdb)
		return
	}

	if data.Depth < maxDepth {
		for _, nextURL := range data.NextUrls {
			res, err := cache.CheckUrlDuplicate(rdb, nextURL)
			if err == nil && !res {
				_, err = db.IncrementPending(rdb, 1)
				if err == nil {
					err = InsertMessage(ch, nextURL, data.Depth, maxDepth)
					if err != nil {
						log.Printf("ERROR enqueuing link %s: %v", nextURL, err)
						db.DecrementPending(rdb)
					}
				} else {
					log.Printf("ERROR incrementing pending count: %v", err)
				}
			}
		}
	}

	db.DecrementPending(rdb)
}

func InsertMessage(ch *amqp.Channel, urlStr string, currentDepth, maxDepth int) error {
	msg := common.CrawlMessage{
		URL:          urlStr,
		CurrentDepth: currentDepth,
		MaxDepth:     maxDepth,
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

	log.Printf("Published message to consistent hashing exchange: URL=%s, CurrentDepth=%d, MaxDepth=%d", urlStr, currentDepth, maxDepth)
	return nil
}
