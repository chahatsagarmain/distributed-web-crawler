package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/cache"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	ConsistentHashingExchange = common.ConsistentHashingExchange
	ResultExchange            = common.ResultExchange
	ResultQueue               = common.ResultQueue
	ResultRoutingKey          = common.ResultRoutingKey
)

var Queues = common.Queues

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

// StartResultConsumer starts consuming from ResultQueue in the background, updating last activity and processing results.
// last ack time is required to detect inactivity and cleaning active job status
func StartResultConsumer(conn *common.Connections, dbchan chan []byte) {
	ch, err := conn.RabbitMQ.Conn.Channel()
	if err != nil {
		log.Fatalf("Scheduler: Failed to open RabbitMQ channel for result consumer: %v", err)
	}

	msgs, err := ch.Consume(
		ResultQueue,
		"",    // consumer name
		true,  // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Scheduler: Failed to consume from result queue: %v", err)
	}

	log.Printf("Scheduler: Listening for results on queue: %s", ResultQueue)
	go func() {
		defer ch.Close()
		for msg := range msgs {
			processResult(ch, conn.RedisClient, dbchan, msg.Body)
		}
	}()
}

func processResult(ch *amqp.Channel, rdb *redis.Client, dbchan chan []byte, body []byte) {
	dbchan <- body // send for batch insert

	ctx := context.Background()
	err := rdb.Set(ctx, "crawler:last_activity_time", time.Now().Unix(), 1*time.Hour).Err()
	if err != nil {
		log.Printf("Scheduler WARNING: failed to update last activity time: %v", err)
	}

	var data common.UrlData
	if err := json.Unmarshal(body, &data); err != nil {
		log.Printf("Scheduler ERROR: unmarshalling result message: %v", err)
		return
	}

	maxDepth, err := db.GetMaxDepth(rdb)
	if err != nil {
		log.Printf("Scheduler WARNING: failed to get max depth from Redis: %v", err)
		maxDepth = data.Depth
	}

	// if depth is less than max depth then queue job
	if data.Depth < maxDepth {
		for _, nextURL := range data.NextUrls {
			res, err := cache.CheckUrlDuplicate(rdb, nextURL)
			if err == nil && !res {
				err = InsertMessage(ch, nextURL, data.Depth+1, maxDepth)
				if err != nil {
					log.Printf("Scheduler ERROR: enqueuing link %s: %v", nextURL, err)
				}
			}
		}
	}
}

// watch dog to check for clearing active after time intervals

func StartWatchdog(conn *common.Connections, interval time.Duration, idleTimeout int64) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			checkJobStatus(conn, idleTimeout)
		}
	}()
}

func checkJobStatus(conn *common.Connections, idleTimeout int64) {
	rdb := conn.RedisClient
	ctx := context.Background()

	// now we check for active job
	// create a temporary channel and conenct to each queue "passively"
	// and if all queues are empty and the last ack >= idletimeout then
	// we clean the job and take the next job
	active, err := db.IsJobActive(rdb)
	if err != nil || !active {
		return
	}

	ch, err := conn.RabbitMQ.Conn.Channel()
	if err != nil {
		log.Printf("Scheduler Watchdog ERROR: failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	for _, qName := range Queues {
		q, err := ch.QueueDeclarePassive(qName, true, false, false, false, nil)
		if err != nil {
			log.Printf("Scheduler Watchdog WARNING: failed to inspect queue %s: %v", qName, err)
			return
		}
		if q.Messages > 0 {
			return // Queue is not empty
		}
	}

	rq, err := ch.QueueDeclarePassive(ResultQueue, true, false, false, false, nil)
	if err != nil {
		log.Printf("Scheduler Watchdog WARNING: failed to inspect queue %s: %v", ResultQueue, err)
		return
	}
	if rq.Messages > 0 {
		return // Result queue is not empty
	}

	lastActStr, err := rdb.Get(ctx, "crawler:last_activity_time").Result()
	if err != nil {
		log.Printf("Scheduler Watchdog: last activity time missing, forcing cleanup")
		db.ForceCleanupJob(rdb)
		return
	}

	var lastAct int64
	_, err = fmt.Sscanf(lastActStr, "%d", &lastAct)
	if err != nil {
		log.Printf("Scheduler Watchdog: failed to parse last activity time: %v", err)
		db.ForceCleanupJob(rdb)
		return
	}

	if time.Now().Unix()-lastAct > idleTimeout {
		log.Printf("Scheduler Watchdog: No activity for %d seconds. Forcibly cleaning up job.", time.Now().Unix()-lastAct)
		db.ForceCleanupJob(rdb)
	}
}
