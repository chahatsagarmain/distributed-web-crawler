package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/bloom"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/metrics"
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
	slog.Info("Declared consistent hashing exchange", "exchange", ConsistentHashingExchange)

	for _, qName := range Queues {
		q, err := ch.QueueDeclare(
			qName, // name
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			amqp.Table{"x-max-length": 10000}, // arguments
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
		slog.Info("Declared and bound queue", "queue", qName, "exchange", ConsistentHashingExchange, "weight", 10)
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
	slog.Info("Declared result exchange", "exchange", ResultExchange)

	rq, err := ch.QueueDeclare(
		ResultQueue, // name
		true,        // durable
		false,       // auto-delete
		false,       // exclusive
		false,       // no-wait
		amqp.Table{"x-max-length": 10000}, // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare result queue %s: %w", ResultQueue, err)
	}

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
	slog.Info("Declared and bound result queue", "queue", ResultQueue, "exchange", ResultExchange, "routingKey", ResultRoutingKey)

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
		metrics.SchedulingErrorsTotal.Inc()
		return fmt.Errorf("failed to publish message to consistent hashing exchange: %w", err)
	}

	metrics.URLsQueuedTotal.Inc()
	slog.Info("Published message to consistent hashing exchange", "url", urlStr, "currentDepth", currentDepth, "maxDepth", maxDepth)
	return nil
}

// StartResultConsumer starts consuming from ResultQueue in the background, updating last activity and processing results.
// last ack time is required to detect inactivity and cleaning active job status
func StartResultConsumer(conn *common.Connections, dbchan chan []byte) {
	ch, err := conn.RabbitMQ.Conn.Channel()
	if err != nil {
		slog.Error("Scheduler: Failed to open RabbitMQ channel for result consumer", "error", err)
		os.Exit(1)
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
		slog.Error("Scheduler: Failed to consume from result queue", "error", err)
		os.Exit(1)
	}

	slog.Info("Scheduler: Listening for results", "queue", ResultQueue)
	go func() {
		defer ch.Close()
		var wg sync.WaitGroup
		for i := 0 ; i < 5 ; i++{
			wg.Add(1)
			go func(){
				defer wg.Done()
				for msg := range msgs {
					processResult(ch, conn.RedisClient, dbchan, msg.Body)
				}
			}()
		}
		wg.Wait()
	}()
}

func processResult(ch *amqp.Channel, rdb *redis.Client, dbchan chan []byte, body []byte) {
	dbchan <- body // send for batch insert

	ctx := context.Background()
	err := rdb.Set(ctx, "crawler:last_activity_time", time.Now().Unix(), time.Duration(common.AppConfig.JobTTL)*time.Hour).Err()
	if err != nil {
		slog.Warn("Scheduler WARNING: failed to update last activity time", "error", err)
	}

	var data common.UrlData
	if err := json.Unmarshal(body, &data); err != nil {
		slog.Error("Scheduler ERROR: unmarshalling result message", "error", err)
		return
	}

	maxDepth, err := db.GetMaxDepth(rdb)
	slog.Info("MAX DEPTH HERE IS", "maxDepth", maxDepth)
	if err != nil {
		if err == redis.Nil {
			slog.Info("Scheduler INFO: job is no longer active (max depth key deleted/expired)")
		} else {
			slog.Warn("Scheduler WARNING: failed to get max depth from Redis", "error", err.Error())
		}
		maxDepth = data.Depth
		slog.Info("MAX DEPTH JUST BELOW IS", "maxDepth", maxDepth)
	}

	// if depth is less than max depth then queue job
	if data.Depth < maxDepth {
		slog.Info("CURRENT DEPTH HERE", "depth", data.Depth)
		if len(data.NextUrls) > 0 {
			slog.Info("NEXT URL", "url", data.NextUrls[0])
		}
		limit := int(float32(len(data.NextUrls)) * (1.00 - common.AppConfig.DropNextUrlRate));
		for _, nextURL := range data.NextUrls[:limit] {
			res, err := cache.CheckUrlDuplicate(rdb, nextURL)
			if err == nil && !res {
				slog.Info("INSERT url to bloom", "url", nextURL)
				_, _ = cache.AddToBloom(rdb, nextURL)
				err = InsertMessage(ch, nextURL, data.Depth+1, maxDepth)
				if err != nil {
					slog.Error("Scheduler ERROR: enqueuing link", "url", nextURL, "error", err)
				}
				time.Sleep(time.Duration(common.AppConfig.TimeDelay) * time.Millisecond)
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
		slog.Error("Scheduler Watchdog ERROR: failed to open channel", "error", err)
		return
	}
	defer ch.Close()

	for _, qName := range Queues {
		q, err := ch.QueueDeclarePassive(qName, true, false, false, false, nil)
		if err != nil {
			slog.Warn("Scheduler Watchdog WARNING: failed to inspect queue", "queue", qName, "error", err)
			return
		}
		if q.Messages > 0 {
			return // Queue is not empty
		}
	}

	rq, err := ch.QueueDeclarePassive(ResultQueue, true, false, false, false, nil)
	if err != nil {
		slog.Warn("Scheduler Watchdog WARNING: failed to inspect queue", "queue", ResultQueue, "error", err)
		return
	}
	if rq.Messages > 0 {
		return // Result queue is not empty
	}

	lastActStr, err := rdb.Get(ctx, "crawler:last_activity_time").Result()
	if err != nil {
		slog.Info("Scheduler Watchdog: last activity time missing, forcing cleanup")
		db.ForceCleanupJob(rdb)
		return
	}

	var lastAct int64
	lastAct , err = strconv.ParseInt(lastActStr , 10 , 64)
	if err != nil {
		slog.Warn("Scheduler Watchdog: failed to parse last activity time", "error", err)
		db.ForceCleanupJob(rdb)
		return
	}

	if time.Now().Unix()-lastAct > idleTimeout {
		slog.Info("Scheduler Watchdog: No activity. Forcibly cleaning up job.", "inactive_seconds", time.Now().Unix()-lastAct)
		db.ForceCleanupJob(rdb)
	}
}
