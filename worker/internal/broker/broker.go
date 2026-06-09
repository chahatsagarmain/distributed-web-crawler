package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/crawler"
	amqp "github.com/rabbitmq/amqp091-go"
)

// StartConsumers starts consuming crawl tasks from specified queues and publishes results to the result queue.
func StartConsumers(conn *common.Connections, queueName string) error {
	var targetQueues []string
	if queueName != "" {
		targetQueues = []string{queueName}
	} else {
		targetQueues = common.Queues
	}

	slog.Info("Worker: Starting consumers", "queues", targetQueues)

	c := crawler.NewCrawler()

	var wg sync.WaitGroup
	errChan := make(chan error, len(targetQueues))

	for _, qName := range targetQueues {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()

			// Open a dedicated channel for this queue consumer
			ch, err := conn.RabbitMQ.Conn.Channel()
			if err != nil {
				slog.Error("Worker ERROR: failed to open channel", "queue", q, "error", err)
				errChan <- err
				return
			}
			defer ch.Close()

			// set prefetch Qos to match concurrency limit 
			err = ch.Qos(5, 0, false)
			if err != nil {
				slog.Warn("Worker WARNING: failed to set Qos", "queue", q, "error", err)
			}

			msgs, err := ch.Consume(
				q,     // queue
				"",    // consumer name (empty for auto-generated)
				false, // autoAck (false: manual ack to manage backpressure)
				false, // exclusive
				false, // noLocal
				false, // noWait
				nil,   // args
			)
			if err != nil {
				slog.Error("Worker ERROR: failed to consume from queue", "queue", q, "error", err)
				errChan <- err
				return
			}

			slog.Info("Worker: Listening for messages", "queue", q)
			var wgWorkers sync.WaitGroup
			for i := 0 ; i < 5 ; i++{
				wgWorkers.Add(1)
				go func(){
					defer wgWorkers.Done()
					for msg := range msgs {
						processMessage(ch , msg , c)
					}
				}()
			}
			wgWorkers.Wait()
			slog.Info("Worker: Consumer stopped", "queue", q)
		}(qName)
	}
	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan
	}
	return nil
}

func processMessage(ch *amqp.Channel, d amqp.Delivery, c *crawler.Crawler) {
	defer d.Ack(false) // Acknowledge message on exit 

	var crawlMsg common.CrawlMessage
	if err := json.Unmarshal(d.Body, &crawlMsg); err != nil {
		slog.Error("Worker ERROR: failed to unmarshal CrawlMessage", "error", err)
		return
	}

	slog.Info("Worker: Received task", "url", crawlMsg.URL, "depth", crawlMsg.CurrentDepth, "maxDepth", crawlMsg.MaxDepth)
	crawlResult, err := c.CrawlUrl(crawlMsg.URL, crawlMsg.CurrentDepth)

	if err != nil {
		slog.Warn("Worker WARNING: Crawl failed", "url", crawlMsg.URL, "error", err)
		return
	}

	result := common.UrlData{
		Url:       crawlMsg.URL,
		Depth:     crawlMsg.CurrentDepth,
		NextUrls:  crawlResult.NextUrls,
		RawHtml:   crawlResult.RawHtml,
		HasRobots: false,
	}

	respBody, err := json.Marshal(result)
	if err != nil {
		slog.Error("Worker ERROR: failed to marshal UrlData", "error", err)
		return
	}

	err = ch.PublishWithContext(
		context.Background(),
		common.ResultExchange,   // exchange
		common.ResultRoutingKey, // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        respBody,
		},
	)
	if err != nil {
		slog.Error("Worker ERROR: failed to publish result", "url", crawlMsg.URL, "error", err)
		return
	}
	
	slog.Info("Worker: Successfully crawled and published results", "url", crawlMsg.URL)
}
