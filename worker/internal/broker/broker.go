package broker

import (
	"context"
	"encoding/json"
	"log"
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

	log.Printf("Worker: Starting consumers for queues: %v", targetQueues)

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
				log.Printf("Worker ERROR: failed to open channel for queue %s: %v", q, err)
				errChan <- err
				return
			}
			defer ch.Close()

			// prefetch count to 1
			err = ch.Qos(1, 0, false)
			if err != nil {
				log.Printf("Worker WARNING: failed to set Qos for queue %s: %v", q, err)
			}

			msgs, err := ch.Consume(
				q,     // queue
				"",    // consumer name (empty for auto-generated)
				true,  // autoAck (true: message is auto-acked on receipt)
				false, // exclusive
				false, // noLocal
				false, // noWait
				nil,   // args
			)
			if err != nil {
				log.Printf("Worker ERROR: failed to consume from queue %s: %v", q, err)
				errChan <- err
				return
			}

			log.Printf("Worker: Listening for messages on queue: %s", q)
			for msg := range msgs {
				processMessage(ch, msg, c)
			}
			log.Printf("Worker: Consumer for queue %s stopped", q)
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
	var crawlMsg common.CrawlMessage
	if err := json.Unmarshal(d.Body, &crawlMsg); err != nil {
		log.Printf("Worker ERROR: failed to unmarshal CrawlMessage: %v", err)
		return
	}

	log.Printf("Worker: Received task: URL=%s, Depth=%d, MaxDepth=%d", crawlMsg.URL, crawlMsg.CurrentDepth, crawlMsg.MaxDepth)
	crawlResult, err := c.CrawlUrl(crawlMsg.URL, crawlMsg.CurrentDepth)

	if err != nil {
		log.Printf("Worker WARNING: Crawl failed for %s: %v", crawlMsg.URL, err)
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
		log.Printf("Worker ERROR: failed to marshal UrlData: %v", err)
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
		log.Printf("Worker ERROR: failed to publish result for %s to result queue: %v", crawlMsg.URL, err)
		return
	}

	log.Printf("Worker: Successfully crawled and published results for %s", crawlMsg.URL)
}
