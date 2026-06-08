package main

import (
	"log"
	"sync"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/cache"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/router"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var Conn *common.Connections

func main() {
	err := common.InitConfig()
	if err != nil {
		log.Fatalf("Warning: initializing configuration failed: %v", err)
	}

	Conn, err = common.ConnectAll(common.AppConfig)
	if err != nil {
		log.Fatalf("Warning: failed to connect to database or message broker: %v", err)
	}
	defer Conn.Close()

	err = broker.SetupBroker(Conn.RabbitMQ.Channel)
	if err != nil {
		log.Fatalf("Warning: failed to setup broker: %v", err)
	}

	err = cache.SetupBloomFilter(Conn.RedisClient)
	if err != nil {
		log.Fatalf("Warning: failed to connect to database or message broker: %v", err)
	}

	r := router.SetupRouter(func() *amqp.Channel {
		if Conn != nil && Conn.RabbitMQ != nil {
			return Conn.RabbitMQ.Channel
		}
		return nil
	}, func() *redis.Client {
		if Conn != nil {
			return Conn.RedisClient
		}
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Starting scheduler server on :8080...")
		if err := r.Run(":8080"); err != nil {
			log.Fatalf("Failed to run server: %v", err)
		}
	}()

	wg.Wait()
}
