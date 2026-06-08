package main

import (
	"log"
	"sync"
	"time"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/bloom"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/router"
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

	// batch insert channel
	dbchan := make(chan []byte, 1000)

	batcher := db.NewBatcher()
	go batcher.BatchInsert(Conn.MongoClient, dbchan)

	broker.StartResultConsumer(Conn, dbchan)

	broker.StartWatchdog(Conn, 10*time.Second, 30) // check every 10s, timeout after 30s of inactivity

	r := router.SetupRouter(Conn.RabbitMQ.Channel, Conn.RedisClient)

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
