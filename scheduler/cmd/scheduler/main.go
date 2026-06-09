package main

import (
	"log/slog"
	"os"
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
		slog.Error("Warning: initializing configuration failed", "error", err)
		os.Exit(1)
	}

	Conn, err = common.ConnectAll(common.AppConfig)
	if err != nil {
		slog.Error("Warning: failed to connect to database or message broker", "error", err)
		os.Exit(1)
	}
	if err := Conn.Ping() ; err != nil{
		slog.Error("ERROR : ping error", "error", err)
		os.Exit(1)
	}
	slog.Info("PINGED!!!")

	defer Conn.Close()

	err = broker.SetupBroker(Conn.RabbitMQ.Channel)
	if err != nil {
		slog.Error("Warning: failed to setup broker", "error", err)
		os.Exit(1)
	}

	err = cache.SetupBloomFilter(Conn.RedisClient)
	if err != nil {
		slog.Error("Warning: In bloom filter failed to connect to database", "error", err)
		os.Exit(1)
	}

	// batch insert channel
	dbchan := make(chan []byte, 1000)

	batcher := db.NewBatcher()
	go batcher.BatchInsert(Conn.MongoClient, dbchan)

	broker.StartResultConsumer(Conn, dbchan)

	broker.StartWatchdog(Conn, 10*time.Second, 300) // check every 10s, timeout after 300s of inactivity

	r := router.SetupRouter(Conn.RabbitMQ.Channel, Conn.RedisClient)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Starting scheduler server on :8080...")
		if err := r.Run(":8080"); err != nil {
			slog.Error("Failed to run server", "error", err)
			os.Exit(1)
		}
	}()

	wg.Wait()
}
