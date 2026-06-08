package main

import (
	"log"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/broker"
)

func main() {
	err := common.InitConfig()
	if err != nil {
		log.Fatalf("ERROR initializing config: %v", err)
	}

	conn, err := common.ConnectAll(common.AppConfig)
	if err != nil {
		log.Fatalf("ERROR connecting to database or message broker: %v", err)
	}
	defer conn.Close()

	log.Println("Worker: Connected successfully, starting consumers...")
	err = broker.StartConsumers(conn, common.AppConfig.QueueName)
	if err != nil {
		log.Fatalf("ERROR running consumers: %v", err)
	}
}
