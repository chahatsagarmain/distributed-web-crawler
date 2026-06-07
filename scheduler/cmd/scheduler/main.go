package main

import (
	"log"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/router"
)

var Conn *common.Connections

func main() {
	err := common.InitConfig()
	if err != nil {
		log.Printf("Warning: initializing configuration failed: %v", err)
	}

	Conn, err = common.ConnectAll(common.AppConfig)
	if err != nil {
		log.Printf("Warning: failed to connect to database or message broker: %v", err)
	} else {
		defer Conn.Close()
		// Setup exchanges, queues, and bindings in RabbitMQ
		if Conn.RabbitMQ != nil && Conn.RabbitMQ.Channel != nil {
			err = broker.SetupBroker(Conn.RabbitMQ.Channel)
			if err != nil {
				log.Printf("Warning: failed to setup broker: %v", err)
			}
		}
	}

	r := router.SetupRouter()

	log.Println("Starting scheduler server on :8080...")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}