package main

import (
	"log"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/router"
)

var Conn *common.Connections

func main() {
	err := common.InitConfig()
	if err != nil {
		log.Printf("Warning: initializing configuration failed: %v", err)
	}

	Conn, err = common.ConnectAll(common.AppConfig)
	defer Conn.Close()
	if err != nil {
		log.Printf("Warning: failed to connect to database or message broker: %v", err)
	}
	r := router.SetupRouter(Conn.RabbitMQ.Channel)

	log.Println("Starting scheduler server on :8080...")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}