package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/broker"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Conn *common.Connections

func main() {
	g := gin.New()
	g.GET("/metrics" , gin.WrapH(promhttp.Handler()))
	g.GET("/ping" , PingHandler)
	go func() {
		if err := g.Run(":8081"); err != nil {
			slog.Error("Metrics server failed", "error", err)
		}
	}()
	err := common.InitConfig()
	if err != nil {
		slog.Error("Worker ERROR: initializing configuration failed", "error", err)
		os.Exit(1)
	}

	Conn, err = common.ConnectAll(common.AppConfig)
	if err != nil {
		slog.Error("Worker ERROR: failed to connect to database or message broker", "error", err)
		os.Exit(1)
	}
	defer Conn.Close()

	Conn.StartConnectionMonitor(common.AppConfig.RabbitMQURI, nil)

	slog.Info("Worker: Connected successfully, starting consumers...")
	err = broker.StartConsumers(Conn, common.AppConfig.QueueName)
	if err != nil {
		slog.Error("Worker ERROR: consumers failed", "error", err)
		os.Exit(1)
	}
}

func PingHandler(c *gin.Context)  {
	if err := Conn.Ping() ; err != nil {
		c.JSON(http.StatusInternalServerError , gin.H{"body" : "error pinging services"})
		return
	}

	c.JSON(http.StatusOK , gin.H{"body" : "PINGED!!!"})
}