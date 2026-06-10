package main

import (
	"log/slog"
	"os"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/broker"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":8081", nil); err != nil {
			slog.Error("Metrics server failed", "error", err)
		}
	}()
	err := common.InitConfig()
	if err != nil {
		slog.Error("Worker ERROR: initializing configuration failed", "error", err)
		os.Exit(1)
	}

	conn, err := common.ConnectAll(common.AppConfig)
	if err != nil {
		slog.Error("Worker ERROR: failed to connect to database or message broker", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	slog.Info("Worker: Connected successfully, starting consumers...")
	err = broker.StartConsumers(conn, common.AppConfig.QueueName)
	if err != nil {
		slog.Error("Worker ERROR: consumers failed", "error", err)
		os.Exit(1)
	}
}
