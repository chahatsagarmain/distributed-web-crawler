package handlers

import (
	"log/slog"
	"net/http"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// MakeHandleStop returns a gin.HandlerFunc that stops the current active crawl job
func MakeHandleStop(conn *common.Connections, rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if rdb == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis client is nil, cannot stop job"})
			return
		}

		active, err := db.IsJobActive(rdb)
		if err != nil {
			slog.Error("ERROR checking job active state during stop", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check job state"})
			return
		}
		if !active {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no active crawl job to stop"})
			return
		}

		db.ForceCleanupJob(rdb)
		slog.Info("Stop handler: Cleared Redis active job state")

		if conn != nil {
			rmq := conn.GetRabbitMQ()
			if rmq != nil && rmq.Conn != nil && !rmq.Conn.IsClosed() {
				ch, err := rmq.Conn.Channel()
				if err != nil {
					slog.Error("failed to open channel for purging", "error", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open channel, but job state cleared"})
					return
				}
				defer ch.Close()

				err = broker.PurgeQueues(ch)
				if err != nil {
					slog.Error("ERROR purging queues during stop", "error", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to purge queues, workers might still process some URLs"})
					return
				}
				slog.Info("Stop handler: Purged RabbitMQ queues successfully")
			} else {
				slog.Warn("Stop handler: RabbitMQ connection not ready, cannot purge queues")
			}
		} else {
			slog.Warn("Stop handler: Connections object is nil, cannot purge queues")
		}

		c.JSON(http.StatusOK, gin.H{"message": "crawl job stopped successfully"})
	}
}
