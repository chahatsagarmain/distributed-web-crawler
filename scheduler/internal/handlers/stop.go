package handlers

import (
	"log/slog"
	"net/http"

	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// MakeHandleStop returns a gin.HandlerFunc that stops the current active crawl job
func MakeHandleStop(ch *amqp.Channel, rdb *redis.Client) gin.HandlerFunc {
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

		if ch != nil {
			err := broker.PurgeQueues(ch)
			if err != nil {
				slog.Error("ERROR purging queues during stop", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to purge queues, workers might still process some URLs"})
				return
			}
			slog.Info("Stop handler: Purged RabbitMQ queues successfully")
		} else {
			slog.Warn("Stop handler: RabbitMQ channel is nil, cannot purge queues")
		}

		c.JSON(http.StatusOK, gin.H{"message": "crawl job stopped successfully"})
	}
}
