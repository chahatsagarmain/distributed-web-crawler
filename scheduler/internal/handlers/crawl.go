package handlers

import (
	"log/slog"
	"net/http"

	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/bloom"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type CrawlRequest struct {
	URL   string `json:"url" form:"url" binding:"required"`
	Depth int    `json:"depth" form:"depth"`
}

// MakeHandleCrawl returns a gin.HandlerFunc that has access to the RabbitMQ channel and Redis client.
func MakeHandleCrawl(ch *amqp.Channel, rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CrawlRequest
		if err := c.ShouldBind(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if req.URL == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "url is required"})
			return
		}

		if rdb != nil {
			// Check if a job is already running
			active, err := db.IsJobActive(rdb)
			if err != nil {
				slog.Error("ERROR checking job active state", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check job state"})
				return
			}
			if active {
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "another crawl job is currently in progress"})
				return
			}

			started, err := db.StartJob(rdb, req.URL, req.Depth)
			if err != nil {
				slog.Error("ERROR starting job", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start job"})
				return
			}
			if !started {
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "another crawl job is currently in progress"})
				return
			}
		} else {
			slog.Warn("Redis client is nil, running in bypass/test mode without locks")
		}

		if ch != nil {
			if rdb != nil {
				_, err := cache.AddToBloom(rdb, req.URL)
				if err != nil {
					slog.Warn("failed to add seed URL to bloom filter", "error", err)
				}
			}
			err := broker.InsertMessage(ch, req.URL, 0, req.Depth)
			if err != nil {
				slog.Error("failed to insert message to broker", "error", err)
				if rdb != nil {
					db.ForceCleanupJob(rdb)
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
				return
			}
		} else {
			slog.Warn("RabbitMQ channel is nil, skipping publish", "url", req.URL)
		}

		c.String(http.StatusOK, "job started")
	}
}
