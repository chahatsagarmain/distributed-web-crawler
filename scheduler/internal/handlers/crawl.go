package handlers

import (
	"log"
	"net/http"

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
func MakeHandleCrawl(getChannel func() *amqp.Channel, getRedis func() *redis.Client) gin.HandlerFunc {
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

		rdb := getRedis()
		if rdb != nil {
			// Check if a job is already running
			active, err := db.IsJobActive(rdb)
			if err != nil {
				log.Printf("ERROR checking job active state: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check job state"})
				return
			}
			if active {
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "another crawl job is currently in progress"})
				return
			}

			// Try to start the job and acquire lock
			started, err := db.StartJob(rdb, req.URL)
			if err != nil {
				log.Printf("ERROR starting job: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start job"})
				return
			}
			if !started {
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "another crawl job is currently in progress"})
				return
			}
		} else {
			log.Printf("Warning: Redis client is nil, running in bypass/test mode without locks")
		}

		ch := getChannel()
		if ch != nil {
			err := broker.InsertMessage(ch, req.URL, 0, req.Depth)
			if err != nil {
				log.Printf("ERROR: failed to insert message to broker: %v", err)
				// Clean up job lock if publish fails
				if rdb != nil {
					rdb.Del(c.Request.Context(), db.ActiveJobKey)
					rdb.Del(c.Request.Context(), db.PendingCountsKey)
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
				return
			}
		} else {
			log.Printf("Warning: RabbitMQ channel is nil, skipping publish for URL: %s", req.URL)
		}

		c.String(http.StatusOK, "job started")
	}
}
