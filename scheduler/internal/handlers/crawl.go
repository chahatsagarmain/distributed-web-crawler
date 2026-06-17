package handlers

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/bloom"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/db"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type CrawlRequest struct {
	URL   string `json:"url" form:"url" binding:"required"`
	Depth int    `json:"depth" form:"depth"`
}

func PingHandlerFunc() gin.HandlerFunc {
	return func (c *gin.Context)  {
		conn , err := common.ConnectAll(common.AppConfig)
		if err != nil {
			c.JSON(http.StatusInternalServerError , gin.H{"error" : fmt.Sprintf("cant connect to services : %v" , err.Error())})
			return
		}
		err = conn.Ping()
		if err != nil {
			c.JSON(http.StatusInternalServerError , gin.H{"error" : fmt.Sprintf("cant pint to services : %v" , err.Error())})
			return
		}
		c.JSON(http.StatusOK , gin.H{"message" : "pinged"})
	}
}

// MakeHandleCrawl returns a gin.HandlerFunc that has access to the database connections and Redis client.
func MakeHandleCrawl(conn *common.Connections, rdb *redis.Client) gin.HandlerFunc {
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

		if conn != nil {
			rmq := conn.GetRabbitMQ()
			if rmq != nil && rmq.Conn != nil && !rmq.Conn.IsClosed() {
				ch, err := rmq.Conn.Channel()
				if err != nil {
					slog.Error("failed to open channel for publishing crawl request", "error", err)
					if rdb != nil {
						db.ForceCleanupJob(rdb)
					}
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to process crawl request"})
					return
				}
				defer ch.Close()

				if rdb != nil {
					_, err := cache.AddToBloom(rdb, req.URL)
					if err != nil {
						slog.Warn("failed to add seed URL to bloom filter", "error", err)
					}
				}
				err = broker.InsertMessage(ch, req.URL, 0, req.Depth)
				if err != nil {
					slog.Error("failed to insert message to broker", "error", err)
					if rdb != nil {
						db.ForceCleanupJob(rdb)
					}
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
					return
				}
			} else {
				slog.Error("RabbitMQ connection not ready, failing crawl request")
				if rdb != nil {
					db.ForceCleanupJob(rdb)
				}
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "message broker is currently offline"})
				return
			}
		} else {
			slog.Warn("Connections object is nil, skipping publish", "url", req.URL)
		}

		c.String(http.StatusOK, "job started")
	}
}
