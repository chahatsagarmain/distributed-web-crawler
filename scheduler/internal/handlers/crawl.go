package handlers

import (
	"log"
	"net/http"

	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/broker"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CrawlRequest struct {
	URL   string `json:"url" form:"url" binding:"required"`
	Depth int    `json:"depth" form:"depth"`
}

// MakeHandleCrawl returns a gin.HandlerFunc that has access to the RabbitMQ channel.
func MakeHandleCrawl(ch *amqp.Channel) gin.HandlerFunc {
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

		if ch != nil {
			err := broker.InsertMessage(ch, req.URL, req.Depth)
			if err != nil {
				log.Printf("ERROR: failed to insert message to broker: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
				return
			}
		} else {
			log.Printf("Warning: RabbitMQ channel is nil, skipping publish for URL: %s", req.URL)
		}

		c.String(http.StatusOK, "job started")
	}
}
