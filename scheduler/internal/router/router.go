package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func SetupRouter(ch *amqp.Channel, rdb *redis.Client) *gin.Engine {
	r := gin.Default()

	handleCrawl := handlers.MakeHandleCrawl(ch, rdb)
	r.GET("/crawl", handleCrawl)
	r.POST("/crawl", handleCrawl)

	return r
}
