package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func SetupRouter(getChannel func() *amqp.Channel, getRedis func() *redis.Client) *gin.Engine {
	r := gin.Default()

	handleCrawl := handlers.MakeHandleCrawl(getChannel, getRedis)
	r.GET("/crawl", handleCrawl)
	r.POST("/crawl", handleCrawl)

	return r
}
