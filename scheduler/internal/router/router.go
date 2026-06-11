package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupRouter(ch *amqp.Channel, rdb *redis.Client) *gin.Engine {
	r := gin.Default()

	handleCrawl := handlers.MakeHandleCrawl(ch, rdb)
	r.POST("/crawl", handleCrawl)

	handleStop := handlers.MakeHandleStop(ch, rdb)
	r.POST("/stop", handleStop)
	
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/ping" , handlers.PingHandlerFunc())

	return r
}
