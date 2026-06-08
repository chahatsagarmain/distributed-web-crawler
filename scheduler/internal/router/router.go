package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

func SetupRouter(ch *amqp.Channel) *gin.Engine {
	r := gin.Default()

	handleCrawl := handlers.MakeHandleCrawl(ch)
	r.POST("/crawl", handleCrawl)

	return r
}
