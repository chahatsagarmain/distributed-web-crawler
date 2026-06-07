package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
)

func SetupRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/crawl", handlers.HandleCrawl)
	r.POST("/crawl", handlers.HandleCrawl)

	return r
}
