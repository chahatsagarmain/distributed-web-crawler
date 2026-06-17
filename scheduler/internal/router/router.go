package router

import (
	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/scheduler/internal/handlers"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupRouter(conn *common.Connections, rdb *redis.Client) *gin.Engine {
	r := gin.Default()

	handleCrawl := handlers.MakeHandleCrawl(conn, rdb)
	r.POST("/crawl", handleCrawl)

	handleStop := handlers.MakeHandleStop(conn, rdb)
	r.POST("/stop", handleStop)
	
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/ping" , handlers.PingHandlerFunc())

	return r
}
