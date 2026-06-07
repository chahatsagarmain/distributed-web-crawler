package handlers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type CrawlRequest struct {
	URL   string `json:"url" form:"url" binding:"required"`
	Depth int    `json:"depth" form:"depth"`
}

func HandleCrawl(c *gin.Context) {
	var req CrawlRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.URL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "url is required"})
		return
	}

	log.Printf("Starting job for URL: %s, Depth: %d", req.URL, req.Depth)
	c.String(http.StatusOK, "job started")
}
