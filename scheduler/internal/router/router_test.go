package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func TestCrawlRoute_GET(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := SetupRouter(
		func() *amqp.Channel { return nil },
		func() *redis.Client { return nil },
	)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/crawl?url=https://example.com&depth=3", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	expected := "job started"
	if w.Body.String() != expected {
		t.Errorf("Expected body %q, got %q", expected, w.Body.String())
	}
}

func TestCrawlRoute_POST_JSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := SetupRouter(
		func() *amqp.Channel { return nil },
		func() *redis.Client { return nil },
	)

	w := httptest.NewRecorder()
	jsonBody := `{"url": "https://example.com", "depth": 5}`
	req, _ := http.NewRequest("POST", "/crawl", strings.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	expected := "job started"
	if w.Body.String() != expected {
		t.Errorf("Expected body %q, got %q", expected, w.Body.String())
	}
}

func TestCrawlRoute_MissingURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := SetupRouter(
		func() *amqp.Channel { return nil },
		func() *redis.Client { return nil },
	)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/crawl?depth=3", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}
