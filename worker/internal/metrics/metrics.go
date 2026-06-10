package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PagesProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_pages_processed_total",
		Help: "Total number of pages processed labeled by status",
	}, []string{"status"})

	HTTPStatusCodesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_http_status_codes_total",
		Help: "Total HTTP status codes encountered labeled by code and domain",
	}, []string{"code", "domain"})

	PageProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "crawler_page_processing_duration_seconds",
		Help: "Time taken to process a single page in seconds",
		Buckets: prometheus.DefBuckets,
	})

	BytesDownloadedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_bytes_downloaded_total",
		Help: "Total bytes of scraped content downloaded",
	})
)
