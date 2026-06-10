package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	URLsQueuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_urls_queued_total",
		Help: "The total number of URLs successfully pushed to RabbitMQ",
	})

	SchedulingErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_scheduling_errors_total",
		Help: "The total number of errors encountered while scheduling",
	})

	ActiveDomains = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_active_domains",
		Help: "The number of unique domains currently being tracked/crawled",
	})
)
