package main

import (
	"log"

	"github.com/trogers1052/trading-go-commons/env"
	"github.com/trogers1052/trading-go-commons/httpserver"

	// Side-effect import: registers all promauto metrics defined in the
	// metrics package so they appear on the /metrics endpoint.
	_ "github.com/trogers1052/stock-alert-system/internal/metrics"
)

// startMetricsServer starts the Prometheus /metrics scrape target and returns
// the server so the caller can shut it down gracefully.
func startMetricsServer() *httpserver.Server {
	port := env.String("METRICS_PORT", "9097")
	srv := httpserver.NewMetricsServer(":" + port)
	errCh := srv.Start()
	go func() {
		if err := <-errCh; err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()
	log.Printf("Metrics server listening on :%s/metrics", port)
	return srv
}
