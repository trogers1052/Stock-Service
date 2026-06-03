package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/trogers1052/stock-alert-system/internal/metrics"
)

const (
	accuracyCacheKey         = "feedback:accuracy"
	outcomeQualityCacheKey   = "feedback:outcome_quality"
	accuracyCacheInterval    = 15 * time.Minute
	accuracyCacheTTL         = 30 * time.Minute
	accuracySinceDays        = 90
	accuracyMinSignals       = 10
	outcomeQualityMinSignals = 5
)

// accuracyEntry is the per-rule data written to Redis.
type accuracyEntry struct {
	TradeRate   float64 `json:"trade_rate"`
	Multiplier  float64 `json:"multiplier"`
	SignalCount int     `json:"signal_count"`
}

// outcomeQualityEntry is the per-rule outcome quality data written to Redis.
type outcomeQualityEntry struct {
	WinRate     float64 `json:"win_rate"`
	Multiplier  float64 `json:"multiplier"`
	SignalCount int     `json:"signal_count"`
	WinCount    int     `json:"win_count"`
	LossCount   int     `json:"loss_count"`
}

// StartAccuracyCacheWriter runs a background goroutine that periodically computes
// per-rule accuracy metrics and writes them to Redis for the decision-engine to consume.
func (h *Handler) StartAccuracyCacheWriter(ctx context.Context) {
	if h.redis == nil {
		log.Println("Accuracy cache writer: Redis unavailable, skipping")
		return
	}

	// Write immediately on startup
	h.writeAccuracyCache(ctx)

	ticker := time.NewTicker(accuracyCacheInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.writeAccuracyCache(ctx)
		case <-ctx.Done():
			log.Println("Accuracy cache writer stopped")
			return
		}
	}
}

func (h *Handler) writeAccuracyCache(ctx context.Context) {
	start := time.Now()

	accuracy, err := h.db.GetRuleAccuracy(accuracySinceDays, accuracyMinSignals)
	if err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to compute rule accuracy: %v", err)
		return
	}

	// Build map: "rule_name:regime_id" -> { trade_rate, multiplier, signal_count }
	data := make(map[string]accuracyEntry, len(accuracy))
	for _, a := range accuracy {
		key := fmt.Sprintf("%s:%s", a.RuleName, a.RegimeID)
		data[key] = accuracyEntry{
			TradeRate:   a.TradeRate,
			Multiplier:  a.Multiplier,
			SignalCount: a.SignalCount,
		}
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to marshal accuracy cache: %v", err)
		return
	}

	if err := h.redis.Set(ctx, accuracyCacheKey, string(jsonBytes), accuracyCacheTTL); err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to write accuracy cache to Redis: %v", err)
		return
	}

	metrics.AccuracyCacheDuration.Observe(time.Since(start).Seconds())
	log.Printf("Accuracy cache updated: %d rule-regime entries", len(data))

	// Also compute and cache outcome quality (signal hit rate)
	h.writeOutcomeQualityCache(ctx)
}

func (h *Handler) writeOutcomeQualityCache(ctx context.Context) {
	start := time.Now()

	quality, err := h.db.GetRuleOutcomeQuality(accuracySinceDays, outcomeQualityMinSignals)
	if err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to compute rule outcome quality: %v", err)
		return
	}

	if len(quality) == 0 {
		return
	}

	data := make(map[string]outcomeQualityEntry, len(quality))
	for _, q := range quality {
		key := fmt.Sprintf("%s:%s", q.RuleName, q.RegimeID)
		data[key] = outcomeQualityEntry{
			WinRate:     q.WinRate,
			Multiplier:  q.Multiplier,
			SignalCount: q.SignalCount,
			WinCount:    q.WinCount,
			LossCount:   q.LossCount,
		}
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to marshal outcome quality cache: %v", err)
		return
	}

	if err := h.redis.Set(ctx, outcomeQualityCacheKey, string(jsonBytes), accuracyCacheTTL); err != nil {
		metrics.AccuracyCacheErrors.Inc()
		log.Printf("WARNING: failed to write outcome quality cache to Redis: %v", err)
		return
	}

	metrics.AccuracyCacheDuration.Observe(time.Since(start).Seconds())
	log.Printf("Outcome quality cache updated: %d rule-regime entries", len(data))
}
