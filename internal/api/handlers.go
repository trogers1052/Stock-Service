package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/trogers1052/stock-alert-system/internal/database"
	"github.com/trogers1052/stock-alert-system/internal/kafka"
	"github.com/trogers1052/stock-alert-system/internal/metrics"
	"github.com/trogers1052/stock-alert-system/internal/models"
	"github.com/trogers1052/stock-alert-system/internal/redis"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	db       *database.DB
	producer *kafka.Producer
	redis    *redis.Client
}

// NewHandler creates a new Handler
func NewHandler(db *database.DB, producer *kafka.Producer, redisClient *redis.Client) *Handler {
	return &Handler{
		db:       db,
		producer: producer,
		redis:    redisClient,
	}
}

// GetAllStocks handles GET /stocks
func (h *Handler) GetAllStocks(w http.ResponseWriter, r *http.Request) {
	stocks, err := h.db.GetAllStocks()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	respondJSON(w, http.StatusOK, stocks)
}

// GetStock handles GET /stocks/{symbol}
func (h *Handler) GetStock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]

	stock, err := h.db.GetStock(symbol)
	if err != nil {
		if errors.Is(err, database.ErrNotFound) {
			respondError(w, http.StatusNotFound, "stock not found", nil)
		} else {
			respondError(w, http.StatusInternalServerError, "internal error", err)
		}
		return
	}

	respondJSON(w, http.StatusOK, stock)
}

// AddStock handles POST /stocks
func (h *Handler) AddStock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Symbol string `json:"symbol"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Symbol == "" {
		http.Error(w, "symbol is required", http.StatusBadRequest)
		return
	}
	if !validSymbol(req.Symbol) {
		http.Error(w, "invalid symbol", http.StatusBadRequest)
		return
	}

	monitoredStock := &models.MonitoredStock{
		Symbol:  req.Symbol,
		Enabled: true,
	}
	if err := h.db.CreateMonitoredStock(monitoredStock); err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	// Get the stock to return and publish event
	stock, err := h.db.GetStock(req.Symbol)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	// Publish Kafka event (best-effort; log on failure, don't fail the request)
	if h.producer != nil {
		if err := h.producer.PublishStockAdded(r.Context(), stock); err != nil {
			log.Printf("kafka publish stock-added failed for %s: %v", req.Symbol, err)
		}
	}

	respondJSON(w, http.StatusCreated, stock)
}

// RemoveStock handles DELETE /stocks/{symbol}
func (h *Handler) RemoveStock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]

	if err := h.db.DeleteMonitoredStock(symbol); err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	// Publish Kafka event (best-effort; log on failure, don't fail the request)
	if h.producer != nil {
		if err := h.producer.PublishStockRemoved(r.Context(), symbol); err != nil {
			log.Printf("kafka publish stock-removed failed for %s: %v", symbol, err)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetSectors handles GET /api/v1/stocks/sectors
func (h *Handler) GetSectors(w http.ResponseWriter, r *http.Request) {
	sectorMap, err := h.db.GetSectorMap()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	respondJSON(w, http.StatusOK, sectorMap)
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"services":  map[string]string{},
	}
	services := health["services"].(map[string]string)
	allHealthy := true

	// Check database
	if h.db != nil {
		if err := h.db.Ping(); err != nil {
			services["postgres"] = "unhealthy: " + err.Error()
			allHealthy = false
		} else {
			services["postgres"] = "healthy"
		}
	} else {
		services["postgres"] = "not configured"
		allHealthy = false
	}

	// Check Redis
	if h.redis != nil {
		if err := h.redis.Ping(ctx); err != nil {
			services["redis"] = "unhealthy: " + err.Error()
		} else {
			services["redis"] = "healthy"
		}
	} else {
		services["redis"] = "not configured"
	}

	// Check Kafka producer
	if h.producer != nil {
		services["kafka"] = "configured"
	} else {
		services["kafka"] = "not configured"
	}

	if !allHealthy {
		health["status"] = "degraded"
	}

	respondJSON(w, http.StatusOK, health)
}

// CreateFeedback handles POST /api/v1/feedback
func (h *Handler) CreateFeedback(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Symbol             string   `json:"symbol"`
		Signal             string   `json:"signal"`
		Action             string   `json:"action"`
		Confidence         float64  `json:"confidence"`
		RulesTriggered     []string `json:"rules_triggered,omitempty"`
		RegimeID           string   `json:"regime_id,omitempty"`
		DecisionConfidence float64  `json:"decision_confidence,omitempty"`
		EntryPrice         float64  `json:"entry_price,omitempty"`
		StopPrice          float64  `json:"stop_price,omitempty"`
		Target1            float64  `json:"target_1,omitempty"`
		Target2            float64  `json:"target_2,omitempty"`
		ValidUntil         string   `json:"valid_until,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Symbol == "" || req.Signal == "" || req.Action == "" {
		http.Error(w, "symbol, signal, and action are required", http.StatusBadRequest)
		return
	}

	fb := &models.SignalFeedback{
		Symbol:             req.Symbol,
		Signal:             req.Signal,
		Action:             req.Action,
		Confidence:         req.Confidence,
		RulesTriggered:     req.RulesTriggered,
		RegimeID:           req.RegimeID,
		DecisionConfidence: req.DecisionConfidence,
		EntryPrice:         req.EntryPrice,
		StopPrice:          req.StopPrice,
		Target1:            req.Target1,
		Target2:            req.Target2,
		FeedbackTimestamp:  time.Now(),
	}

	if req.ValidUntil != "" {
		t, err := time.Parse(time.RFC3339, req.ValidUntil)
		if err == nil {
			fb.ValidUntil = &t
		}
	}

	dbStart := time.Now()
	if err := h.db.CreateSignalFeedback(fb); err != nil {
		metrics.DBWriteErrors.Inc()
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
	metrics.FeedbackCreated.Inc()

	log.Printf("Feedback stored: %s %s -> %s (confidence=%.2f, entry=%.2f, stop=%.2f, rules=%v, regime=%s)",
		fb.Symbol, fb.Signal, fb.Action, fb.Confidence, fb.EntryPrice, fb.StopPrice,
		fb.RulesTriggered, fb.RegimeID)
	respondJSON(w, http.StatusCreated, fb)
}

// UpdateFeedback handles PUT /api/v1/feedback/{id}
func (h *Handler) UpdateFeedback(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		http.Error(w, "invalid feedback id", http.StatusBadRequest)
		return
	}

	var req struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Action == "" {
		http.Error(w, "action is required", http.StatusBadRequest)
		return
	}

	dbStart := time.Now()
	if err := h.db.UpdateFeedbackAction(id, req.Action); err != nil {
		metrics.DBWriteErrors.Inc()
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
	metrics.FeedbackUpdated.Inc()

	log.Printf("Feedback updated: id=%d -> %s", id, req.Action)
	respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// GetUnresolvedSignals handles GET /api/v1/feedback/unresolved
func (h *Handler) GetUnresolvedSignals(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil || l < 1 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
		limit = l
	}

	entries, err := h.db.GetUnresolvedSignals(limit)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	if entries == nil {
		entries = []*models.SignalFeedback{}
	}
	respondJSON(w, http.StatusOK, entries)
}

// UpdateSignalOutcome handles PUT /api/v1/feedback/{id}/outcome
func (h *Handler) UpdateSignalOutcome(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		http.Error(w, "invalid feedback id", http.StatusBadRequest)
		return
	}

	var req struct {
		Outcome string `json:"outcome"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	validOutcomes := map[string]bool{
		"TARGET_1_HIT": true,
		"TARGET_2_HIT": true,
		"STOPPED_OUT":  true,
		"EXPIRED":      true,
	}
	if !validOutcomes[req.Outcome] {
		http.Error(w, "outcome must be one of: TARGET_1_HIT, TARGET_2_HIT, STOPPED_OUT, EXPIRED", http.StatusBadRequest)
		return
	}

	dbStart := time.Now()
	if err := h.db.UpdateSignalOutcome(id, req.Outcome); err != nil {
		metrics.DBWriteErrors.Inc()
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
	metrics.FeedbackUpdated.Inc()

	log.Printf("Signal outcome updated: id=%d -> %s", id, req.Outcome)
	respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// GetRuleAccuracy handles GET /api/v1/feedback/accuracy
func (h *Handler) GetRuleAccuracy(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	sinceDays := 90
	if sd := query.Get("since_days"); sd != "" {
		d, err := strconv.Atoi(sd)
		if err != nil || d < 1 {
			http.Error(w, "invalid since_days parameter", http.StatusBadRequest)
			return
		}
		sinceDays = d
	}

	minSignals := 10
	if ms := query.Get("min_signals"); ms != "" {
		m, err := strconv.Atoi(ms)
		if err != nil || m < 1 {
			http.Error(w, "invalid min_signals parameter", http.StatusBadRequest)
			return
		}
		minSignals = m
	}

	accuracy, err := h.db.GetRuleAccuracy(sinceDays, minSignals)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	if accuracy == nil {
		accuracy = []*models.RuleAccuracy{}
	}
	respondJSON(w, http.StatusOK, accuracy)
}

// GetRuleOutcomeQuality handles GET /api/v1/feedback/outcome-quality
func (h *Handler) GetRuleOutcomeQuality(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	sinceDays := 90
	if sd := query.Get("since_days"); sd != "" {
		d, err := strconv.Atoi(sd)
		if err != nil || d < 1 {
			http.Error(w, "invalid since_days parameter", http.StatusBadRequest)
			return
		}
		sinceDays = d
	}

	minSignals := 5
	if ms := query.Get("min_signals"); ms != "" {
		m, err := strconv.Atoi(ms)
		if err != nil || m < 1 {
			http.Error(w, "invalid min_signals parameter", http.StatusBadRequest)
			return
		}
		minSignals = m
	}

	quality, err := h.db.GetRuleOutcomeQuality(sinceDays, minSignals)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	if quality == nil {
		quality = []*models.RuleOutcomeQuality{}
	}
	respondJSON(w, http.StatusOK, quality)
}

// GetFeedback handles GET /api/v1/feedback
func (h *Handler) GetFeedback(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	var sinceDate *time.Time
	if sinceDays := query.Get("since_days"); sinceDays != "" {
		days, err := strconv.Atoi(sinceDays)
		if err != nil || days < 0 {
			http.Error(w, "invalid since_days parameter", http.StatusBadRequest)
			return
		}
		t := time.Now().AddDate(0, 0, -days)
		sinceDate = &t
	}

	symbol := query.Get("symbol")

	limit := 1000
	if limitStr := query.Get("limit"); limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil || l < 0 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
		limit = l
	}

	entries, err := h.db.GetSignalFeedback(limit, sinceDate, symbol)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	respondJSON(w, http.StatusOK, entries)
}

// GetFeedbackSummary handles GET /api/v1/feedback/summary
func (h *Handler) GetFeedbackSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.db.GetFeedbackSummary()
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	respondJSON(w, http.StatusOK, summary)
}

// GetAllTiers handles GET /api/v1/tiers
func (h *Handler) GetAllTiers(w http.ResponseWriter, r *http.Request) {
	tierFilter := r.URL.Query().Get("tier")

	var tiers []*models.BacktestTier
	var err error

	if tierFilter != "" {
		tiers, err = h.db.GetBacktestTiersByTier(tierFilter)
	} else {
		tiers, err = h.db.GetAllBacktestTiers()
	}

	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}

	respondJSON(w, http.StatusOK, tiers)
}

// GetTier handles GET /api/v1/tiers/{symbol}
func (h *Handler) GetTier(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]

	tier, err := h.db.GetBacktestTier(symbol)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	if tier == nil {
		http.Error(w, "tier not found: "+symbol, http.StatusNotFound)
		return
	}

	respondJSON(w, http.StatusOK, tier)
}

// UpsertTier handles PUT /api/v1/tiers
func (h *Handler) UpsertTier(w http.ResponseWriter, r *http.Request) {
	var tier models.BacktestTier
	if err := json.NewDecoder(r.Body).Decode(&tier); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if tier.Symbol == "" || tier.Tier == "" {
		http.Error(w, "symbol and tier are required", http.StatusBadRequest)
		return
	}

	dbStart := time.Now()
	if err := h.db.UpsertBacktestTier(&tier); err != nil {
		metrics.DBWriteErrors.Inc()
		respondError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
	metrics.TierUpserts.Inc()

	log.Printf("Tier upserted: %s -> %s (score=%.1f)", tier.Symbol, tier.Tier, tier.CompositeScore)

	// Cache in Redis
	if h.redis != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		td := &redis.TierData{
			Symbol:                 tier.Symbol,
			Tier:                   tier.Tier,
			CompositeScore:         tier.CompositeScore,
			ConfidenceMultiplier:   tier.ConfidenceMultiplier,
			PositionSizeMultiplier: tier.PositionSizeMultiplier,
			Blacklisted:            tier.Blacklisted,
			AllowedRegimes:         tier.AllowedRegimes,
		}
		_ = h.redis.SetTierData(ctx, td, 24*time.Hour)
	}

	respondJSON(w, http.StatusOK, tier)
}

// BulkUpsertTiers handles PUT /api/v1/tiers/bulk
func (h *Handler) BulkUpsertTiers(w http.ResponseWriter, r *http.Request) {
	var tiers []models.BacktestTier
	if err := json.NewDecoder(r.Body).Decode(&tiers); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(tiers) == 0 {
		http.Error(w, "at least one tier is required", http.StatusBadRequest)
		return
	}

	var errors []string
	succeeded := 0
	for i := range tiers {
		if tiers[i].Symbol == "" || tiers[i].Tier == "" {
			errors = append(errors, "entry missing symbol or tier")
			continue
		}

		dbStart := time.Now()
		if err := h.db.UpsertBacktestTier(&tiers[i]); err != nil {
			metrics.DBWriteErrors.Inc()
			errors = append(errors, tiers[i].Symbol+": "+err.Error())
			continue
		}
		metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
		metrics.TierUpserts.Inc()

		// Cache in Redis
		if h.redis != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			td := &redis.TierData{
				Symbol:                 tiers[i].Symbol,
				Tier:                   tiers[i].Tier,
				CompositeScore:         tiers[i].CompositeScore,
				ConfidenceMultiplier:   tiers[i].ConfidenceMultiplier,
				PositionSizeMultiplier: tiers[i].PositionSizeMultiplier,
				Blacklisted:            tiers[i].Blacklisted,
				AllowedRegimes:         tiers[i].AllowedRegimes,
			}
			_ = h.redis.SetTierData(ctx, td, 24*time.Hour)
			cancel()
		}

		succeeded++
	}

	log.Printf("Bulk tier upsert: %d/%d succeeded", succeeded, len(tiers))

	result := map[string]interface{}{
		"succeeded": succeeded,
		"total":     len(tiers),
	}
	if len(errors) > 0 {
		result["errors"] = errors
	}

	respondJSON(w, http.StatusOK, result)
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError logs the internal error (if any) and returns a generic message to
// the client, so internal and database error details never leak in the response.
func respondError(w http.ResponseWriter, status int, publicMsg string, err error) {
	if err != nil {
		log.Printf("api error (%d): %s: %v", status, publicMsg, err)
	}
	http.Error(w, publicMsg, status)
}

// validSymbol reports whether s is a plausible ticker: 1-10 chars of letters,
// digits, '.', or '-'. Rejects obvious junk before it reaches the database.
func validSymbol(s string) bool {
	if s == "" || len(s) > 10 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-':
		default:
			return false
		}
	}
	return true
}
