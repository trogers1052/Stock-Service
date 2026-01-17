package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/trogers1052/stock-alert-system/internal/database"
	"github.com/trogers1052/stock-alert-system/internal/kafka"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	db       *database.DB
	producer *kafka.Producer
}

// NewHandler creates a new Handler
func NewHandler(db *database.DB, producer *kafka.Producer) *Handler {
	return &Handler{
		db:       db,
		producer: producer,
	}
}

// GetAllStocks handles GET /stocks
func (h *Handler) GetAllStocks(w http.ResponseWriter, r *http.Request) {
	stocks, err := h.db.GetAllStocks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusNotFound)
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

	monitoredStock := &models.MonitoredStock{
		Symbol:  req.Symbol,
		Enabled: true,
	}
	if err := h.db.CreateMonitoredStock(monitoredStock); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the stock to return and publish event
	stock, err := h.db.GetStock(req.Symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Publish Kafka event
	if h.producer != nil {
		if err := h.producer.PublishStockAdded(r.Context(), stock); err != nil {
			// Log error but don't fail the request
			// In production, you'd use a proper logger here
		}
	}

	respondJSON(w, http.StatusCreated, stock)
}

// RemoveStock handles DELETE /stocks/{symbol}
func (h *Handler) RemoveStock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]

	if err := h.db.DeleteMonitoredStock(symbol); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Publish Kafka event
	if h.producer != nil {
		if err := h.producer.PublishStockRemoved(r.Context(), symbol); err != nil {
			// Log error but don't fail the request
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
