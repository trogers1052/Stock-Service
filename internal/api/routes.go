package api

import (
	"github.com/gorilla/mux"
)

// SetupRoutes configures all API routes
func SetupRoutes(handler *Handler) *mux.Router {
	r := mux.NewRouter()

	// Health check
	r.HandleFunc("/health", handler.HealthCheck).Methods("GET")

	// Stock routes
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/stocks", handler.GetAllStocks).Methods("GET")
	api.HandleFunc("/stocks", handler.AddStock).Methods("POST")
	api.HandleFunc("/stocks/{symbol}", handler.GetStock).Methods("GET")
	api.HandleFunc("/stocks/{symbol}", handler.RemoveStock).Methods("DELETE")

	return r
}
