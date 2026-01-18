package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/trogers1052/stock-alert-system/internal/api"
	"github.com/trogers1052/stock-alert-system/internal/config"
	"github.com/trogers1052/stock-alert-system/internal/database"
	"github.com/trogers1052/stock-alert-system/internal/kafka"
	"github.com/trogers1052/stock-alert-system/internal/redis"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Connect to database
	db, err := database.New(cfg.Database.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Run migrations
	if err := runMigrations(cfg.Database.ConnectionString()); err != nil {
		log.Fatalf("Failed to run database migrations: %v", err)
	}

	defer db.Close()
	log.Println("Connected to PostgreSQL database")

	// Connect to Redis
	redisClient, err := redis.New(cfg.Redis)
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v (continuing without cache)", err)
		redisClient = nil
	} else {
		defer redisClient.Close()
		log.Println("Connected to Redis cache")
	}

	// Create Kafka producer
	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer producer.Close()
	log.Printf("Kafka producer initialized (brokers: %v)", cfg.Kafka.Brokers)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start Kafka consumer for trade events
	consumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TradesTopic,
		cfg.Kafka.ConsumerGroup,
		db,
	)
	go func() {
		log.Printf("Starting Kafka consumer for topic: %s (group: %s)",
			cfg.Kafka.TradesTopic, cfg.Kafka.ConsumerGroup)
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// Set up HTTP handler and routes
	handler := api.NewHandler(db, producer, redisClient)
	router := api.SetupRoutes(handler)

	// Create HTTP server
	addr := cfg.Server.Host + ":" + cfg.Server.Port
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Starting server on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Cancel context to stop Kafka consumer
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Close Kafka consumer
	if err := consumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v", err)
	}

	log.Println("Server stopped")
}

func runMigrations(databaseUrl string) error {
	// The "file://" prefix tells the migrate library to use the file driver
	// Specify the path to your migrations directory
	m, err := migrate.New(
		"file://./db/migrations", // Path to your migrations directory
		databaseUrl)
	if err != nil {
		log.Fatalf("could not create migrate instance: %v", err)
	}

	// Apply all available migrations up to the latest version
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	// If ErrNoChange is returned, it simply means the database was already current
	if err == migrate.ErrNoChange {
		log.Println("No migrations to apply; database is up to date.")
	}

	return nil
}
