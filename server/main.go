package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"velocity/internal/api"
	"velocity/internal/log"
	"velocity/internal/storage"
	"velocity/internal/ui"
	"velocity/internal/version"
)


func main() {
	// CLI flags with env var fallbacks: --flag > ENV_VAR > default
	port := flag.String("port", getEnv("PORT", "8080"), "Server port")
	environment := flag.String("environment", getEnv("ENVIRONMENT", "development"), "Environment (development or production)")
	s3Endpoint := flag.String("s3-endpoint", getEnv("S3_ENDPOINT", "s3.wasabisys.com"), "S3/Wasabi endpoint")
	s3Region := flag.String("s3-region", getEnv("S3_REGION", "us-east-1"), "S3 region")
	s3Bucket := flag.String("s3-bucket", getEnv("S3_BUCKET", "velocity"), "S3 bucket name")
	s3AccessKeyID := flag.String("s3-access-key-id", getEnv("S3_ACCESS_KEY_ID", ""), "S3 access key ID")
	s3SecretAccessKey := flag.String("s3-secret-access-key", getEnv("S3_SECRET_ACCESS_KEY", ""), "S3 secret access key")
	s3Root := flag.String("s3-root", getEnv("S3_ROOT", ""), "S3 root path (default: /{environment})")
	maxVersions := flag.String("max-versions", getEnv("MAX_VERSIONS", "10"), "Max versions to keep per content item (use 'all' for unlimited)")
	logLevel := flag.String("logging", getEnv("LOG_LEVEL", "info"), "Log level (debug, info, error)")
	showVersion := flag.Bool("version", false, "Show version and exit")

	flag.Parse()

	if *showVersion {
		fmt.Println(version.GetVersion())
		os.Exit(0)
	}

	// Set log level
	log.SetLevel(log.ParseLevel(*logLevel))

	// Print styled header
	ui.PrintHeader(version.GetVersion())

	// Build config from flags
	env := parseEnvironment(*environment)

	// Compute S3 root: use flag value if provided, otherwise default to environment name
	root := *s3Root
	if root == "" {
		root = string(env)
	}

	config := &Config{
		Port:              *port,
		Environment:       env,
		S3Endpoint:        *s3Endpoint,
		S3Region:          *s3Region,
		S3Bucket:          *s3Bucket,
		S3AccessKeyID:     *s3AccessKeyID,
		S3SecretAccessKey: *s3SecretAccessKey,
		S3Root:            root,
		LogLevel:          *logLevel,
	}

	// Print config info
	ui.PrintKeyValue("Port", config.Port)
	ui.PrintKeyValue("Logging", log.GetLevel().String())
	ui.PrintKeyValue("Environment", string(config.Environment))
	fmt.Println()
	ui.PrintKeyValue("S3 Endpoint", config.S3Endpoint)
	ui.PrintKeyValue("S3 Bucket", config.S3Bucket)
	ui.PrintKeyValue("S3 Root", config.S3Root)
	fmt.Println()

	// Parse max versions (negative means unlimited)
	maxVer := 10
	if strings.ToLower(*maxVersions) == "all" {
		maxVer = -1
	} else if v, err := strconv.Atoi(*maxVersions); err == nil {
		maxVer = v
	}

	// Create S3/Wasabi storage client
	storageClient, err := storage.NewS3Storage(storage.S3Config{
		Endpoint:        config.S3Endpoint,
		Region:          config.S3Region,
		Bucket:          config.S3Bucket,
		AccessKeyID:     config.S3AccessKeyID,
		SecretAccessKey: config.S3SecretAccessKey,
		Root:            config.S3Root,
		MaxVersions:     maxVer,
	})
	if err != nil {
		log.Fatal("Failed to create storage client: %v", err)
	}

	// Check storage connection
	log.Info("Connecting to storage...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := storageClient.CheckConnection(ctx); err != nil {
		log.Fatal("Storage connection failed: %v", err)
	}
	log.Info("Connected to storage.")

	// Create the API server
	server := api.NewServer(storageClient, &api.ServerConfig{
		Port: config.Port,
	})

	// Create HTTP server with graceful shutdown
	addr := fmt.Sprintf(":%s", config.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Handler(),
	}

	// Channel to listen for shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		log.Info("Starting http server on port %s...", config.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed: %v", err)
		}
	}()

	// Give server a moment to start, then log success
	time.Sleep(100 * time.Millisecond)
	log.Info("Started http server.")
	log.Info("Velocity %s server is ready!", version.GetVersion())

	// Wait for shutdown signal
	<-stop
	fmt.Println()
	log.Info("Server stopping...")

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("Server shutdown error: %v.", err)
	} else {
		log.Info("Server stopped.")
	}
}

// Config holds all server configuration
type Config struct {
	Port              string
	Environment       storage.Environment
	S3Endpoint        string
	S3Region          string
	S3Bucket          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Root            string
	LogLevel          string
}

// parseEnvironment converts string to Environment type
func parseEnvironment(env string) storage.Environment {
	switch env {
	case "production", "prod":
		return storage.Production
	default:
		return storage.Development
	}
}

// getEnv returns an environment variable value or a default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

