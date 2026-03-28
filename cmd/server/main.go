package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/application/command"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/application/query"
	gcsInfra "github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/infrastructure/gcs"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/infrastructure/postgres"
	pubsubInfra "github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/infrastructure/pubsub"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/interfaces/http/handler"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	gcsBucket := os.Getenv("GCS_BUCKET")
	if gcsBucket == "" {
		gcsBucket = "lowcarbon-files"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/files?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Unable to ping database: %v", err)
	}

	if err := runMigrations(pool); err != nil {
		log.Printf("Warning: migration error: %v", err)
	}

	pubsubHost := os.Getenv("PUBSUB_EMULATOR_HOST")

	type fullStorage interface {
		command.Storage
		query.Storage
	}

	var storage fullStorage
	if pubsubHost != "" {
		storage = gcsInfra.NewMockClient(gcsBucket)
		log.Println("Using mock GCS client (local development)")
	} else {
		gcsClient, err := gcsInfra.NewClient(ctx, gcsBucket)
		if err != nil {
			log.Fatalf("Unable to create GCS client: %v", err)
		}
		defer gcsClient.Close()
		storage = gcsClient
		log.Println("Using real GCS client")
	}

	var publisher command.Publisher
	if pubsubHost != "" {
		publisher = pubsubInfra.NewMockPublisher()
		log.Println("Using mock PubSub publisher (local development)")
	} else {
		projectID := os.Getenv("GCP_PROJECT_ID")
		if projectID == "" {
			projectID = "low-carbon-491109"
		}
		topicID := os.Getenv("PUBSUB_TOPIC_ID")
		if topicID == "" {
			topicID = "file-events"
		}
		pub, err := pubsubInfra.NewPublisher(ctx, projectID, topicID)
		if err != nil {
			log.Fatalf("Unable to create PubSub publisher: %v", err)
		}
		defer pub.Close()
		publisher = pub
		log.Println("Using real PubSub publisher")
	}

	fileRepo := postgres.NewFileRepository(pool)

	uploadHandler := command.NewUploadFileHandler(fileRepo, storage, publisher, gcsBucket)
	deleteHandler := command.NewDeleteFileHandler(fileRepo, storage)
	getFileHandler := query.NewGetFileHandler(fileRepo, storage)
	validateHandler := query.NewValidateFileHandler(fileRepo, storage)
	initiateHandler := command.NewInitiateResumableUploadHandler(fileRepo, storage, gcsBucket)
	completeHandler := command.NewCompleteUploadHandler(fileRepo, publisher)

	fileHTTPHandler := handler.NewFileHandler(
		uploadHandler,
		deleteHandler,
		getFileHandler,
		validateHandler,
		initiateHandler,
		completeHandler,
	)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"openapi":"3.1.0","info":{"title":"File Service","version":"1.0.0"}}`))
	})

	mux.HandleFunc("POST /v1/files/upload", fileHTTPHandler.UploadFile)
	mux.HandleFunc("GET /v1/files/{fileId}", fileHTTPHandler.GetFile)
	mux.HandleFunc("DELETE /v1/files/{fileId}", fileHTTPHandler.DeleteFile)
	mux.HandleFunc("GET /v1/files/{fileId}/validate", fileHTTPHandler.ValidateFile)
	mux.HandleFunc("POST /v1/files/upload-url", fileHTTPHandler.InitiateResumableUpload)
	mux.HandleFunc("POST /v1/files/upload-complete", fileHTTPHandler.CompleteUpload)

	loggingMux := loggingMiddleware(mux)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: loggingMux,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		<-sigCh
		log.Println("Shutting down gracefully...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
	}()

	log.Printf("File Service starting on port %s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func runMigrations(pool *pgxpool.Pool) error {
	migration := `
	CREATE TABLE IF NOT EXISTS files (
		id VARCHAR(64) PRIMARY KEY,
		filename VARCHAR(512) NOT NULL,
		content_type VARCHAR(128) NOT NULL,
		size_bytes BIGINT NOT NULL,
		gcs_uri VARCHAR(1024) NOT NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'uploading',
		uploaded_by VARCHAR(64) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
	CREATE INDEX IF NOT EXISTS idx_files_uploaded_by ON files(uploaded_by);
	CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at);
	`
	_, err := pool.Exec(context.Background(), migration)
	return err
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("Request: %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
		log.Printf("Completed: %s %s in %v", r.Method, r.URL.Path, time.Since(start))
	})
}
