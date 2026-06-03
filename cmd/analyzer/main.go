// Command analyzer is the HTTP server for root-cause analysis of
// production errors. Log ingestion from Grafana/Loki lives in a
// separate service that POSTs to this server's `POST /analyze` API.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/handler"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/summarizer"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"

	"github.com/redis/go-redis/v9"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	sub := os.Args[1]

	switch sub {
	case "serve":
		os.Exit(cmdServe())
	case "version", "--version", "-v":
		fmt.Println("analyzer", version)
	case "help", "-h", "--help":
		usage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", sub)
		usage(os.Stderr)
		os.Exit(2)
	}
}

func usage(w *os.File) {
	fmt.Fprintf(w, `analyzer %s

USAGE:
  analyzer serve       Start the HTTP API server (env-configured)
  analyzer version     Print version
`, version)
}

func cmdServe() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		return 1
	}

	initLogging(cfg.LogLevel, cfg.LogFormat)

	wc := weaviate.New(cfg.WeaviateURL, cfg.WeaviateClass, cfg.WeaviateTimeout)
	gen := ollama.New(cfg.OllamaURL, cfg.OllamaTimeout)
	// Separate Ollama client with its own (shorter) timeout for embeddings.
	emb := ollama.New(cfg.OllamaURL, cfg.EmbedTimeout)

	// Optional Redis client for the summary cache (§7a). Failure to connect
	// is non-fatal — the analyzer just runs without doc priming.
	var rdb *redis.Client
	if cfg.SummaryEnabled && cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			slog.Warn("invalid REDIS_URL; running without summary cache", "error", err)
		} else {
			rdb = redis.NewClient(opts)
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := rdb.Ping(pingCtx).Err(); err != nil {
				slog.Warn("redis unreachable; running without summary cache", "error", err)
				_ = rdb.Close()
				rdb = nil
			}
			cancel()
		}
	}
	summ := summarizer.New(cfg, rdb, wc, wc, gen)

	engine := handler.New(cfg, wc, wc, gen, emb, summ)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      engine,
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting server", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Graceful shutdown on SIGINT/SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		slog.Info("received signal, shutting down", "signal", sig)
	case err := <-errCh:
		if err != nil {
			slog.Error("server error", "error", err)
			return 1
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
		return 1
	}
	slog.Info("server stopped")
	return 0
}

func initLogging(level, format string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}
