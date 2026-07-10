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
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/infoblox/vibecoder-analyzer/internal/analyzer"
	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/handler"
	"github.com/infoblox/vibecoder-analyzer/internal/kafkaio"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/retriever"
	"github.com/infoblox/vibecoder-analyzer/internal/store"
	"github.com/infoblox/vibecoder-analyzer/internal/summarizer"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"

	"github.com/redis/go-redis/v9"
)

const version = "0.1.0"

func main() {
	_ = godotenv.Load()
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

	// Weaviate is a required dependency — the retriever step of the
	// pipeline cannot run without it. Fail fast if the configured URL
	// is unreachable rather than serving requests that will all return
	// "0 chunks" with no obvious cause.
	{
		pingCtx, cancel := context.WithTimeout(context.Background(), cfg.WeaviateTimeout)
		err := wc.Ping(pingCtx)
		cancel()
		if err != nil {
			slog.Error("weaviate unreachable", "url", cfg.WeaviateURL, "error", err)
			return 1
		}
		slog.Info("weaviate connected", "url", cfg.WeaviateURL, "class", cfg.WeaviateClass)
	}

	// Ollama health check. The LLM step (9/10) and embedding step both
	// depend on it; a missing/down endpoint should surface at boot, not
	// as a stream of analyze failures.
	{
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := gen.Ping(pingCtx)
		cancel()
		if err != nil {
			slog.Error("ollama unreachable", "url", cfg.OllamaURL, "error", err)
			return 1
		}
		slog.Info("ollama connected", "url", cfg.OllamaURL, "model", cfg.OllamaModel)
	}

	// Redis client for the summary cache (§7a). When SUMMARY_ENABLED and
	// REDIS_URL are set, the operator has explicitly opted into the cache —
	// any init failure is fatal so misconfiguration surfaces at boot rather
	// than as silently-missing context at request time.
	var rdb *redis.Client
	if cfg.SummaryEnabled && cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			slog.Error("invalid REDIS_URL", "error", err)
			return 1
		}
		rdb = redis.NewClient(opts)
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr := rdb.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			slog.Error("redis unreachable", "url", cfg.RedisURL, "error", pingErr)
			_ = rdb.Close()
			return 1
		}
		slog.Info("redis connected", "url", cfg.RedisURL)
	}
	summ := summarizer.New(cfg, rdb, wc, wc, gen)

	// Postgres-backed analytics store. When ANALYTICS_DB_URL is set the
	// operator has opted into persistence — any init failure (bad DSN,
	// network, missing DB, failed schema migration) is fatal so we never
	// silently drop Issues. Leave the URL empty to keep persistence off.
	var issueStore store.Store = store.NoOp()
	if cfg.AnalyticsDBURL != "" {
		initCtx, cancel := context.WithTimeout(context.Background(), cfg.AnalyticsDBTimeout)
		pg, err := store.NewPostgres(initCtx, store.PostgresOptions{
			URL:          cfg.AnalyticsDBURL,
			QueryTimeout: cfg.AnalyticsDBTimeout,
			MaxConns:     cfg.AnalyticsDBMaxConns,
		})
		cancel()
		if err != nil {
			slog.Error("analytics store init failed", "error", err)
			return 1
		}
		issueStore = pg
		slog.Info("analytics store enabled",
			"max_conns", cfg.AnalyticsDBMaxConns,
			"retention_days", cfg.IssueRetentionDays,
			"prune_interval", cfg.IssueRetentionPruneInterval.String())
	}

	engine := handler.New(cfg, wc, wc, gen, emb, summ, issueStore)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      engine,
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	}

	// Root context for long-running background workers (Kafka consumer).
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	var bgWG sync.WaitGroup
	var kafkaProducer *kafkaio.Producer

	if cfg.KafkaEnabled {
		// Reuse the same downstream clients as the HTTP path; Analyzer is
		// stateless beyond its config + injected deps.
		ret := retriever.New(wc, wc, emb, cfg)
		az := analyzer.New(ret, gen, summ, cfg)

		var pub kafkaio.Publisher
		if cfg.KafkaOutputTopic != "" {
			p, err := kafkaio.NewProducer(kafkaio.ProducerOptions{
				Brokers:        cfg.KafkaBrokers,
				ClientID:       cfg.KafkaClientID + "-producer",
				Topic:          cfg.KafkaOutputTopic,
				PublishTimeout: cfg.KafkaPublishTimeout,
			})
			if err != nil {
				slog.Error("kafka producer init failed", "error", err)
				return 1
			}
			kafkaProducer = p
			pub = p
		}

		// CDCUpdater: the *store.Postgres writes the analyzer result back to
		// the originating cdc_* row. When Postgres is not configured, the
		// consumer still runs but UpdateCDCFinal calls are silently skipped
		// (CDCUpdater is required; fall through to error below if nil).
		var cdcUpdater store.CDCUpdater
		if pg, ok := issueStore.(*store.Postgres); ok {
			cdcUpdater = pg
			slog.Info("cdc-updater enabled (writes final_* back to cdc_* rows)")
		} else {
			slog.Warn("analytics DB not configured — CDCUpdater unavailable; Kafka consumer disabled")
		}
		if cdcUpdater == nil {
			slog.Error("kafka consumer requires ANALYTICS_DB_URL to be set for cdc write-back")
			return 1
		}

		consumer, err := kafkaio.NewConsumer(kafkaio.ConsumerOptions{
			Brokers:           cfg.KafkaBrokers,
			ClientID:          cfg.KafkaClientID,
			GroupID:           cfg.KafkaGroupID,
			Topic:             cfg.KafkaInputTopic,
			SessionTimeout:    cfg.KafkaSessionTimeout,
			AnalyzeTimeout:    cfg.KafkaAnalyzeTimeout,
			Workers:           cfg.KafkaWorkers,
			ShutdownTimeout:   cfg.KafkaShutdownTimeout,
			ThresholdProvider: cdcUpdater.(store.ThresholdProvider),
			RepoMetaProvider:  cdcUpdater.(store.RepoMetaProvider),
			NotifLogger:       cdcUpdater.(store.NotifLogger),
			NotifyCooldown:    cfg.NotifyCooldown,
			Analyzer:          az,
			Publisher:         pub,
			Store:             issueStore,
			CDCUpdater:        cdcUpdater,
		})
		if err != nil {
			slog.Error("kafka consumer init failed", "error", err)
			if kafkaProducer != nil {
				kafkaProducer.Close(context.Background())
			}
			return 1
		}

		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			if err := consumer.Run(rootCtx); err != nil {
				slog.Error("kafka consumer exited with error", "error", err)
			}
		}()
	}

	// Retention pruner. Only runs when the Postgres store is active and
	// retention is finite (IssueRetentionDays > 0).
	if pg, ok := issueStore.(*store.Postgres); ok && cfg.IssueRetentionDays > 0 {
		retention := time.Duration(cfg.IssueRetentionDays) * 24 * time.Hour
		bgWG.Add(1)
		go runPruner(rootCtx, &bgWG, pg, retention, cfg.IssueRetentionPruneInterval)
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

	// Stop background workers (Kafka consumer) and flush producer.
	rootCancel()
	bgWG.Wait()
	if kafkaProducer != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), cfg.KafkaShutdownTimeout)
		kafkaProducer.Close(flushCtx)
		flushCancel()
	}
	issueStore.Close()

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

// runPruner deletes issues older than retention every interval until ctx is done.
// Errors are logged but never fatal.
func runPruner(ctx context.Context, wg *sync.WaitGroup, pg *store.Postgres, retention, interval time.Duration) {
	defer wg.Done()

	tick := time.NewTicker(interval)
	defer tick.Stop()

	prune := func() {
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		n, err := pg.Prune(runCtx, retention)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("issue pruner failed", "error", err)
			return
		}
		if n > 0 {
			slog.Info("issue pruner removed rows", "rows", n, "retention", retention.String())
		}
	}

	// Run once on startup so freshly-restarted services prune immediately.
	prune()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			prune()
		}
	}
}
