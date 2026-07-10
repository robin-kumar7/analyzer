// Consumer reads enriched log envelopes from the logs.enriched Kafka topic
// (produced by log-classifier), runs each one through the analyzer pipeline,
// writes the final verdict back to the originating cdc_* row, and optionally
// publishes the resulting Issue to a downstream sink (e.g. notifier).
//
// Offset commit strategy — "at-least-once with per-partition ordering":
//
//  1. kgo.AutoCommitMarks() is the ONLY commit path. Marks are written
//     by handleRecord immediately after it reaches a terminal state.
//
//  2. One goroutine per assigned Kafka partition (lazy, on first record).
//     Within a partition, records are processed one-at-a-time and marked
//     in strict offset order, so the committed offset can never skip ahead
//     of an in-flight record on the same partition.
//
//  3. On shutdown (ctx cancelled):
//     - The dispatcher stops sending new records to partition workers.
//     - Each partition worker's channel is closed; remaining buffered
//     records are processed with the already-cancelled context.
//     handleRecord detects ctx.Done() and returns WITHOUT marking —
//     those records are re-delivered on the next start.
//     - A final CommitMarkedOffsets flush writes any marks not yet
//     auto-committed by the background ticker.
//
//  4. Poison records (decode errors) are always marked to prevent a
//     stalled partition; genuine processing failures are also marked
//     (visible loss is preferred over silently-growing lag).
//
// Per-partition channel capacity is 1. This lets the dispatcher
// pre-stage the next record while the worker is still running the
// current one, so Kafka poll and LLM processing overlap. The dispatcher
// only blocks when both the in-flight slot and the staged slot are
// occupied, which provides natural backpressure without starving other
// partitions.
package kafkaio

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/infoblox/vibecoder-analyzer/internal/analyzer"
	"github.com/infoblox/vibecoder-analyzer/internal/issue"
	"github.com/infoblox/vibecoder-analyzer/internal/store"
)

// partKey uniquely identifies a topic-partition pair.
type partKey struct {
	topic     string
	partition int32
}

// partWorker holds the per-partition processing state.
type partWorker struct {
	recs chan *kgo.Record // buffered(1): dispatcher can stage one record ahead
	done chan struct{}    // closed when the worker goroutine exits
}

// Runner is the subset of *analyzer.Analyzer used by the consumer.
type Runner interface {
	Run(ctx context.Context, q analyzer.Query) (*issue.Issue, error)
}

// Publisher publishes an analyzed Issue to a downstream sink (typically Kafka).
// Implementations should be safe to call from multiple goroutines.
// A nil Publisher disables publishing (the Issue is just logged).
type Publisher interface {
	Publish(ctx context.Context, key []byte, iss *issue.Issue, logCtx map[string]any) error
}

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	Brokers         string
	ClientID        string
	GroupID         string
	Topic           string
	SessionTimeout  time.Duration
	AnalyzeTimeout  time.Duration
	Workers         int // kept for compat; unused — concurrency = partition count
	ShutdownTimeout time.Duration

	// ThresholdProvider looks up the per-service notification gate from
	// service_mappings (DB-backed, short-TTL cache). When nil, every
	// Issue is forwarded to Publisher (no filtering).
	ThresholdProvider store.ThresholdProvider

	// RepoMetaProvider looks up repo URL + default branch for the
	// service so the outbound Kafka envelope can carry them for the
	// notifier's deep-link buttons. Optional; when nil the notifier
	// still renders the card but without an "Open source" link.
	RepoMetaProvider store.RepoMetaProvider

	// NotifLogger deduplicates outbound notifications. When set, the
	// consumer checks whether the same bugFingerprint was already sent
	// within NotifyCooldown before forwarding to Publisher. On send it
	// records the fingerprint via MarkNotified. When nil, every Issue
	// is forwarded without deduplication.
	NotifLogger    store.NotifLogger
	NotifyCooldown time.Duration // 0 = use default (1h)

	Analyzer  Runner
	Publisher Publisher // optional

	// CDCUpdater fetches the full log row and writes the analyzer result
	// back to the originating cdc_* table. Required.
	CDCUpdater store.CDCUpdater

	// Store persists every successfully analyzed Issue for analytics (best-effort).
	// nil disables persistence; the pipeline keeps running.
	Store store.Store
}

// Consumer is a Kafka consumer group that routes records to per-partition
// worker goroutines for strictly-ordered processing and safe offset commits.
type Consumer struct {
	opts      ConsumerOptions
	client    *kgo.Client
	workersMu sync.Mutex
	workers   map[partKey]*partWorker
}

// NewConsumer creates a Consumer (does not start it).
func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if opts.Analyzer == nil {
		return nil, fmt.Errorf("consumer: Analyzer is required")
	}
	if opts.CDCUpdater == nil {
		return nil, fmt.Errorf("consumer: CDCUpdater is required")
	}
	if opts.Brokers == "" {
		return nil, fmt.Errorf("consumer: brokers required")
	}
	if opts.GroupID == "" {
		return nil, fmt.Errorf("consumer: group ID required")
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("consumer: topic required")
	}
	if opts.SessionTimeout <= 0 {
		opts.SessionTimeout = 5 * time.Minute
	}
	if opts.AnalyzeTimeout <= 0 {
		opts.AnalyzeTimeout = 5 * time.Minute
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 30 * time.Second
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(opts.Brokers, ",")...),
		kgo.ClientID(opts.ClientID),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topic),
		kgo.SessionTimeout(opts.SessionTimeout),
		// ConsumeResetOffset only applies to consumer groups with NO committed
		// offset yet (first start or group reset). Groups with an existing
		// committed offset always resume from that offset regardless of this
		// option — which is exactly what we want for restart-from-where-left-off.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer kafka client: %w", err)
	}
	return &Consumer{
		opts:    opts,
		client:  client,
		workers: map[partKey]*partWorker{},
	}, nil
}

// Run blocks until ctx is cancelled. It dispatches records to
// per-partition worker goroutines, then drains and shuts them down
// before doing a final offset commit.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()

	slog.Info("kafka consumer started",
		"topic", c.opts.Topic,
		"group", c.opts.GroupID,
		"offset_policy", "per-partition-sequential")

	c.dispatchLoop(ctx)

	// --- Shutdown sequence ---
	//
	// 1. Close every partition channel so each worker drains its buffer
	//    (at most 1 pre-staged record) and then exits.
	c.workersMu.Lock()
	for _, w := range c.workers {
		close(w.recs)
	}
	snapshot := make([]*partWorker, 0, len(c.workers))
	for _, w := range c.workers {
		snapshot = append(snapshot, w)
	}
	c.workersMu.Unlock()

	// 2. Wait for all partition workers to finish.
	for _, w := range snapshot {
		<-w.done
	}

	// 3. Best-effort flush of any marks not yet pushed by the auto-commit ticker.
	flushCtx, cancel := context.WithTimeout(context.Background(), c.opts.ShutdownTimeout)
	defer cancel()
	if err := c.client.CommitMarkedOffsets(flushCtx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("kafka final commit failed", "error", err)
	}
	return nil
}

// dispatchLoop polls Kafka and routes each record to its partition worker.
// Returns when ctx is cancelled.
func (c *Consumer) dispatchLoop(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				slog.Warn("kafka fetch error",
					"topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
			}
			// Back-off briefly on errors; if the context was cancelled
			// during the sleep we will detect it at the next loop iteration.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			w := c.workerFor(ctx, rec.Topic, rec.Partition)
			if w == nil {
				return // ctx cancelled while spawning a new worker
			}
			select {
			case <-ctx.Done():
				return
			case w.recs <- rec:
			}
		}
	}
}

// workerFor returns the per-partition worker, lazily spawning one on
// the first record from that partition.
func (c *Consumer) workerFor(ctx context.Context, topic string, partition int32) *partWorker {
	key := partKey{topic: topic, partition: partition}
	c.workersMu.Lock()
	defer c.workersMu.Unlock()
	if w, ok := c.workers[key]; ok {
		return w
	}
	if ctx.Err() != nil {
		return nil // don't spawn new workers during shutdown
	}
	w := &partWorker{
		recs: make(chan *kgo.Record, 1), // capacity 1: pipeline one record ahead
		done: make(chan struct{}),
	}
	c.workers[key] = w
	go c.runWorker(ctx, w, key)
	return w
}

// runWorker processes records for one partition strictly sequentially.
// After each record reaches a terminal state, handleRecord marks the
// offset via MarkCommitRecords. Because records arrive in offset order
// (guaranteed by the single Kafka partition → single channel mapping)
// and are processed one-at-a-time, marks are always issued in
// monotone offset order for this partition.
func (c *Consumer) runWorker(ctx context.Context, w *partWorker, key partKey) {
	defer close(w.done)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("partition worker panic recovered",
				"topic", key.topic, "partition", key.partition, "panic", r)
		}
	}()
	for rec := range w.recs {
		c.handleRecord(ctx, rec)
	}
}

// handleRecord runs the full pipeline for one logs.enriched record:
//  1. Decode enriched envelope.
//  2. Fetch the full cdc_* row from Postgres.
//  3. Run the analyzer pipeline.
//  4. Write the final verdict back to the cdc_* row.
//  5. Persist to the issues table (best-effort analytics).
//  6. Publish to notifier if threshold is met.
func (c *Consumer) handleRecord(ctx context.Context, rec *kgo.Record) {
	start := time.Now()

	// 1. Decode envelope.
	env, err := decodeEnriched(rec.Value)
	if err != nil {
		// Log the full raw value (capped at 2 KB) so the operator can see
		// exactly what landed on the topic and diagnose wrong-topic issues.
		rawBody := string(rec.Value)
		const maxBody = 2048
		if len(rawBody) > maxBody {
			rawBody = rawBody[:maxBody] + "…(truncated)"
		}
		slog.Error("poison enriched record dropped — check KAFKA_INPUT_TOPIC is set to logs.enriched",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
			"size", len(rec.Value),
			"raw_body", rawBody,
			"error", err)
		c.client.MarkCommitRecords(rec)
		return
	}

	// 2. Fetch full log row from Postgres.
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 15*time.Second)
	cdcRow, err := c.opts.CDCUpdater.FetchCDCRow(fetchCtx, env.TableName, env.RowID)
	fetchCancel()
	if err != nil {
		// Table gone (SQLSTATE 42P01) is a benign situation: Kafka
		// still holds records produced against a cdc_* table that
		// has since been dropped (e.g. the retired cdc_other
		// fallback). Log at WARN, commit the offset, and move on so
		// the log doesn't fill with ERRORs for records nothing can
		// ever process.
		if errors.Is(err, store.ErrTableMissing) {
			slog.Warn("cdc table gone; skipping stale enriched record",
				"table", env.TableName, "row_id", env.RowID,
				"offset", rec.Offset,
				"hint", "this is expected right after removing a cdc_* table; drain the topic and it will stop")
			c.client.MarkCommitRecords(rec)
			return
		}
		slog.Error("fetch cdc row failed; dropping record",
			"table", env.TableName, "row_id", env.RowID,
			"offset", rec.Offset, "error", err)
		c.client.MarkCommitRecords(rec)
		return
	}

	// Resolve the Weaviate repo: envelope carries it when the service mapping
	// was known at classification time; fall back to the cdc_* row value.
	repo := env.WeaviateRepo
	if repo == "" {
		repo = cdcRow.WeaviateRepo
	}
	serviceName := env.ServiceName
	if serviceName == "" {
		serviceName = cdcRow.ServiceName
	}

	// 3. Run analyzer pipeline with raw log + repo scope.
	runCtx, runCancel := context.WithTimeout(ctx, c.opts.AnalyzeTimeout)
	iss, err := c.opts.Analyzer.Run(runCtx, analyzer.Query{
		Input: cdcRow.Raw,
		Mode:  "logs",
		Repo:  repo,
	})
	runCancel()

	if iss != nil && serviceName != "" && iss.ServiceName == "" {
		iss.ServiceName = serviceName
	}

	// 4. Write result back to the cdc_* row.
	// Do this regardless of whether the analyzer returned an error — on
	// error we write a 'failed' marker so the UI doesn't show 'pending' forever.
	updateCtx, updateCancel := context.WithTimeout(context.Background(), 15*time.Second)
	updateErr := c.opts.CDCUpdater.UpdateCDCFinal(updateCtx, env.TableName, env.RowID, iss)
	updateCancel()
	if updateErr != nil {
		slog.Warn("update cdc final failed (non-fatal)",
			"table", env.TableName, "row_id", env.RowID, "error", updateErr)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			slog.Info("analyze interrupted by shutdown",
				"table", env.TableName, "row_id", env.RowID, "offset", rec.Offset)
			return // do NOT mark — redelivered on restart
		}
		slog.Error("analyze failed; marking record",
			"table", env.TableName, "row_id", env.RowID, "offset", rec.Offset,
			"duration_ms", time.Since(start).Milliseconds(), "error", err)
		c.client.MarkCommitRecords(rec)
		return
	}

	// 5. Persist to issues table for analytics (best-effort).
	// Save() mutates logCtx with the new issue_id so the outbound
	// Kafka envelope carries it for the notifier's "View issue" link.
	logCtx := buildEnrichedLogCtx(env, cdcRow)
	store.SaveOrLog(ctx, c.opts.Store, iss, logCtx, store.SourceKafka)

	// Attach repo URL + default branch so the notifier can render a
	// clickable "Open source" link to the GitHub file. Best-effort:
	// a missing mapping just omits the link, never blocks the publish.
	if c.opts.RepoMetaProvider != nil {
		if meta, err := c.opts.RepoMetaProvider.LookupRepoMeta(ctx, iss.ServiceName, iss.ResolvedRepo); err == nil {
			if meta.RepoURL != "" {
				logCtx["repo_url"] = meta.RepoURL
			}
			if meta.DefaultBranch != "" {
				logCtx["default_branch"] = meta.DefaultBranch
			}
		} else {
			slog.Debug("repo meta lookup failed (publish will omit source link)",
				"service", iss.ServiceName, "repo", iss.ResolvedRepo, "err", err)
		}
	}

	// 6. Publish to notifier topic if per-service threshold is met.
	if c.opts.Publisher != nil {
		// Compute fingerprint once and stash in logCtx so the notifier
		// can use it for its own Redis-based dedup layer.
		fp := store.BugFingerprint(iss.ServiceName, iss.ResolvedRepo, iss.File, iss.Line, iss.Category)
		logCtx["fingerprint"] = fp

		if skip, reason := c.shouldSkipPublish(ctx, iss, fp); skip {
			slog.Info("analyzed record (publish skipped)",
				"table", env.TableName, "row_id", env.RowID,
				"severity", iss.Severity, "confidence", iss.Confidence,
				"reason", reason,
				"duration_ms", time.Since(start).Milliseconds())
			c.client.MarkCommitRecords(rec)
			return
		}
		if err := c.opts.Publisher.Publish(ctx, rec.Key, iss, logCtx); err != nil {
			if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() != nil {
				slog.Info("publish interrupted by shutdown",
					"table", env.TableName, "row_id", env.RowID)
				return // do NOT mark
			}
			slog.Error("publish failed; marking record",
				"table", env.TableName, "row_id", env.RowID,
				"severity", iss.Severity, "error", err)
			c.client.MarkCommitRecords(rec)
			return
		}
		// Record the send so subsequent identical bugs are suppressed
		// within the cooldown window.
		if c.opts.NotifLogger != nil {
			if err := c.opts.NotifLogger.MarkNotified(ctx, fp, iss.Title, iss.ServiceName); err != nil {
				slog.Warn("notification_log write failed (non-fatal)", "err", err)
			}
		}
	}

	slog.Info("analyzed record",
		"table", env.TableName, "row_id", env.RowID,
		"severity", iss.Severity, "category", iss.Category, "repo", iss.ResolvedRepo,
		"file", iss.File, "grounded", iss.GroundingOK,
		"duration_ms", time.Since(start).Milliseconds())
	c.client.MarkCommitRecords(rec)
}

// shouldSkipPublish applies two gates:
//  1. Per-service threshold (severity rank + confidence) from service_mappings.
//  2. Cooldown deduplication: if the same bugFingerprint was already
//     notified within the configured window, suppress this one.
//
// Falls back to publishing when a gate can't be evaluated (DB unreachable
// etc.) — degrading to "publish anyway" is safer than silent drops.
func (c *Consumer) shouldSkipPublish(ctx context.Context, iss *issue.Issue, fingerprint string) (bool, string) {
	// Gate 1: per-service severity / confidence threshold.
	if c.opts.ThresholdProvider != nil {
		threshold, err := c.opts.ThresholdProvider.LookupNotifyThreshold(ctx, iss.ServiceName)
		if err != nil {
			slog.Warn("threshold lookup failed; publishing anyway",
				"service", iss.ServiceName, "error", err)
		} else {
			if threshold.MinSeverityRank > 0 {
				rank := severityRank(iss.Severity)
				if rank < threshold.MinSeverityRank {
					return true, fmt.Sprintf("severity %q below threshold", iss.Severity)
				}
			}
			if threshold.MinConfidence > 0 && iss.Confidence < threshold.MinConfidence {
				return true, fmt.Sprintf("confidence %.2f below threshold %.2f", iss.Confidence, threshold.MinConfidence)
			}
		}
	}

	// Gate 2: cooldown deduplication.
	if c.opts.NotifLogger != nil && fingerprint != "" {
		cooldown := c.opts.NotifyCooldown
		if cooldown <= 0 {
			cooldown = time.Hour // default: 1h per unique bug
		}
		ok, err := c.opts.NotifLogger.ShouldNotify(ctx, fingerprint, cooldown)
		if err != nil {
			slog.Warn("notification_log check failed; publishing anyway", "err", err)
		} else if !ok {
			return true, fmt.Sprintf("duplicate: same bug notified within %s cooldown", cooldown)
		}
	}

	return false, ""
}

// buildEnrichedLogCtx builds the logCtx map from an enrichedEnvelope and
// the fetched CDCRow. It is passed to store.Save and Publisher.Publish so
// downstream consumers (notifier) have full tenant context.
func buildEnrichedLogCtx(env *enrichedEnvelope, row *store.CDCRow) map[string]any {
	ctx := map[string]any{
		"table_name":   env.TableName,
		"row_id":       env.RowID,
		"service_name": env.ServiceName,
		"severity":     env.Severity,
	}
	if env.Ophid != "" {
		ctx["ophid"] = env.Ophid
	}
	if env.AccountID != "" {
		ctx["account_id"] = env.AccountID
	}
	if env.FlowID != "" {
		ctx["flow_id"] = env.FlowID
	}
	if env.CorrelationID != "" {
		ctx["correlation_id"] = env.CorrelationID
	}
	if row != nil && row.Raw != "" {
		ctx["line"] = row.Raw
	}
	return ctx
}

func severityRank(s string) int {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	case "critical":
		return 4
	default:
		return 0
	}
}

// preview returns a short, safe rendering of a payload for error logs.
func preview(b []byte) string {
	const maxLen = 256
	s := string(b)
	if len(s) > maxLen {
		s = s[:maxLen] + "…"
	}
	s = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\n' && r != '\t' {
			return '.'
		}
		return r
	}, s)
	return s
}

// headerValue returns the first matching Kafka header value (case-insensitive).
// Retained for completeness; not used in the enriched-envelope path.
func headerValue(headers []kgo.RecordHeader, name string) string {
	for _, h := range headers {
		if strings.EqualFold(h.Key, name) {
			return string(h.Value)
		}
	}
	return ""
}
