// Producer publishes analyzed Issues back to Kafka.
package kafkaio

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

// ProducerOptions configures a Producer.
type ProducerOptions struct {
	Brokers        string
	ClientID       string
	Topic          string
	PublishTimeout time.Duration
}

// Producer is a thin wrapper around a kgo client that publishes
// Issue JSON payloads to a single topic.
type Producer struct {
	opts   ProducerOptions
	client *kgo.Client
}

// NewProducer creates a Producer (does not start any goroutines).
func NewProducer(opts ProducerOptions) (*Producer, error) {
	if opts.Brokers == "" {
		return nil, fmt.Errorf("producer: brokers required")
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("producer: topic required")
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = 10 * time.Second
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(opts.Brokers, ",")...),
		kgo.ClientID(opts.ClientID),
		kgo.DefaultProduceTopic(opts.Topic),
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("producer kafka client: %w", err)
	}
	return &Producer{opts: opts, client: client}, nil
}

// Publish marshals iss as JSON and synchronously produces it to the
// configured topic with the given key. When logCtx is non-nil and
// non-empty, the payload is wrapped as {"issue": iss, "log": logCtx}
// so downstream consumers (notifier) can render per-log context
// (customer / flow IDs) on the card. Blocks up to PublishTimeout.
func (p *Producer) Publish(ctx context.Context, key []byte, iss *issue.Issue, logCtx map[string]any) error {
	if iss == nil {
		return fmt.Errorf("publish: nil issue")
	}
	var body []byte
	var err error
	if len(logCtx) > 0 {
		body, err = json.Marshal(map[string]any{"issue": iss, "log": logCtx})
	} else {
		body, err = json.Marshal(iss)
	}
	if err != nil {
		return fmt.Errorf("marshal issue: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, p.opts.PublishTimeout)
	defer cancel()
	rec := &kgo.Record{Topic: p.opts.Topic, Key: key, Value: body}
	if err := p.client.ProduceSync(pctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}
	return nil
}

// Close flushes pending records and shuts the client down.
func (p *Producer) Close(ctx context.Context) {
	if err := p.client.Flush(ctx); err != nil {
		slog.Warn("producer flush failed", "error", err)
	}
	p.client.Close()
}
