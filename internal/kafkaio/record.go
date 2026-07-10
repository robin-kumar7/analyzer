// Package kafkaio implements the analyzer's Kafka ingestion path.
//
// Input topic (v3): logs.enriched — small JSON envelopes produced by
// log-classifier after it has inserted the raw log into a cdc_* table.
// Each envelope carries a (row_id, table_name) pointer plus service
// metadata. The analyzer fetches the full log row from Postgres, runs
// its big-LLM pipeline, and writes the result back to the same cdc_*
// row via UpdateCDCFinal.
//
// See docs/04-analyzer-service.md §7c and docs/10-log-classifier-service.md §5b.
package kafkaio

import (
	"encoding/json"
	"fmt"
	"time"
)

// enrichedEnvelope matches the JSON produced by log-classifier to the
// logs.enriched Kafka topic. See log-classifier/internal/envelope/envelope.go.
//
// Ts is kept as a raw string (not time.Time) so that a malformed
// timestamp — e.g. nanosecond-integer records accidentally landing on
// the wrong topic — never prevents us from reaching the row_id /
// table_name validation that gives the operator a useful error.
type enrichedEnvelope struct {
	SchemaVersion int    `json:"schema_version"`
	RowID         int64  `json:"row_id"`
	TableName     string `json:"table_name"`
	TsRaw         string `json:"ts"`

	ServiceName  string `json:"service_name,omitempty"`
	Container    string `json:"container,omitempty"`
	WeaviateRepo string `json:"weaviate_repo,omitempty"`

	Severity      string `json:"severity"`
	LogType       string `json:"log_type"`
	Ophid         string `json:"ophid,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
	FlowID        string `json:"flow_id,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`

	KafkaTopic     string `json:"kafka_topic"`
	KafkaPartition int32  `json:"kafka_partition"`
	KafkaOffset    int64  `json:"kafka_offset"`

	// Optional: set when the classifier LLM ran (Phase 2 of log-classifier).
	ClassifierTitle        string  `json:"classifier_title,omitempty"`
	ClassifierCategory     string  `json:"classifier_category,omitempty"`
	ClassifierSeverityHint string  `json:"classifier_severity_hint,omitempty"`
	ClassifierConfidence   float64 `json:"classifier_confidence,omitempty"`
}

// Ts parses TsRaw as RFC3339. Returns zero-time on failure; callers
// that need the timestamp must handle the zero case.
func (e *enrichedEnvelope) Ts() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, e.TsRaw)
	return t
}

// decodeEnriched parses a logs.enriched Kafka record value.
// Returns a structured error when the payload is not a valid enriched
// envelope, including a human-readable hint about the likely cause so
// operators can distinguish "wrong topic" from "malformed record".
func decodeEnriched(value []byte) (*enrichedEnvelope, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("empty record value")
	}
	var e enrichedEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, fmt.Errorf("json decode enriched envelope: %w", err)
	}
	// Structural validation: these two fields are required for the
	// analyzer to fetch the Postgres row. Their absence almost always
	// means the record came from the wrong Kafka topic (e.g. service-logs
	// instead of logs.enriched). Surface that hint explicitly.
	if e.RowID <= 0 {
		hint := ""
		if e.isRawLogShipperRecord(value) {
			hint = " (record looks like a raw log-shipper envelope — is KAFKA_INPUT_TOPIC set to logs.enriched?)"
		}
		return nil, fmt.Errorf("enriched envelope: row_id must be > 0 (got %d)%s", e.RowID, hint)
	}
	if e.TableName == "" {
		return nil, fmt.Errorf("enriched envelope: table_name is required")
	}
	return &e, nil
}

// isRawLogShipperRecord returns true when the payload looks like a
// log-shipper record (has "stream" + "line" keys but no "row_id").
// Used only to produce a better error hint — the boolean is never
// used for routing decisions.
func (e *enrichedEnvelope) isRawLogShipperRecord(raw []byte) bool {
	var probe struct {
		Stream map[string]string `json:"stream"`
		Line   string            `json:"line"`
	}
	return json.Unmarshal(raw, &probe) == nil && probe.Line != "" && len(probe.Stream) > 0
}

// ── kept for reference (no longer the primary decode path) ───────────────────
// The unwrapEnvelope helper below is retained in case the HTTP path or
// tests still need it. It is not used by the Kafka consumer in v3.

func unwrapEnvelope(line string, stream map[string]string) (string, map[string]string) {
	// No-op stub — the consumer no longer processes raw log lines.
	return line, stream
}
