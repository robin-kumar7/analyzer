package kafkaio

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/infoblox/vibecoder-analyzer/internal/analyzer"
	"github.com/infoblox/vibecoder-analyzer/internal/issue"
	"github.com/infoblox/vibecoder-analyzer/internal/store"
)

// ── decodeEnriched tests ──────────────────────────────────────────────────────

func TestDecodeEnriched(t *testing.T) {
	tests := []struct {
		name      string
		value     []byte
		wantErr   bool
		wantRowID int64
		wantTable string
	}{
		{
			name:      "valid envelope",
			value:     []byte(`{"schema_version":1,"row_id":42,"table_name":"cdc_grpc_in","ts":"2026-06-25T17:58:38.719Z","service_name":"grpc-in","severity":"error","kafka_topic":"service-logs","kafka_partition":0,"kafka_offset":56}`),
			wantErr:   false,
			wantRowID: 42,
			wantTable: "cdc_grpc_in",
		},
		{
			name:    "empty value",
			value:   nil,
			wantErr: true,
		},
		{
			name:    "invalid JSON",
			value:   []byte(`not json`),
			wantErr: true,
		},
		{
			name:    "missing row_id",
			value:   []byte(`{"schema_version":1,"table_name":"cdc_grpc_in"}`),
			wantErr: true,
		},
		{
			name:    "missing table_name",
			value:   []byte(`{"schema_version":1,"row_id":1}`),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := decodeEnriched(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("decodeEnriched err = %v, wantErr = %v", err, tt.wantErr)
			}
			if err == nil {
				if e.RowID != tt.wantRowID {
					t.Errorf("RowID = %d, want %d", e.RowID, tt.wantRowID)
				}
				if e.TableName != tt.wantTable {
					t.Errorf("TableName = %q, want %q", e.TableName, tt.wantTable)
				}
			}
		})
	}
}

// ── stubs ─────────────────────────────────────────────────────────────────────

type stubRunner struct {
	gotInput string
	gotRepo  string
	gotMode  string
	iss      *issue.Issue
	err      error
}

func (s *stubRunner) Run(_ context.Context, q analyzer.Query) (*issue.Issue, error) {
	s.gotInput = q.Input
	s.gotRepo = q.Repo
	s.gotMode = q.Mode
	return s.iss, s.err
}

// compile-time check that *stubRunner satisfies Runner.
var _ Runner = (*stubRunner)(nil)

type stubCDCUpdater struct {
	row       *store.CDCRow
	fetchErr  error
	updateErr error
	updated   bool
}

func (s *stubCDCUpdater) FetchCDCRow(_ context.Context, _ string, _ int64) (*store.CDCRow, error) {
	return s.row, s.fetchErr
}
func (s *stubCDCUpdater) UpdateCDCFinal(_ context.Context, _ string, _ int64, _ *issue.Issue) error {
	s.updated = true
	return s.updateErr
}

var _ store.CDCUpdater = (*stubCDCUpdater)(nil)

// ── interface tests ───────────────────────────────────────────────────────────

func TestRunnerInterface(t *testing.T) {
	s := &stubRunner{iss: &issue.Issue{Severity: "high"}}
	got, err := s.Run(context.Background(), analyzer.Query{Input: "x", Mode: "logs"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.Severity != "high" {
		t.Errorf("Severity = %q, want high", got.Severity)
	}
	if s.gotInput != "x" || s.gotMode != "logs" {
		t.Errorf("query not passed through: %+v", s)
	}
}

func TestRunnerError(t *testing.T) {
	want := errors.New("boom")
	s := &stubRunner{err: want}
	_, err := s.Run(context.Background(), analyzer.Query{})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

type stubThresholdProvider struct {
	threshold store.NotifyThreshold
	err       error
}

func (s *stubThresholdProvider) LookupNotifyThreshold(_ context.Context, _ string) (store.NotifyThreshold, error) {
	return s.threshold, s.err
}

var _ store.ThresholdProvider = (*stubThresholdProvider)(nil)

// ── buildEnrichedLogCtx ───────────────────────────────────────────────────────

func TestBuildEnrichedLogCtx(t *testing.T) {
	env := &enrichedEnvelope{
		TableName:   "cdc_grpc_in",
		RowID:       7,
		ServiceName: "grpc-in",
		Severity:    "error",
		AccountID:   "acct-1",
		FlowID:      "flow-2",
		TsRaw:       time.Now().UTC().Format(time.RFC3339Nano),
	}
	row := &store.CDCRow{Raw: "panic: nil pointer"}
	ctx := buildEnrichedLogCtx(env, row)
	if ctx["table_name"] != "cdc_grpc_in" {
		t.Errorf("table_name = %v", ctx["table_name"])
	}
	if ctx["row_id"] != int64(7) {
		t.Errorf("row_id = %v", ctx["row_id"])
	}
	if ctx["account_id"] != "acct-1" {
		t.Errorf("account_id = %v", ctx["account_id"])
	}
	if ctx["line"] != "panic: nil pointer" {
		t.Errorf("line = %v", ctx["line"])
	}
}
