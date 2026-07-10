package store

import (
	"context"
	"strings"
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

func TestBugFingerprint_Deterministic(t *testing.T) {
	a := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	b := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	if a != b {
		t.Fatalf("fingerprint not deterministic: %q != %q", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("expected 64-char hex digest, got %d", len(a))
	}
}

func TestBugFingerprint_CaseInsensitive(t *testing.T) {
	a := bugFingerprint("HTTP-OUT", "CDC_HTTP_OUT", "Auth.go", 42, "Bug")
	b := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	if a != b {
		t.Fatalf("fingerprint should be case-insensitive:\n  %q\n  %q", a, b)
	}
}

func TestBugFingerprint_FieldsMatter(t *testing.T) {
	base := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	cases := map[string]string{
		"different service":  bugFingerprint("grpc-in", "cdc_http_out", "auth.go", 42, "bug"),
		"different repo":     bugFingerprint("http-out", "other", "auth.go", 42, "bug"),
		"different file":     bugFingerprint("http-out", "cdc_http_out", "other.go", 42, "bug"),
		"different line":     bugFingerprint("http-out", "cdc_http_out", "auth.go", 43, "bug"),
		"different category": bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "config"),
	}
	for name, got := range cases {
		if got == base {
			t.Errorf("%s: fingerprint collided with base", name)
		}
	}
}

func TestBugFingerprint_EmptyAndZeroLineNormalized(t *testing.T) {
	a := bugFingerprint("", "", "", 0, "")
	b := bugFingerprint(" ", " ", " ", -1, " ")
	if a != b {
		t.Fatalf("empty / whitespace / non-positive line should normalize identically:\n  %q\n  %q", a, b)
	}
}

func TestTenantFingerprint_TenantBound(t *testing.T) {
	bug := func(account string) string {
		return tenantFingerprint(account, "flow-1", "oph-1", "http-out", "auth.go", 42, "bug")
	}
	a := bug("acme")
	b := bug("globex")
	if a == b {
		t.Fatal("tenant fingerprint must differ across accounts")
	}
	if bug("acme") != bug("acme") {
		t.Fatal("tenant fingerprint must be deterministic")
	}
}

func TestTenantFingerprint_DifferentFromBugFingerprint(t *testing.T) {
	bug := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	tenant := tenantFingerprint("acme", "flow-1", "oph-1", "http-out", "auth.go", 42, "bug")
	if bug == tenant {
		t.Fatal("bug fingerprint and tenant fingerprint must not collide")
	}
}

func TestTenancyFromLogCtx(t *testing.T) {
	cases := []struct {
		name                       string
		in                         map[string]any
		wantAcc, wantFlow, wantOph string
	}{
		{"nil", nil, "", "", ""},
		{"empty", map[string]any{}, "", "", ""},
		{"no stream", map[string]any{"ts": "1"}, "", "", ""},
		{
			"happy path",
			map[string]any{"stream": map[string]any{
				"account_id": "acme-7",
				"flow_id":    "ingest.v3",
				"ophid":      "oph-9",
			}},
			"acme-7", "ingest.v3", "oph-9",
		},
		{
			"customer_id aliases account_id",
			map[string]any{"stream": map[string]any{
				"customer_id": "acme-7",
			}},
			"acme-7", "", "",
		},
		{
			"accountId (camelCase) aliases account_id",
			map[string]any{"stream": map[string]any{
				"accountId": "3000137",
			}},
			"3000137", "", "",
		},
		{
			"oph_id aliases ophid",
			map[string]any{"stream": map[string]any{
				"oph_id": "oph-9",
			}},
			"", "", "oph-9",
		},
		{
			"non-string values ignored",
			map[string]any{"stream": map[string]any{
				"account_id": 123, // int, not string
			}},
			"", "", "",
		},
		{
			"agentId aliases ophid (cdc_grpc_in promtail label)",
			map[string]any{"stream": map[string]any{
				"agentId": "3327d2e0b02888d418fd2dff21a6a331",
			}},
			"", "", "3327d2e0b02888d418fd2dff21a6a331",
		},
		{
			"ophid extracted from line text when stream is silent",
			map[string]any{
				"line": "Subscription failed: failed to subscribe to topic for ophid 3327d2e0b02888d418fd2dff21a6a331 and flow ID [182171037859842 182171037859840]: rpc error: code = Canceled",
				"stream": map[string]any{
					"accountId": "2010391",
				},
			},
			"2010391", "182171037859842", "3327d2e0b02888d418fd2dff21a6a331",
		},
		{
			"line text fallback ignored when stream already populated",
			map[string]any{
				"line": "Subscription failed for ophid OVERRIDE and flow ID [99999]",
				"stream": map[string]any{
					"agentId": "winning-ophid",
					"flow_id": "winning-flow",
				},
			},
			"", "winning-flow", "winning-ophid",
		},
		{
			"single flow_id in line (no brackets)",
			map[string]any{
				"line": "operation failed for flow_id=42 in subsystem",
			},
			"", "42", "",
		},
		{
			"oph_id with hyphen variant",
			map[string]any{
				"line": "audit op_id: abc12345 was canceled",
			},
			"", "", "abc12345",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, f, o := tenancyFromLogCtx(tc.in)
			if a != tc.wantAcc || f != tc.wantFlow || o != tc.wantOph {
				t.Errorf("got (%q,%q,%q), want (%q,%q,%q)",
					a, f, o, tc.wantAcc, tc.wantFlow, tc.wantOph)
			}
		})
	}
}

func TestNoOp_NeverErrors(t *testing.T) {
	s := NoOp()
	defer s.Close()
	err := s.Save(context.Background(), &issue.Issue{Title: "x"}, nil, SourceHTTP)
	if err != nil {
		t.Fatalf("noop.Save returned error: %v", err)
	}
	err = s.Save(context.Background(), nil, nil, SourceKafka)
	if err != nil {
		t.Fatalf("noop.Save(nil) returned error: %v", err)
	}
}

func TestSaveOrLog_NilStoreIsSafe(t *testing.T) {
	// Just must not panic.
	SaveOrLog(context.Background(), nil, &issue.Issue{Title: "x"}, nil, SourceHTTP)
}

// recordingStore lets us assert SaveOrLog forwards the right Source.
type recordingStore struct {
	calls []recordedCall
	err   error
}

type recordedCall struct {
	iss    *issue.Issue
	logCtx map[string]any
	source Source
}

func (r *recordingStore) Save(_ context.Context, iss *issue.Issue, logCtx map[string]any, source Source) error {
	r.calls = append(r.calls, recordedCall{iss: iss, logCtx: logCtx, source: source})
	return r.err
}
func (r *recordingStore) Close() {}

func TestSaveOrLog_ForwardsArgs(t *testing.T) {
	rec := &recordingStore{}
	iss := &issue.Issue{Title: "x", Severity: "high"}
	logCtx := map[string]any{"stream": map[string]any{"customer_id": "acme"}}

	SaveOrLog(context.Background(), rec, iss, logCtx, SourceKafka)

	if len(rec.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(rec.calls))
	}
	c := rec.calls[0]
	if c.iss != iss || c.source != SourceKafka {
		t.Fatalf("unexpected forward: iss=%v source=%q", c.iss, c.source)
	}
	if c.logCtx["stream"].(map[string]any)["customer_id"] != "acme" {
		t.Fatalf("log ctx not forwarded")
	}
}

func TestSaveOrLog_SwallowsError(t *testing.T) {
	rec := &recordingStore{err: errBoom}
	// Must not panic; must not propagate.
	SaveOrLog(context.Background(), rec, &issue.Issue{Title: "x"}, nil, SourceHTTP)
}

var errBoom = stringError("boom")

type stringError string

func (e stringError) Error() string { return string(e) }

func TestHashParts_PipeSeparated(t *testing.T) {
	// Sanity: changing only the separator content should change the hash.
	a := hashParts("a", "b", "c")
	b := hashParts("a|b", "c")
	if a == b {
		t.Fatal("pipe collisions: separator must be preserved across parts")
	}
}

func TestNormalize(t *testing.T) {
	if got := normalize(" Foo "); got != "foo" {
		t.Errorf("normalize: got %q, want %q", got, "foo")
	}
	if got := normalize(""); got != "-" {
		t.Errorf("normalize empty: got %q, want %q", got, "-")
	}
	if got := normalizeInt(0); got != "-" {
		t.Errorf("normalizeInt(0): got %q, want %q", got, "-")
	}
	if got := normalizeInt(42); got != "42" {
		t.Errorf("normalizeInt(42): got %q, want %q", got, "42")
	}
}

// Sanity check: importing this file should not pull in any pg driver
// for the noop / fingerprint paths. (Compiled binary check.)
func TestFingerprintsAreHexLowercase(t *testing.T) {
	fp := bugFingerprint("http-out", "cdc_http_out", "auth.go", 42, "bug")
	if strings.ToLower(fp) != fp {
		t.Fatalf("fingerprint should be lowercase hex, got %q", fp)
	}
	for _, r := range fp {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			t.Fatalf("non-hex rune %q in fingerprint %q", r, fp)
		}
	}
}
