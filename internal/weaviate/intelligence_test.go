package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLookupSymbols(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"Get": map[string]any{
					"Symbol": []map[string]any{
						{
							"repo":     "my-repo",
							"symbol":   "DeleteDNS",
							"type":     "function",
							"receiver": "",
							"package":  "dns",
							"filepath": "internal/dns/handler.go",
							"line":     float64(42),
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	syms, err := c.LookupSymbols(context.Background(), "my-repo", []string{"DeleteDNS"}, 10)
	if err != nil {
		t.Fatalf("LookupSymbols: %v", err)
	}
	if len(syms) != 1 {
		t.Fatalf("got %d symbols, want 1", len(syms))
	}
	if syms[0].Name != "DeleteDNS" {
		t.Errorf("Name = %q, want DeleteDNS", syms[0].Name)
	}
	if syms[0].Line != 42 {
		t.Errorf("Line = %d, want 42", syms[0].Line)
	}
	if syms[0].FilePath != "internal/dns/handler.go" {
		t.Errorf("FilePath = %q, want internal/dns/handler.go", syms[0].FilePath)
	}
}

func TestLookupSymbols_EmptyInput(t *testing.T) {
	c := New("http://unused", "RepoChunk", 0)
	syms, err := c.LookupSymbols(context.Background(), "repo", nil, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(syms) != 0 {
		t.Errorf("expected 0 symbols for empty input, got %d", len(syms))
	}
}

func TestSearchFunctions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"Get": map[string]any{
					"Function": []map[string]any{
						{
							"repo":         "my-repo",
							"functionName": "DeleteDNS",
							"receiver":     "Handler",
							"packageName":  "dns",
							"filepath":     "internal/dns/handler.go",
							"signature":    "func (h *Handler) DeleteDNS(ctx context.Context, id string) error",
							"imports":      []any{"context", "fmt"},
							"returns":      []any{"error"},
							"summary":      "Deletes a DNS record.",
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	fns, err := c.SearchFunctions(context.Background(), "my-repo", []string{"DeleteDNS"}, 10)
	if err != nil {
		t.Fatalf("SearchFunctions: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("got %d functions, want 1", len(fns))
	}
	if fns[0].FunctionName != "DeleteDNS" {
		t.Errorf("FunctionName = %q, want DeleteDNS", fns[0].FunctionName)
	}
	if fns[0].Receiver != "Handler" {
		t.Errorf("Receiver = %q, want Handler", fns[0].Receiver)
	}
	if len(fns[0].Imports) != 2 {
		t.Errorf("Imports = %v, want 2 items", fns[0].Imports)
	}
}

func TestGetCallGraph(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var edges []map[string]any
		if callCount == 1 {
			edges = []map[string]any{
				{
					"repo":       "my-repo",
					"caller":     "DeleteDNS",
					"callee":     "DeleteRecord",
					"callerFile": "internal/dns/handler.go",
					"calleeFile": "internal/infoblox/client.go",
				},
			}
		}
		resp := map[string]any{
			"data": map[string]any{
				"Get": map[string]any{
					"CallEdge": edges,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	edges, err := c.GetCallGraph(context.Background(), "my-repo", []string{"DeleteDNS"}, 2, 20)
	if err != nil {
		t.Fatalf("GetCallGraph: %v", err)
	}
	if len(edges) < 1 {
		t.Fatalf("got %d edges, want >= 1", len(edges))
	}
	if edges[0].Caller != "DeleteDNS" || edges[0].Callee != "DeleteRecord" {
		t.Errorf("edge = %s→%s, want DeleteDNS→DeleteRecord", edges[0].Caller, edges[0].Callee)
	}
}

func TestGetFileSummaries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"Get": map[string]any{
					"FileSummary": []map[string]any{
						{
							"repo":          "my-repo",
							"filepath":      "internal/dns/handler.go",
							"package":       "dns",
							"summary":       "Handles DNS CRUD operations.",
							"purpose":       "DNS record CRUD",
							"functions":     []string{"CreateRecord", "DeleteRecord"},
							"externalCalls": []string{"store.Save"},
							"notes":         []string{"Assumes records are pre-validated."},
						},
						{
							"repo":     "my-repo",
							"filepath": "cmd/main.go",
							"package":  "main",
							"summary":  "Entry point.",
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	summaries, err := c.GetFileSummaries(context.Background(), "my-repo", 30)
	if err != nil {
		t.Fatalf("GetFileSummaries: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("got %d summaries, want 2", len(summaries))
	}
	// Should be sorted by filepath ascending.
	if summaries[0].FilePath != "cmd/main.go" {
		t.Errorf("first summary filepath = %q, want cmd/main.go (sorted)", summaries[0].FilePath)
	}

	dnsHandler := summaries[1]
	if dnsHandler.FilePath != "internal/dns/handler.go" {
		t.Fatalf("second summary filepath = %q, want internal/dns/handler.go", dnsHandler.FilePath)
	}
	if dnsHandler.Purpose != "DNS record CRUD" {
		t.Errorf("Purpose = %q", dnsHandler.Purpose)
	}
	if len(dnsHandler.Functions) != 2 || dnsHandler.Functions[0] != "CreateRecord" {
		t.Errorf("Functions = %v", dnsHandler.Functions)
	}
	if len(dnsHandler.ExternalCalls) != 1 || dnsHandler.ExternalCalls[0] != "store.Save" {
		t.Errorf("ExternalCalls = %v", dnsHandler.ExternalCalls)
	}
	if len(dnsHandler.Notes) != 1 || dnsHandler.Notes[0] != "Assumes records are pre-validated." {
		t.Errorf("Notes = %v", dnsHandler.Notes)
	}
	// The pre-structured-summary entry (cmd/main.go) must decode with
	// nil/zero structured fields rather than panicking or erroring.
	if summaries[0].Purpose != "" || summaries[0].Functions != nil {
		t.Errorf("expected zero-valued structured fields for entry without them, got Purpose=%q Functions=%v",
			summaries[0].Purpose, summaries[0].Functions)
	}
}

func TestGetRepoMap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"Get": map[string]any{
					"RepositoryMap": []map[string]any{
						{
							"repo":      "my-repo",
							"nodeType":  "handler",
							"nodeName":  "DNSHandler",
							"dependsOn": []any{"DNSService", "Logger"},
							"filepath":  "internal/dns/handler.go",
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	nodes, err := c.GetRepoMap(context.Background(), "my-repo", 20)
	if err != nil {
		t.Fatalf("GetRepoMap: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0].NodeName != "DNSHandler" {
		t.Errorf("NodeName = %q, want DNSHandler", nodes[0].NodeName)
	}
	if len(nodes[0].DependsOn) != 2 {
		t.Errorf("DependsOn = %v, want 2 items", nodes[0].DependsOn)
	}
}

func TestGetFileSummaries_EmptyRepo(t *testing.T) {
	c := New("http://unused", "RepoChunk", 0)
	summaries, err := c.GetFileSummaries(context.Background(), "", 30)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(summaries) != 0 {
		t.Errorf("expected 0 summaries for empty repo, got %d", len(summaries))
	}
}
