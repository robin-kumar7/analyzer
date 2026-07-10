package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// fakeSearcher implements weaviate.Searcher for handler tests.
type fakeSearcher struct{}

func (f *fakeSearcher) Search(_ context.Context, _ string, _ []float32, _ string, _ int, _ float64) ([]weaviate.Chunk, error) {
	return []weaviate.Chunk{
		{
			Repo:      "test-repo",
			FilePath:  "main.go",
			Language:  "go",
			StartLine: 1,
			EndLine:   10,
			Content:   "func main() {\n  panic(\"nil pointer\")\n}",
			Score:     0.95,
		},
	}, nil
}

func (f *fakeSearcher) FetchDocs(_ context.Context, _, _ string, _ int) ([]weaviate.Chunk, error) {
	return nil, nil
}

// fakeGenerator implements ollama.Generator for handler tests.
type fakeGenerator struct{}

func (f *fakeGenerator) Generate(_ context.Context, _, _, _ string) (string, error) {
	iss := map[string]any{
		"title":      "nil pointer dereference",
		"severity":   "high",
		"category":   "bug",
		"problem":    "nil pointer dereference in main.go",
		"file":       "main.go",
		"line":       2,
		"cause_code": "panic(\"nil pointer\")",
		"root_cause": "The code dereferences a nil pointer.",
		"evidence":   []map[string]any{{"chunk_index": 1, "file": "main.go", "start_line": 1, "end_line": 10, "why": "contains the panic"}},
		"fixes":      []map[string]any{{"file": "main.go", "start_line": 2, "end_line": 2, "before": "panic(\"nil pointer\")", "after": "if ptr != nil { use(ptr) }", "language": "go", "rationale": "nil check"}},
		"suggestion": "add nil checks",
		"tests":      []string{"test nil input"},
		"confidence": 0.85,
	}
	b, _ := json.Marshal(iss)
	return string(b), nil
}

func testConfig() *config.Config {
	return &config.Config{
		Port:                 8081,
		OllamaURL:            "http://localhost:11434",
		OllamaModel:          "test",
		WeaviateURL:          "http://localhost:8080",
		WeaviateClass:        "RepoChunk",
		DefaultTopK:          8,
		HybridAlpha:          0.5,
		ResolveRepoMinScore:  0.0,
		ResolveRepoAmbiguity: 1.0,
		AnchorLen:            6,
		AnchorMin:            2,
		MaxBodyBytes:         1048576,
		RateLimitRPS:         100,
		RateLimitBurst:       200,
	}
}

func TestHealthz(t *testing.T) {
	cfg := testConfig()
	engine := New(cfg, &fakeSearcher{}, nil, &fakeGenerator{}, nil, nil, nil)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q, want ok", body["status"])
	}
}

func TestAnalyze_ValidRequest(t *testing.T) {
	cfg := testConfig()
	engine := New(cfg, &fakeSearcher{}, nil, &fakeGenerator{}, nil, nil, nil)

	body := `{"input": "panic: nil pointer dereference", "mode": "logs"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/analyze", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	// Verify X-Request-ID is set.
	if w.Header().Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID header")
	}
}

func TestAnalyze_MissingInput(t *testing.T) {
	cfg := testConfig()
	engine := New(cfg, &fakeSearcher{}, nil, &fakeGenerator{}, nil, nil, nil)

	body := `{"mode": "logs"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/analyze", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAnalyze_InvalidMode(t *testing.T) {
	cfg := testConfig()
	engine := New(cfg, &fakeSearcher{}, nil, &fakeGenerator{}, nil, nil, nil)

	body := `{"input": "test", "mode": "invalid"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/analyze", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAPIKeyMiddleware(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "secret-key"
	engine := New(cfg, &fakeSearcher{}, nil, &fakeGenerator{}, nil, nil, nil)

	// Without key.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("without key: status = %d, want 401", w.Code)
	}

	// With Bearer key.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("with key: status = %d, want 200", w.Code)
	}

	// With X-API-Key header.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-API-Key", "secret-key")
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("with X-API-Key: status = %d, want 200", w.Code)
	}
}
