package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSearch_WithRepoFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		q := body["query"]
		if !containsStr(q, `operator: Equal`) {
			t.Errorf("expected where clause with operator: Equal, got:\n%s", q)
		}
		if !containsStr(q, `valueText: "my-repo"`) {
			t.Errorf("expected valueText: my-repo, got:\n%s", q)
		}
		writeGraphQLResponse(w, t)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	chunks, err := c.Search(context.Background(), "panic error", []float32{0.1, 0.2, 0.3}, "my-repo", 5, 0.5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
	if chunks[0].Score < chunks[1].Score {
		t.Error("chunks not sorted by score desc")
	}
}

func TestSearch_WithoutRepoFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		q := body["query"]
		if containsStr(q, "where:") {
			t.Errorf("expected no where clause, got:\n%s", q)
		}
		writeGraphQLResponse(w, t)
	}))
	defer srv.Close()

	c := New(srv.URL, "RepoChunk", 0)
	chunks, err := c.Search(context.Background(), "panic error", []float32{0.1, 0.2, 0.3}, "", 5, 0.5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
}

func TestNormalizeScore(t *testing.T) {
	tests := []struct {
		name     string
		score    *float64
		distance *float64
		want     float64
	}{
		{"score present", floatPtr(0.85), nil, 0.85},
		{"distance present", nil, floatPtr(1.0), 0.5},
		{"both present prefers score", floatPtr(0.9), floatPtr(1.0), 0.9},
		{"neither", nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeScore(tt.score, tt.distance)
			if got != tt.want {
				t.Errorf("normalizeScore = %f, want %f", got, tt.want)
			}
		})
	}
}

func writeGraphQLResponse(w http.ResponseWriter, t *testing.T) {
	t.Helper()
	resp := map[string]any{
		"data": map[string]any{
			"Get": map[string]any{
				"RepoChunk": []map[string]any{
					{
						"content":     "func main() {}",
						"file_path":   "cmd/main.go",
						"repo":        "my-repo",
						"language":    "go",
						"start_line":  1,
						"end_line":    10,
						"_additional": map[string]any{"score": 0.9, "distance": nil},
					},
					{
						"content":     "func helper() {}",
						"file_path":   "internal/helper.go",
						"repo":        "my-repo",
						"language":    "go",
						"start_line":  1,
						"end_line":    5,
						"_additional": map[string]any{"score": 0.7, "distance": nil},
					},
				},
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func floatPtr(f float64) *float64 { return &f }

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
