package ollama

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGenerate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/generate" {
			t.Errorf("path = %s, want /api/generate", r.URL.Path)
		}

		var req generateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Format != "json" {
			t.Errorf("format = %q, want json", req.Format)
		}
		if req.Stream {
			t.Error("stream should be false")
		}
		if req.Model != "test-model" {
			t.Errorf("model = %q, want test-model", req.Model)
		}
		temp, ok := req.Options["temperature"]
		if !ok {
			t.Fatal("temperature not set")
		}
		if temp.(float64) != 0.1 {
			t.Errorf("temperature = %v, want 0.1", temp)
		}

		resp := generateResponse{Response: `{"title":"test issue"}`}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	got, err := c.Generate(context.Background(), "test-model", "system prompt", "user prompt")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if got != `{"title":"test issue"}` {
		t.Errorf("response = %q, want {\"title\":\"test issue\"}", got)
	}
}

func TestGenerate_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("model not found"))
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	_, err := c.Generate(context.Background(), "bad-model", "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
