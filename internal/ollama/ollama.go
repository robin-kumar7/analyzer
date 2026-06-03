// Package ollama wraps the Ollama /api/generate endpoint.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Generator abstracts LLM text generation for testability.
type Generator interface {
	Generate(ctx context.Context, model, system, prompt string) (string, error)
}

// Embedder abstracts embedding generation for testability. The analyzer
// embeds the search query locally before calling Weaviate because the
// RepoChunk class is created with vectorizer:none (the feeder uploads
// pre-computed vectors).
type Embedder interface {
	Embed(ctx context.Context, model, text string) ([]float32, error)
}

// Client implements Generator against a live Ollama instance.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New returns a Client with the given timeout.
func New(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: timeout},
	}
}

type generateRequest struct {
	Model   string         `json:"model"`
	System  string         `json:"system"`
	Prompt  string         `json:"prompt"`
	Format  string         `json:"format"`
	Stream  bool           `json:"stream"`
	Think   *bool          `json:"think,omitempty"`
	Options map[string]any `json:"options"`
}

type generateResponse struct {
	Response string `json:"response"`
}

// Generate calls Ollama /api/generate with format=json, low temperature,
// and streaming disabled. Returns the raw response string (expected to be JSON).
func (c *Client) Generate(ctx context.Context, model, system, prompt string) (string, error) {
	reqBody := generateRequest{
		Model:  model,
		System: system,
		Prompt: prompt,
		Format: "json",
		Stream: false,
		// think:false disables <think> blocks on reasoning models (qwen3, deepseek-r1).
		// Older Ollama servers ignore the field, which is fine.
		Think: new(bool),
		Options: map[string]any{
			"temperature": 0.1,
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal generate request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama generate: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama generate: status %d: %s", resp.StatusCode, string(b))
	}

	var genResp generateResponse
	if err := json.NewDecoder(resp.Body).Decode(&genResp); err != nil {
		return "", fmt.Errorf("decode generate response: %w", err)
	}
	return genResp.Response, nil
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// Embed returns a single embedding for text using model (e.g. "qwen3-embedding").
// Must match the embedding model the feeder used; otherwise vector spaces
// are incompatible and search quality collapses.
func (c *Client) Embed(ctx context.Context, model, text string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Model: model, Input: []string{text}})
	if err != nil {
		return nil, fmt.Errorf("marshal embed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama embed: status %d: %s", resp.StatusCode, string(b))
	}

	var er embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}
	if len(er.Embeddings) == 0 || len(er.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("ollama embed: empty embedding returned")
	}
	return er.Embeddings[0], nil
}

// Ping checks basic connectivity to the Ollama server.
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("ping new request: %w", err)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping: status %d", resp.StatusCode)
	}
	return nil
}
