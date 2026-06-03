package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Unset all env vars that might interfere.
	for _, k := range []string{"PORT", "OLLAMA_URL", "WEAVIATE_URL", "DEFAULT_TOP_K", "HYBRID_ALPHA", "ANCHOR_LEN", "ANCHOR_MIN"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() with defaults: %v", err)
	}
	if c.Port != 8081 {
		t.Errorf("Port = %d, want 8081", c.Port)
	}
	if c.DefaultTopK != 8 {
		t.Errorf("DefaultTopK = %d, want 8", c.DefaultTopK)
	}
	if c.HybridAlpha != 0.65 {
		t.Errorf("HybridAlpha = %f, want 0.65", c.HybridAlpha)
	}
	if c.AnchorLen != 6 {
		t.Errorf("AnchorLen = %d, want 6", c.AnchorLen)
	}
	if c.AnchorMin != 2 {
		t.Errorf("AnchorMin = %d, want 2", c.AnchorMin)
	}
	if c.WeaviateClass != "RepoChunk" {
		t.Errorf("WeaviateClass = %q, want RepoChunk", c.WeaviateClass)
	}
}

func TestLoad_Validation(t *testing.T) {
	tests := []struct {
		name   string
		envs   map[string]string
		errSub string
	}{
		{
			name:   "bad PORT",
			envs:   map[string]string{"PORT": "99999"},
			errSub: "PORT must be in",
		},
		{
			name:   "bad HYBRID_ALPHA high",
			envs:   map[string]string{"HYBRID_ALPHA": "1.5"},
			errSub: "HYBRID_ALPHA must be in",
		},
		{
			name:   "bad HYBRID_ALPHA low",
			envs:   map[string]string{"HYBRID_ALPHA": "-0.1"},
			errSub: "HYBRID_ALPHA must be in",
		},
		{
			name:   "ANCHOR_LEN less than ANCHOR_MIN",
			envs:   map[string]string{"ANCHOR_LEN": "1", "ANCHOR_MIN": "3"},
			errSub: "ANCHOR_LEN",
		},
		{
			name:   "ANCHOR_MIN zero",
			envs:   map[string]string{"ANCHOR_MIN": "0"},
			errSub: "ANCHOR_MIN must be >= 1",
		},
		{
			name:   "DEFAULT_TOP_K zero",
			envs:   map[string]string{"DEFAULT_TOP_K": "0"},
			errSub: "DEFAULT_TOP_K must be >= 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear everything first.
			for _, k := range []string{"PORT", "HYBRID_ALPHA", "ANCHOR_LEN", "ANCHOR_MIN", "DEFAULT_TOP_K"} {
				os.Unsetenv(k)
			}
			for k, v := range tt.envs {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !containsSubstring(err.Error(), tt.errSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.errSub)
			}
		})
	}
}

func containsSubstring(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && containsStr(s, sub)
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
