package calibration

import (
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

func TestLint(t *testing.T) {
	tests := []struct {
		name           string
		issue          issue.Issue
		wantConfidence float64
		wantNeedsMore  int
	}{
		{
			name: "high confidence + empty evidence",
			issue: issue.Issue{
				Confidence: 0.9,
				Evidence:   nil,
				File:       "main.go",
				Line:       42,
			},
			wantConfidence: 0.5,
			wantNeedsMore:  1,
		},
		{
			name: "high confidence + no file",
			issue: issue.Issue{
				Confidence: 0.8,
				Evidence:   []issue.Evidence{{ChunkIndex: 1}},
				File:       "",
				Line:       0,
			},
			wantConfidence: 0.6,
			wantNeedsMore:  1,
		},
		{
			name: "high confidence + empty evidence + no file",
			issue: issue.Issue{
				Confidence: 0.95,
				Evidence:   nil,
				File:       "",
				Line:       0,
			},
			wantConfidence: 0.5, // capped to 0.5 first, then 0.5 <= 0.6 so no second cap
			wantNeedsMore:  1,
		},
		{
			name: "low confidence unchanged",
			issue: issue.Issue{
				Confidence: 0.3,
				Evidence:   nil,
				File:       "",
				Line:       0,
			},
			wantConfidence: 0.3,
			wantNeedsMore:  0,
		},
		{
			name: "good issue unchanged",
			issue: issue.Issue{
				Confidence: 0.85,
				Evidence:   []issue.Evidence{{ChunkIndex: 1}},
				File:       "main.go",
				Line:       42,
			},
			wantConfidence: 0.85,
			wantNeedsMore:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iss := tt.issue
			Lint(&iss)
			if iss.Confidence != tt.wantConfidence {
				t.Errorf("confidence = %f, want %f", iss.Confidence, tt.wantConfidence)
			}
			if len(iss.NeedsMore) != tt.wantNeedsMore {
				t.Errorf("needs_more_info len = %d, want %d", len(iss.NeedsMore), tt.wantNeedsMore)
			}
		})
	}
}
