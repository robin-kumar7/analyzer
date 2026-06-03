package grounding

import (
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

func TestVerify_ExactMatch(t *testing.T) {
	iss := &issue.Issue{
		ResolvedRepo: "my-repo",
		CauseCode:    "items[index] // off by one",
		Confidence:   0.9,
		Fixes: []issue.Fix{
			{Before: "items[index] // off by one"},
		},
	}
	refs := []weaviate.Chunk{
		{Repo: "my-repo", Content: "func handler() {\n  items[index] // off by one\n  return nil\n}"},
	}

	ok := Verify(iss, refs, 6, 2)
	if !ok {
		t.Error("expected grounding to pass for exact match")
	}
	if !iss.GroundingOK {
		t.Error("GroundingOK should be true")
	}
}

func TestVerify_WhitespaceReformatted(t *testing.T) {
	iss := &issue.Issue{
		ResolvedRepo: "my-repo",
		CauseCode:    "items [ index ]  //  off  by  one",
		Confidence:   0.9,
	}
	refs := []weaviate.Chunk{
		{Repo: "my-repo", Content: "func handler() {\n  items[index] // off by one\n  return nil\n}"},
	}

	ok := Verify(iss, refs, 6, 2)
	if !ok {
		t.Error("expected grounding to pass for whitespace-reformatted match")
	}
}

func TestVerify_InventedSnippet(t *testing.T) {
	iss := &issue.Issue{
		ResolvedRepo: "my-repo",
		CauseCode:    "completely fabricated code that does not exist anywhere",
		Confidence:   0.9,
	}
	refs := []weaviate.Chunk{
		{Repo: "my-repo", Content: "func handler() {\n  items[index] // off by one\n  return nil\n}"},
	}

	ok := Verify(iss, refs, 6, 2)
	if ok {
		t.Error("expected grounding to fail for invented snippet")
	}
	if iss.GroundingOK {
		t.Error("GroundingOK should be false")
	}
	if iss.Confidence > 0.4 {
		t.Errorf("confidence = %f, should be <= 0.4", iss.Confidence)
	}
	if len(iss.NeedsMore) == 0 {
		t.Error("needs_more_info should be populated")
	}
}

func TestVerify_StitchedAcrossChunks(t *testing.T) {
	iss := &issue.Issue{
		ResolvedRepo: "my-repo",
		CauseCode:    "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu",
		Confidence:   0.9,
	}
	refs := []weaviate.Chunk{
		{Repo: "my-repo", Content: "alpha beta gamma delta epsilon zeta"},
		{Repo: "my-repo", Content: "eta theta iota kappa lambda mu"},
	}

	// anchorLen=6, anchorMin=2: the snippet needs 2 non-overlapping windows
	// of 6 tokens from the SAME chunk. The first chunk has "alpha..zeta" (6 tokens),
	// the second has "eta..mu" (6 tokens). No single chunk has 2 windows.
	ok := Verify(iss, refs, 6, 2)
	if ok {
		t.Error("expected grounding to fail when stitching across chunks")
	}
}

func TestVerify_RepoPurityViolation(t *testing.T) {
	iss := &issue.Issue{
		ResolvedRepo: "repo-a",
		CauseCode:    "some code",
		Confidence:   0.9,
	}
	refs := []weaviate.Chunk{
		{Repo: "repo-a", Content: "some code here"},
		{Repo: "repo-b", Content: "other code"}, // wrong repo
	}

	ok := Verify(iss, refs, 6, 2)
	if ok {
		t.Error("expected grounding to fail on repo purity violation")
	}
	if iss.GroundingOK {
		t.Error("GroundingOK should be false")
	}
}
