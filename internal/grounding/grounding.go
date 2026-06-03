// Package grounding verifies that model-generated snippets actually
// appear in the retrieved chunks (NFR-3 / Plan B1).
package grounding

import (
	"log/slog"
	"strings"
	"unicode"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// Verify checks that cause_code and every fixes[].before are grounded
// in the retrieved chunks using anchor-token matching. It also asserts
// repo purity. Modifies the issue in-place on failure.
func Verify(iss *issue.Issue, refs []weaviate.Chunk, anchorLen, anchorMin int) bool {
	if anchorLen < 1 {
		anchorLen = 6
	}
	if anchorMin < 1 {
		anchorMin = 2
	}

	allOK := true

	// Assert repo purity: all refs must belong to the same repo.
	if !checkRepoPurity(iss, refs) {
		allOK = false
	}

	// Tokenize all reference contents once.
	refTokens := make([][]string, len(refs))
	for i, ref := range refs {
		refTokens[i] = tokenize(ref.Content)
	}

	// Check cause_code.
	if iss.CauseCode != "" {
		if !checkField(iss.CauseCode, refTokens, anchorLen, anchorMin) {
			iss.NeedsMore = append(iss.NeedsMore, "cause_code not found verbatim in any reference")
			allOK = false
		}
	}

	// Check each fix.before.
	for i, fix := range iss.Fixes {
		if fix.Before == "" {
			continue
		}
		if !checkField(fix.Before, refTokens, anchorLen, anchorMin) {
			iss.NeedsMore = append(iss.NeedsMore,
				"fixes["+itoa(i)+"].before not found verbatim in any reference")
			allOK = false
		}
	}

	if !allOK {
		iss.GroundingOK = false
		iss.Confidence *= 0.5
		if iss.Confidence > 0.4 {
			iss.Confidence = 0.4
		}
	} else {
		iss.GroundingOK = true
	}

	return allOK
}

// checkRepoPurity asserts all refs belong to the same repo as resolved_repo.
func checkRepoPurity(iss *issue.Issue, refs []weaviate.Chunk) bool {
	if len(refs) == 0 || iss.ResolvedRepo == "" {
		return true
	}
	for _, ref := range refs {
		if ref.Repo != iss.ResolvedRepo {
			slog.Error("repo purity violation",
				"expected", iss.ResolvedRepo,
				"got", ref.Repo,
				"file", ref.FilePath)
			iss.NeedsMore = append(iss.NeedsMore,
				"internal error: mixed-repo references detected")
			return false
		}
	}
	return true
}

// checkField verifies that the snippet has at least anchorMin non-overlapping
// anchor windows (of anchorLen tokens) matching the SAME chunk.
func checkField(snippet string, refTokens [][]string, anchorLen, anchorMin int) bool {
	snippetTokens := tokenize(snippet)
	if len(snippetTokens) < anchorLen {
		// Snippet too short for windowed check; fall back to substring match.
		joined := strings.Join(snippetTokens, " ")
		for _, rt := range refTokens {
			if strings.Contains(strings.Join(rt, " "), joined) {
				return true
			}
		}
		return false
	}

	// Build windows from the snippet.
	windows := make([][]string, 0, len(snippetTokens)-anchorLen+1)
	for i := 0; i <= len(snippetTokens)-anchorLen; i++ {
		windows = append(windows, snippetTokens[i:i+anchorLen])
	}

	// For each reference chunk, count non-overlapping window hits.
	for _, rt := range refTokens {
		if len(rt) < anchorLen {
			continue
		}
		hits := countNonOverlappingHits(windows, rt, anchorLen)
		if hits >= anchorMin {
			return true
		}
	}
	return false
}

// countNonOverlappingHits counts how many snippet windows match non-overlapping
// positions in the reference tokens.
func countNonOverlappingHits(windows [][]string, refTokens []string, anchorLen int) int {
	hits := 0
	usedRef := make([]bool, len(refTokens))
	usedWin := make([]bool, len(windows))

	for wi, win := range windows {
		if usedWin[wi] {
			continue
		}
		for ri := 0; ri <= len(refTokens)-anchorLen; ri++ {
			if usedRef[ri] {
				continue
			}
			if tokensEqual(win, refTokens[ri:ri+anchorLen]) {
				hits++
				// Mark as used (non-overlapping).
				usedWin[wi] = true
				for k := ri; k < ri+anchorLen && k < len(usedRef); k++ {
					usedRef[k] = true
				}
				break
			}
		}
	}
	return hits
}

func tokensEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// tokenize splits text into lowercase identifier tokens, collapsing whitespace
// and stripping non-identifier characters.
func tokenize(text string) []string {
	var tokens []string
	var current strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			current.WriteRune(unicode.ToLower(r))
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
