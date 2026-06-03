// Package calibration implements the confidence linter (Plan B2).
package calibration

import (
	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

// Lint applies deterministic post-processing rules to prevent
// overconfident results. It never raises confidence.
func Lint(iss *issue.Issue) {
	if len(iss.Evidence) == 0 && iss.Confidence > 0.5 {
		iss.Confidence = 0.5
		iss.NeedsMore = append(iss.NeedsMore,
			"confidence capped: no evidence chunks cited")
	}
	if iss.File == "" && iss.Line == 0 && iss.Confidence > 0.6 {
		iss.Confidence = 0.6
		iss.NeedsMore = append(iss.NeedsMore,
			"confidence capped: no file or line identified")
	}
}
