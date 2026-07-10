// Package issue defines the Issue response schema (FR-A7 / requirements §3.4).
package issue

// Issue is the structured analysis result returned by POST /analyze.
// Fields match 01-requirements.md §3.4 verbatim.
type Issue struct {
	Title    string `json:"title"`
	Severity string `json:"severity"` // low|medium|high|critical
	Category string `json:"category"` // bug|config|dependency|infra|data|unknown
	Problem  string `json:"problem"`
	// ServiceName is the originating log-shipper service name (e.g. "http-out").
	// Set from the Kafka header service-name on ingest; used by notifier for routing.
	ServiceName  string      `json:"service_name,omitempty"`
	ResolvedRepo string      `json:"resolved_repo"`
	File         string      `json:"file"`
	Line         int         `json:"line"`
	CauseCode    string      `json:"cause_code"`
	RootCause    string      `json:"root_cause"`
	GroundingOK  bool        `json:"grounding_ok"`
	Evidence     []Evidence  `json:"evidence"`
	Fixes        []Fix       `json:"fixes"`
	Suggestion   string      `json:"suggestion"`
	Tests        []string    `json:"tests"`
	Alternatives []string    `json:"alternatives"`
	NeedsMore    []string    `json:"needs_more_info"`
	Confidence   float64     `json:"confidence"`
	References   []Reference `json:"references"`
}

// Evidence links a retrieved chunk to the analysis.
type Evidence struct {
	ChunkIndex int    `json:"chunk_index"`
	File       string `json:"file"`
	StartLine  int    `json:"start_line"`
	EndLine    int    `json:"end_line"`
	Why        string `json:"why"`
}

// Fix is a before→after code patch.
type Fix struct {
	File      string `json:"file"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	Before    string `json:"before"`
	After     string `json:"after"`
	Language  string `json:"language"`
	Rationale string `json:"rationale"`
}

// Reference is a chunk the model was shown during analysis.
type Reference struct {
	Repo      string  `json:"repo"`
	FilePath  string  `json:"file_path"`
	StartLine int     `json:"start_line"`
	EndLine   int     `json:"end_line"`
	Content   string  `json:"content"`
	Language  string  `json:"language"`
	Score     float64 `json:"score"`
}
