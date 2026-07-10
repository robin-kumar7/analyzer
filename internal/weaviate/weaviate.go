// Package weaviate provides a read-only GraphQL client for searching
// RepoChunk objects. The analyzer never writes to Weaviate; only the
// repo-indexer does.
package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Chunk is a single code chunk returned from a Weaviate search.
type Chunk struct {
	Repo      string
	FilePath  string
	Language  string
	StartLine int
	EndLine   int
	Content   string
	Score     float64 // normalized [0,1], higher is better (FR-A4b)
}

// Searcher abstracts Weaviate search for testability.
type Searcher interface {
	// Search runs a hybrid (BM25 + dense vector) query against the
	// configured class. queryVector MUST be supplied because the repo-indexer
	// creates RepoChunk with vectorizer:none — Weaviate cannot embed
	// queries server-side. queryText is still used for the BM25 portion.
	Search(ctx context.Context, queryText string, queryVector []float32, repo string, limit int, alpha float64) ([]Chunk, error)
	// FetchDocs returns up to limit chunks for repo whose file_path starts
	// with pathPrefix. Used to prime per-repo service summaries from the
	// Hugo `site/` folder (see §7a of the design doc). Ordering is by
	// file_path ascending so summaries are deterministic across calls.
	FetchDocs(ctx context.Context, repo, pathPrefix string, limit int) ([]Chunk, error)
}

// Client implements Searcher against a live Weaviate instance.
type Client struct {
	BaseURL string
	Class   string
	HTTP    *http.Client
}

// New returns a Client with the given timeout.
func New(baseURL, class string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Class:   class,
		HTTP:    &http.Client{Timeout: timeout},
	}
}

// Search performs a hybrid search against Weaviate. queryVector is required
// because the RepoChunk class is configured with vectorizer:none. When repo
// is non-empty, a where clause restricts results to that repo (FR-A5).
// Results are returned sorted by Score descending.
func (c *Client) Search(ctx context.Context, queryText string, queryVector []float32, repo string, limit int, alpha float64) ([]Chunk, error) {
	gql := c.buildQuery(queryText, queryVector, repo, limit, alpha)

	body, err := json.Marshal(map[string]string{"query": gql})
	if err != nil {
		return nil, fmt.Errorf("marshal graphql: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphql POST: status %d: %s", resp.StatusCode, string(b))
	}

	return c.parseResponse(resp.Body)
}

func (c *Client) buildQuery(query string, vector []float32, repo string, limit int, alpha float64) string {
	escapedQuery := escapeGraphQLString(query)

	var where string
	if repo != "" {
		where = fmt.Sprintf(`where: { path: ["repo"], operator: Equal, valueText: "%s" }`, repo)
	}

	// Build the hybrid clause. vector is required (vectorizer:none on the
	// class); query is included so the BM25 (keyword) portion still works.
	var hybridParts []string
	hybridParts = append(hybridParts, fmt.Sprintf(`query: "%s"`, escapedQuery))
	hybridParts = append(hybridParts, fmt.Sprintf(`alpha: %f`, alpha))
	if len(vector) > 0 {
		hybridParts = append(hybridParts, fmt.Sprintf(`vector: %s`, formatVector(vector)))
	}

	var clauses []string
	clauses = append(clauses, fmt.Sprintf(`hybrid: { %s }`, strings.Join(hybridParts, ", ")))
	if where != "" {
		clauses = append(clauses, where)
	}
	clauses = append(clauses, fmt.Sprintf("limit: %d", limit))

	return fmt.Sprintf(`{
  Get {
    %s(%s) {
      content
      file_path
      repo
      language
      start_line
      end_line
      _additional {
        score
        distance
      }
    }
  }
}`, c.Class, strings.Join(clauses, ", "))
}

// graphqlResponse models the Weaviate GraphQL response envelope.
type graphqlResponse struct {
	Data   map[string]map[string][]graphqlObject `json:"data"`
	Errors []struct{ Message string }            `json:"errors"`
}

type graphqlObject struct {
	Content    string `json:"content"`
	FilePath   string `json:"file_path"`
	Repo       string `json:"repo"`
	Language   string `json:"language"`
	StartLine  int    `json:"start_line"`
	EndLine    int    `json:"end_line"`
	Additional struct {
		// Weaviate serializes score/distance as JSON strings in some versions
		// and as numbers in others. flexFloat accepts both.
		Score    flexFloat `json:"score"`
		Distance flexFloat `json:"distance"`
	} `json:"_additional"`
}

// flexFloat decodes a JSON number OR a JSON string containing a number,
// OR null. Use IsSet() to distinguish "missing" from "zero".
type flexFloat struct {
	Value float64
	Set   bool
}

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		return nil
	}
	// Strip surrounding quotes if present (Weaviate string form).
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
		if s == "" {
			return nil
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("flexFloat: %w", err)
	}
	f.Value = v
	f.Set = true
	return nil
}

// Ptr returns a *float64 view: nil when no value was present, &Value otherwise.
func (f flexFloat) Ptr() *float64 {
	if !f.Set {
		return nil
	}
	v := f.Value
	return &v
}

func (c *Client) parseResponse(r io.Reader) ([]Chunk, error) {
	var resp graphqlResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", resp.Errors[0].Message)
	}

	getClass, ok := resp.Data["Get"]
	if !ok {
		return nil, nil
	}
	objects, ok := getClass[c.Class]
	if !ok {
		return nil, nil
	}

	chunks := make([]Chunk, 0, len(objects))
	for _, obj := range objects {
		ch := Chunk{
			Repo:      obj.Repo,
			FilePath:  obj.FilePath,
			Language:  obj.Language,
			StartLine: obj.StartLine,
			EndLine:   obj.EndLine,
			Content:   obj.Content,
			Score:     normalizeScore(obj.Additional.Score.Ptr(), obj.Additional.Distance.Ptr()),
		}
		chunks = append(chunks, ch)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Score > chunks[j].Score
	})
	return chunks, nil
}

// normalizeScore returns a score in [0,1] where higher is better (FR-A4b).
func normalizeScore(score *float64, distance *float64) float64 {
	if score != nil {
		return *score
	}
	if distance != nil {
		return 1.0 / (1.0 + *distance)
	}
	return 0
}

// escapeGraphQLString escapes a string for embedding inside a GraphQL string
// literal (per the GraphQL spec). Backslash MUST be escaped first; otherwise
// any pre-existing `\X` in user input becomes an invalid escape sequence.
func escapeGraphQLString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

// formatVector renders a []float32 as a GraphQL array literal: [0.123, -0.456, ...].
func formatVector(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		// %g gives compact representation; %.7g keeps precision modest.
		fmt.Fprintf(&b, "%.7g", x)
	}
	b.WriteByte(']')
	return b.String()
}

// Ping checks basic connectivity to the Weaviate server.
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/meta", nil)
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

// FetchDocs returns up to limit doc chunks for the given repo whose
// file_path begins with pathPrefix. Results are sorted by file_path
// ascending so summaries are stable across calls.
func (c *Client) FetchDocs(ctx context.Context, repo, pathPrefix string, limit int) ([]Chunk, error) {
	if repo == "" {
		return nil, fmt.Errorf("FetchDocs: repo is required")
	}
	if pathPrefix == "" {
		pathPrefix = "site/"
	}
	if limit <= 0 {
		limit = 30
	}

	gql := c.buildDocsQuery(repo, pathPrefix, limit)
	body, err := json.Marshal(map[string]string{"query": gql})
	if err != nil {
		return nil, fmt.Errorf("marshal graphql: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("graphql POST: status %d: %s", resp.StatusCode, string(b))
	}

	chunks, err := c.parseResponse(resp.Body)
	if err != nil {
		return nil, err
	}

	// Sort by file_path ascending for deterministic summarization input.
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].FilePath == chunks[j].FilePath {
			return chunks[i].StartLine < chunks[j].StartLine
		}
		return chunks[i].FilePath < chunks[j].FilePath
	})
	return chunks, nil
}

func (c *Client) buildDocsQuery(repo, pathPrefix string, limit int) string {
	// AND filter: repo == repo  AND  file_path LIKE pathPrefix*
	// Weaviate's Like operator uses "*" as the wildcard.
	escRepo := strings.ReplaceAll(repo, `"`, `\"`)
	escPrefix := strings.ReplaceAll(pathPrefix, `"`, `\"`) + "*"

	where := fmt.Sprintf(`where: {
      operator: And,
      operands: [
        { path: ["repo"],      operator: Equal, valueText: "%s" },
        { path: ["file_path"], operator: Like,  valueText: "%s" }
      ]
    }`, escRepo, escPrefix)

	return fmt.Sprintf(`{
  Get {
    %s(%s, limit: %d) {
      content
      file_path
      repo
      language
      start_line
      end_line
      _additional {
        score
        distance
      }
    }
  }
}`, c.Class, where, limit)
}
