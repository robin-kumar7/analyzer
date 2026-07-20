// Package weaviate — intelligence.go adds read-only GraphQL queries for the
// repo-indexer v2 classes: Symbol, Function, CallEdge, FileSummary, RepositoryMap.
package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

// --- Types ---

// Symbol is a go-to-definition entry from the repo-indexer's AST extraction.
type Symbol struct {
	Repo     string `json:"repo"`
	Name     string `json:"symbol"`
	Type     string `json:"type"` // struct, interface, function, method, const, var
	Receiver string `json:"receiver"`
	Package  string `json:"package"`
	FilePath string `json:"filepath"`
	Line     int    `json:"line"`
}

// Function holds function-level metadata from the repo-indexer.
type Function struct {
	Repo         string   `json:"repo"`
	FunctionName string   `json:"functionName"`
	Receiver     string   `json:"receiver"`
	PackageName  string   `json:"packageName"`
	FilePath     string   `json:"filepath"`
	Signature    string   `json:"signature"`
	Imports      []string `json:"imports"`
	Returns      []string `json:"returns"`
	Summary      string   `json:"summary"`
}

// CallEdge is a caller→callee relationship from the repo-indexer's call graph.
type CallEdge struct {
	Repo       string `json:"repo"`
	Caller     string `json:"caller"`
	Callee     string `json:"callee"`
	CallerFile string `json:"callerFile"`
	CalleeFile string `json:"calleeFile"`
}

// FileSummary is an LLM-generated per-file summary from the repo-indexer.
// Summary is the original 1-3 sentence prose description; Purpose through
// Notes are a structured breakdown of the same file produced by the same
// repo-indexer LLM call (see vibecoder-repo-indexer/internal/summary's
// StructuredSummary) — repos indexed before that feature shipped will
// have these as zero values, so treat them as optional everywhere.
type FileSummary struct {
	Repo     string `json:"repo"`
	FilePath string `json:"filepath"`
	Package  string `json:"package"`
	Summary  string `json:"summary"`

	Purpose       string   `json:"purpose"`
	Structs       []string `json:"structs"`
	Interfaces    []string `json:"interfaces"`
	Functions     []string `json:"functions"`
	Imports       []string `json:"imports"`
	ExternalCalls []string `json:"externalCalls"`
	Reads         []string `json:"reads"`
	Writes        []string `json:"writes"`
	Notes         []string `json:"notes"`
}

// RepoMapNode is an architectural dependency node from the repo-indexer.
type RepoMapNode struct {
	Repo      string   `json:"repo"`
	NodeType  string   `json:"nodeType"`
	NodeName  string   `json:"nodeName"`
	DependsOn []string `json:"dependsOn"`
	FilePath  string   `json:"filepath"`
}

// --- Intelligence interface ---

// IntelligenceSearcher abstracts intelligence queries for testability.
type IntelligenceSearcher interface {
	LookupSymbols(ctx context.Context, repo string, symbols []string, limit int) ([]Symbol, error)
	SearchFunctions(ctx context.Context, repo string, names []string, limit int) ([]Function, error)
	GetCallGraph(ctx context.Context, repo string, functionNames []string, depth, maxEdges int) ([]CallEdge, error)
	GetFileSummaries(ctx context.Context, repo string, limit int) ([]FileSummary, error)
	GetRepoMap(ctx context.Context, repo string, limit int) ([]RepoMapNode, error)
}

// --- Client methods ---

// LookupSymbols queries the Symbol class for exact-match symbol resolution.
func (c *Client) LookupSymbols(ctx context.Context, repo string, symbols []string, limit int) ([]Symbol, error) {
	if len(symbols) == 0 || repo == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}

	var all []Symbol
	for _, sym := range symbols {
		gql := c.buildSymbolQuery(repo, sym, limit)
		results, err := c.execGraphQL(ctx, gql)
		if err != nil {
			return nil, fmt.Errorf("lookup symbol %q: %w", sym, err)
		}
		objs, ok := extractObjects(results, "Symbol")
		if !ok {
			continue
		}
		for _, obj := range objs {
			all = append(all, Symbol{
				Repo:     getString(obj, "repo"),
				Name:     getString(obj, "symbol"),
				Type:     getString(obj, "type"),
				Receiver: getString(obj, "receiver"),
				Package:  getString(obj, "package"),
				FilePath: getString(obj, "filepath"),
				Line:     getInt(obj, "line"),
			})
		}
	}
	return all, nil
}

func (c *Client) buildSymbolQuery(repo, symbol string, limit int) string {
	return fmt.Sprintf(`{
  Get {
    Symbol(where: {
      operator: And,
      operands: [
        { path: ["repo"],   operator: Equal, valueText: "%s" },
        { path: ["symbol"], operator: Equal, valueText: "%s" }
      ]
    }, limit: %d) {
      repo
      symbol
      type
      receiver
      package
      filepath
      line
    }
  }
}`, escapeGraphQLString(repo), escapeGraphQLString(symbol), limit)
}

// SearchFunctions queries the Function class by exact function name match.
func (c *Client) SearchFunctions(ctx context.Context, repo string, names []string, limit int) ([]Function, error) {
	if len(names) == 0 || repo == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}

	var all []Function
	for _, name := range names {
		gql := c.buildFunctionQuery(repo, name, limit)
		results, err := c.execGraphQL(ctx, gql)
		if err != nil {
			return nil, fmt.Errorf("search function %q: %w", name, err)
		}
		objs, ok := extractObjects(results, "Function")
		if !ok {
			continue
		}
		for _, obj := range objs {
			all = append(all, Function{
				Repo:         getString(obj, "repo"),
				FunctionName: getString(obj, "functionName"),
				Receiver:     getString(obj, "receiver"),
				PackageName:  getString(obj, "packageName"),
				FilePath:     getString(obj, "filepath"),
				Signature:    getString(obj, "signature"),
				Imports:      getStringSlice(obj, "imports"),
				Returns:      getStringSlice(obj, "returns"),
				Summary:      getString(obj, "summary"),
			})
		}
	}
	return all, nil
}

func (c *Client) buildFunctionQuery(repo, funcName string, limit int) string {
	return fmt.Sprintf(`{
  Get {
    Function(where: {
      operator: And,
      operands: [
        { path: ["repo"],         operator: Equal, valueText: "%s" },
        { path: ["functionName"], operator: Equal, valueText: "%s" }
      ]
    }, limit: %d) {
      repo
      functionName
      receiver
      packageName
      filepath
      signature
      imports
      returns
      summary
    }
  }
}`, escapeGraphQLString(repo), escapeGraphQLString(funcName), limit)
}

// GetCallGraph traverses CallEdge objects up to depth hops. Starts from
// the given function names and follows both caller and callee edges.
func (c *Client) GetCallGraph(ctx context.Context, repo string, functionNames []string, depth, maxEdges int) ([]CallEdge, error) {
	if len(functionNames) == 0 || repo == "" {
		return nil, nil
	}
	if depth <= 0 {
		depth = 2
	}
	if maxEdges <= 0 {
		maxEdges = 20
	}

	seen := map[string]bool{}
	var result []CallEdge
	frontier := make([]string, len(functionNames))
	copy(frontier, functionNames)

	for d := 0; d < depth && len(frontier) > 0 && len(result) < maxEdges; d++ {
		var nextFrontier []string
		for _, name := range frontier {
			edges, err := c.fetchEdges(ctx, repo, name, maxEdges-len(result))
			if err != nil {
				return nil, fmt.Errorf("call graph for %q: %w", name, err)
			}
			for _, e := range edges {
				key := e.Caller + "→" + e.Callee
				if seen[key] {
					continue
				}
				seen[key] = true
				result = append(result, e)
				if len(result) >= maxEdges {
					break
				}
				// Expand both directions.
				if !seen[e.Callee+"→"] {
					nextFrontier = append(nextFrontier, e.Callee)
					seen[e.Callee+"→"] = true
				}
			}
			if len(result) >= maxEdges {
				break
			}
		}
		frontier = nextFrontier
	}

	return result, nil
}

func (c *Client) fetchEdges(ctx context.Context, repo, name string, limit int) ([]CallEdge, error) {
	if limit <= 0 {
		limit = 20
	}
	// Query edges where caller OR callee matches.
	gql := fmt.Sprintf(`{
  Get {
    CallEdge(where: {
      operator: And,
      operands: [
        { path: ["repo"], operator: Equal, valueText: "%s" },
        {
          operator: Or,
          operands: [
            { path: ["caller"], operator: Equal, valueText: "%s" },
            { path: ["callee"], operator: Equal, valueText: "%s" }
          ]
        }
      ]
    }, limit: %d) {
      repo
      caller
      callee
      callerFile
      calleeFile
    }
  }
}`, escapeGraphQLString(repo), escapeGraphQLString(name), escapeGraphQLString(name), limit)

	results, err := c.execGraphQL(ctx, gql)
	if err != nil {
		return nil, err
	}
	objs, ok := extractObjects(results, "CallEdge")
	if !ok {
		return nil, nil
	}
	edges := make([]CallEdge, 0, len(objs))
	for _, obj := range objs {
		edges = append(edges, CallEdge{
			Repo:       getString(obj, "repo"),
			Caller:     getString(obj, "caller"),
			Callee:     getString(obj, "callee"),
			CallerFile: getString(obj, "callerFile"),
			CalleeFile: getString(obj, "calleeFile"),
		})
	}
	return edges, nil
}

// GetFileSummaries returns FileSummary objects for the given repo.
func (c *Client) GetFileSummaries(ctx context.Context, repo string, limit int) ([]FileSummary, error) {
	if repo == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 30
	}

	gql := fmt.Sprintf(`{
  Get {
    FileSummary(where: {
      path: ["repo"], operator: Equal, valueText: "%s"
    }, limit: %d) {
      repo
      filepath
      package
      summary
      purpose
      structs
      interfaces
      functions
      imports
      externalCalls
      reads
      writes
      notes
    }
  }
}`, escapeGraphQLString(repo), limit)

	results, err := c.execGraphQL(ctx, gql)
	if err != nil {
		return nil, fmt.Errorf("get file summaries: %w", err)
	}
	objs, ok := extractObjects(results, "FileSummary")
	if !ok {
		return nil, nil
	}
	summaries := make([]FileSummary, 0, len(objs))
	for _, obj := range objs {
		summaries = append(summaries, FileSummary{
			Repo:          getString(obj, "repo"),
			FilePath:      getString(obj, "filepath"),
			Package:       getString(obj, "package"),
			Summary:       getString(obj, "summary"),
			Purpose:       getString(obj, "purpose"),
			Structs:       getStringSlice(obj, "structs"),
			Interfaces:    getStringSlice(obj, "interfaces"),
			Functions:     getStringSlice(obj, "functions"),
			Imports:       getStringSlice(obj, "imports"),
			ExternalCalls: getStringSlice(obj, "externalCalls"),
			Reads:         getStringSlice(obj, "reads"),
			Writes:        getStringSlice(obj, "writes"),
			Notes:         getStringSlice(obj, "notes"),
		})
	}
	// Sort by filepath ascending for deterministic output.
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].FilePath < summaries[j].FilePath
	})
	return summaries, nil
}

// GetRepoMap returns RepositoryMap nodes for the given repo.
func (c *Client) GetRepoMap(ctx context.Context, repo string, limit int) ([]RepoMapNode, error) {
	if repo == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}

	gql := fmt.Sprintf(`{
  Get {
    RepositoryMap(where: {
      path: ["repo"], operator: Equal, valueText: "%s"
    }, limit: %d) {
      repo
      nodeType
      nodeName
      dependsOn
      filepath
    }
  }
}`, escapeGraphQLString(repo), limit)

	results, err := c.execGraphQL(ctx, gql)
	if err != nil {
		return nil, fmt.Errorf("get repo map: %w", err)
	}
	objs, ok := extractObjects(results, "RepositoryMap")
	if !ok {
		return nil, nil
	}
	nodes := make([]RepoMapNode, 0, len(objs))
	for _, obj := range objs {
		nodes = append(nodes, RepoMapNode{
			Repo:      getString(obj, "repo"),
			NodeType:  getString(obj, "nodeType"),
			NodeName:  getString(obj, "nodeName"),
			DependsOn: getStringSlice(obj, "dependsOn"),
			FilePath:  getString(obj, "filepath"),
		})
	}
	return nodes, nil
}

// --- Helpers ---

// execGraphQL sends a GraphQL query and returns the raw response data.
func (c *Client) execGraphQL(ctx context.Context, gql string) (map[string]json.RawMessage, error) {
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

	var raw struct {
		Data   map[string]json.RawMessage `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w", err)
	}
	if len(raw.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", raw.Errors[0].Message)
	}
	return raw.Data, nil
}

// extractObjects pulls the object array for a given class from the GraphQL
// Get response.
func extractObjects(data map[string]json.RawMessage, className string) ([]map[string]any, bool) {
	getRaw, ok := data["Get"]
	if !ok {
		return nil, false
	}
	var getMap map[string]json.RawMessage
	if err := json.Unmarshal(getRaw, &getMap); err != nil {
		return nil, false
	}
	classRaw, ok := getMap[className]
	if !ok {
		return nil, false
	}
	var objs []map[string]any
	if err := json.Unmarshal(classRaw, &objs); err != nil {
		return nil, false
	}
	return objs, len(objs) > 0
}

func getString(obj map[string]any, key string) string {
	v, _ := obj[key].(string)
	return v
}

func getInt(obj map[string]any, key string) int {
	switch v := obj[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func getStringSlice(obj map[string]any, key string) []string {
	v, ok := obj[key]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
