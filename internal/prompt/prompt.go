// Package prompt builds the system and user messages for the Ollama LLM call.
package prompt

import (
	"fmt"
	"strings"

	"github.com/infoblox/vibecoder-analyzer/internal/logparse"
	"github.com/infoblox/vibecoder-analyzer/internal/retriever"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

const systemPrompt = `You are a senior software engineer performing root-cause analysis on production errors.
You will receive an error description, intelligence context (symbol definitions, function metadata,
call graph, architecture map), and numbered code chunks retrieved from the codebase.

## OUTPUT FORMAT — STRICT

Respond with EXACTLY ONE JSON object and NOTHING else. No prose, no preamble,
no closing remarks, no markdown code fences, no <think> blocks, no reasoning
text. The very first character of your response MUST be '{' and the very last
character MUST be '}'. The object MUST be valid JSON that parses with a strict
JSON parser (RFC 8259): double-quoted keys and strings, no trailing commas,
no comments, no single quotes, no unquoted identifiers. Escape newlines inside
string values as \n. If a field is unknown, use an empty string, 0, or [] —
never null, never omit a required field. Conform exactly to the schema below.

Follow this EXACT 6-step rubric internally — but emit ONLY the final JSON.

## Steps

1. CLASSIFY — Determine error_type and category (one of: bug, config, dependency, infra, data, unknown).
2. LOCATE — Identify the file and line number of the defect. Use ONLY files present in the supplied chunks or symbol definitions. Never invent file paths, package names, or symbols.
3. EXPLAIN — Write a root_cause explanation in 2–4 sentences.
4. FIX — Provide a minimal before→after patch. The "before" field MUST be copied VERBATIM from one of the supplied chunks. The "after" field is your corrected version.
5. GROUND — Every snippet you quote (cause_code, fixes[].before) MUST appear verbatim in the supplied chunks. Do NOT fabricate code.
6. CALIBRATE — Assign a confidence score between 0.0 and 1.0:
   - 0.8–1.0: exact file+line identified, fix is straightforward, code appears in chunks
   - 0.5–0.79: likely file identified, root cause plausible but not certain
   - 0.2–0.49: weak evidence, multiple possibilities
   - 0.0–0.19: insufficient information
   When confidence < 0.5, populate "alternatives" and "needs_more_info" instead of guessing.

## Intelligence Context Usage

- Use the Symbol definitions to precisely locate where code is defined (exact file:line).
- Use Function metadata (signature, imports, returns) to understand function contracts.
- Use the Call graph to trace execution flow from the error site to related functions.
- Use the Repository map to understand architectural layers and dependencies.
- Prefer these exact references over searching through code chunks when identifying file:line locations.

## Output JSON Schema (return this object verbatim — fill in values)

{
  "title": "string — short summary of the issue",
  "severity": "low|medium|high|critical",
  "category": "bug|config|dependency|infra|data|unknown",
  "problem": "string — one-paragraph description",
  "file": "string — file path from chunks",
  "line": 0,
  "cause_code": "string — the buggy code snippet, VERBATIM from a chunk",
  "root_cause": "string — 2-4 sentence explanation",
  "evidence": [{"chunk_index": 0, "file": "", "start_line": 0, "end_line": 0, "why": ""}],
  "fixes": [{"file": "", "start_line": 0, "end_line": 0, "before": "VERBATIM from chunk", "after": "your fix", "language": "", "rationale": ""}],
  "suggestion": "string — additional recommendation",
  "tests": ["string — regression test ideas"],
  "alternatives": ["string — other possible causes if unsure"],
  "needs_more_info": ["string — what additional info would help"],
  "confidence": 0.0
}

## REMINDERS

- Output ONLY the JSON object. No markdown, no explanation, no <think> blocks, no text before '{' or after '}'.
- Use the exact field names and types from the schema. All required fields must be present.
- Never reference files or symbols not present in the chunks or intelligence context.
- If you cannot identify the bug, set confidence < 0.3 and populate needs_more_info — still return the full JSON object.
- When a "Service context" block is provided, treat it as authoritative for what each package/file is SUPPOSED to do; treat the code chunks as evidence of how it currently behaves. If a chunk contradicts the documented intent, prefer the code as ground truth for the bug location and cite the contradiction in root_cause.`

// Build constructs the system prompt and user message for the LLM.
// serviceSummary is the cached per-repo summary (§7a); pass "" to omit.
// intel contains feeder v2 intelligence data; zero-value is safe.
func Build(signals logparse.Signals, chunks []weaviate.Chunk, serviceSummary string, intel retriever.Intelligence) (system string, user string) {
	var b strings.Builder

	// 1. Service context (§7a) — always first so the model frames
	// the code chunks against documented intent.
	if s := strings.TrimSpace(serviceSummary); s != "" {
		b.WriteString("## Service context\n")
		b.WriteString(s)
		if !strings.HasSuffix(s, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	// 2. Symbol definitions — exact file:line locations.
	if len(intel.Symbols) > 0 {
		b.WriteString("## Symbol Definitions\n\n")
		for _, sym := range intel.Symbols {
			if sym.Receiver != "" {
				b.WriteString(fmt.Sprintf("- %s (%s on %s) at %s:%d [%s]\n",
					sym.Name, sym.Type, sym.Receiver, sym.FilePath, sym.Line, sym.Package))
			} else {
				b.WriteString(fmt.Sprintf("- %s (%s) at %s:%d [%s]\n",
					sym.Name, sym.Type, sym.FilePath, sym.Line, sym.Package))
			}
		}
		b.WriteString("\n")
	}

	// 3. Function metadata — signatures, imports, returns, summaries.
	if len(intel.Functions) > 0 {
		b.WriteString("## Function Metadata\n\n")
		for _, fn := range intel.Functions {
			b.WriteString(fmt.Sprintf("### %s\n", fn.FunctionName))
			b.WriteString(fmt.Sprintf("  File: %s\n", fn.FilePath))
			b.WriteString(fmt.Sprintf("  Signature: %s\n", fn.Signature))
			if fn.Receiver != "" {
				b.WriteString(fmt.Sprintf("  Receiver: %s\n", fn.Receiver))
			}
			if len(fn.Returns) > 0 {
				b.WriteString(fmt.Sprintf("  Returns: %s\n", strings.Join(fn.Returns, ", ")))
			}
			if fn.Summary != "" {
				b.WriteString(fmt.Sprintf("  Summary: %s\n", fn.Summary))
			}
			b.WriteString("\n")
		}
	}

	// 4. Call graph — caller→callee edges.
	if len(intel.CallEdges) > 0 {
		b.WriteString("## Call Graph\n\n")
		for _, edge := range intel.CallEdges {
			files := ""
			if edge.CallerFile != "" || edge.CalleeFile != "" {
				files = fmt.Sprintf(" (%s → %s)", edge.CallerFile, edge.CalleeFile)
			}
			b.WriteString(fmt.Sprintf("- %s → %s%s\n", edge.Caller, edge.Callee, files))
		}
		b.WriteString("\n")
	}

	// 5. Repository map — architectural layers and dependencies.
	if len(intel.RepoMap) > 0 {
		b.WriteString("## Architecture Map\n\n")
		for _, node := range intel.RepoMap {
			deps := ""
			if len(node.DependsOn) > 0 {
				deps = " → " + strings.Join(node.DependsOn, ", ")
			}
			b.WriteString(fmt.Sprintf("- %s [%s] (%s)%s\n",
				node.NodeName, node.NodeType, node.FilePath, deps))
		}
		b.WriteString("\n")
	}

	// 6. Error signals.
	b.WriteString("## Error Analysis Request\n\n")
	if signals.ErrorType != "" {
		b.WriteString(fmt.Sprintf("Error Type: %s\n", signals.ErrorType))
	}
	if signals.ErrorMessage != "" {
		b.WriteString(fmt.Sprintf("Error Message: %s\n", signals.ErrorMessage))
	}
	if signals.HTTPMethod != "" {
		b.WriteString(fmt.Sprintf("HTTP: %s %s", signals.HTTPMethod, signals.HTTPPath))
		if signals.HTTPStatus > 0 {
			b.WriteString(fmt.Sprintf(" → %d", signals.HTTPStatus))
		}
		b.WriteString("\n")
	}
	if len(signals.FileFrames) > 0 {
		b.WriteString("Stack frames:\n")
		for _, ff := range signals.FileFrames {
			b.WriteString(fmt.Sprintf("  %s:%d\n", ff.Path, ff.Line))
		}
	}
	if len(signals.Symbols) > 0 {
		b.WriteString(fmt.Sprintf("Symbols: %s\n", strings.Join(signals.Symbols, ", ")))
	}

	b.WriteString(fmt.Sprintf("\nQuery: %s\n", signals.QueryString()))

	// 7. Retrieved code chunks — primary evidence.
	b.WriteString("\n## Retrieved Code Chunks\n\n")
	for i, ch := range chunks {
		b.WriteString(fmt.Sprintf("[%d] %s/%s:%d-%d (%s)\n", i+1, ch.Repo, ch.FilePath, ch.StartLine, ch.EndLine, ch.Language))
		b.WriteString(ch.Content)
		if !strings.HasSuffix(ch.Content, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	// Final reminder — last instruction wins for most models.
	b.WriteString("\n## Respond Now\n")
	b.WriteString("Return EXACTLY ONE JSON object matching the schema. ")
	b.WriteString("First character must be '{', last must be '}'. ")
	b.WriteString("No prose, no markdown, no <think> blocks.\n")

	return systemPrompt, b.String()
}
