# analyzer

HTTP service that takes a production log line or free-form error prompt,
retrieves relevant code chunks **and intelligence artifacts** (symbols,
functions, call graph, architecture map) from Weaviate, primes the LLM
with per-repo file summaries cached in Redis, and returns a structured
root-cause `Issue` JSON.

The **repo-indexer v2** indexes repos into six Weaviate classes. The analyzer
is read-only against Weaviate and writes only to Redis (summary cache).

See [docs/04-analyzer-service.md](../docs/04-analyzer-service.md) for the full design.

## Prerequisites

| Requirement | Purpose | Default URL |
|---|---|---|
| Go 1.23+ | Build | — |
| Weaviate (already populated by `repo-indexer`) | Code + docs search | `http://localhost:8080` |
| Ollama with a chat model | LLM generation | `http://localhost:11434` |
| Ollama with `nomic-embed-text:latest` | Query embeddings | same |
| Redis | Summary cache (`§7a`), Loki checkpoint, issue sink | `redis://localhost:6379/0` |

```bash
# One-time: pull models
ollama pull nomic-embed-text:latest
ollama pull deepseek-r1:32b

# Sanity-check deps
redis-cli ping                                                            # → PONG
curl -sf http://localhost:8080/v1/.well-known/ready && echo weaviate OK
curl -sf http://localhost:11434/api/tags | grep -q deepseek && echo ollama OK
```

## Build & run

```bash
cd analyzer
go build -o build/analyzer ./cmd/analyzer

# Start the HTTP server (port 8081 by default)
./build/analyzer serve
```

You should see:

```
{"time":"…","level":"INFO","msg":"starting server","addr":":8081"}
```

If Redis is reachable, the summary cache is active. If it's not, you'll
see a single `WARN redis unreachable; running without summary cache` and
the service keeps running — doc priming just degrades to off.

Other subcommands:

```bash
./build/analyzer version
./build/analyzer help
```

The `serve` command runs **two** ingestion paths concurrently:

1. The HTTP API on `PORT` (default `8081`) — `GET /healthz`, `POST /analyze`.
2. A Kafka consumer (when `KAFKA_BROKERS` is set) that reads log records
   produced by [log-shipper](../log-shipper) **one at a time** (configurable
   batch size, default `1`), runs the same pipeline, and publishes the
   resulting `Issue` JSON to Kafka topic `teams-notifier` so that
   [notifier](../notifier) can forward it to Microsoft Teams.

```
log-shipper ─▶ Kafka(service-logs) ─▶ analyzer (1 log → 1 analyze call → 1 Issue)
                                       ─▶ Kafka(teams-notifier) ─▶ notifier ─▶ Teams
                                       (HTTP /analyze remains available)
```

## Configuration (env vars)

All defaults are wired for local development. Override via env.

### Core

| Var | Default | Notes |
|---|---|---|
| `PORT` | `8081` | HTTP listen port |
| `WEAVIATE_URL` | `http://localhost:8080` | |
| `WEAVIATE_CLASS` | `RepoChunk` | Must match the repo-indexer |
| `OLLAMA_URL` | `http://localhost:11434` | |
| `OLLAMA_MODEL` | `deepseek-r1:32b` | Chat model |
| `OLLAMA_TIMEOUT` | `180s` | Chat call timeout |
| `EMBED_MODEL` | `nomic-embed-text:latest` | **Must match the model the repo-indexer used.** Query is embedded locally because the `RepoChunk` class is created with `vectorizer: none`. |
| `EMBED_TIMEOUT` | `30s` | Embed call timeout |
| `DEFAULT_TOP_K` | `8` | Chunks per query |
| `HYBRID_ALPHA` | `0.65` | Weaviate hybrid α (1=vector, 0=BM25) |
| `API_KEY` | _(empty)_ | When set, required as `Authorization: Bearer <key>` |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | `5` / `10` | Token bucket |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | `debug|info|warn|error`, `json|text` |

### Doc-priming summary cache (§7a)

| Var | Default | Notes |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Set empty to disable Redis entirely |
| `SUMMARY_ENABLED` | `true` | Master switch |
| `DOCS_PATH_PREFIX` | `site/` | Where Hugo docs live in each repo |
| `SUMMARY_REDIS_KEY_PREFIX` | `analyzer:summary:` | Final key: `analyzer:summary:<repo>` |
| `SUMMARY_TTL` | `24h` | |
| `SUMMARY_MAX_DOC_CHUNKS` | `30` | Cap on docs fed to the summarizer |
| `SUMMARY_MAX_TOKENS` | `4096` | Truncates the summary string |

### Intelligence enrichment (repo-indexer v2)

| Var | Default | Notes |
|---|---|---|
| `INTELLIGENCE_ENABLED` | `true` | Master switch for Symbol/Function/CallEdge/RepoMap queries |
| `SYMBOL_LOOKUP_LIMIT` | `10` | Max symbols to resolve per request |
| `FUNCTION_SEARCH_LIMIT` | `10` | Max function metadata results |
| `CALLGRAPH_DEPTH` | `2` | Max hops when traversing call edges |
| `CALLGRAPH_MAX_EDGES` | `20` | Cap on total call edges per request |
| `REPOMAP_LIMIT` | `20` | Max repository map nodes |
| `MAX_CONTEXT_TOKENS` | `50000` | Hard cap on total context sent to LLM |

When intelligence classes are empty (repo-indexer v1 index), the analyzer
degrades gracefully — enrichment steps return zero results and the
pipeline proceeds with RepoChunk-only evidence.

### Kafka ingestion (consume log-shipper, publish to the notifier)

Auto-enabled when `KAFKA_BROKERS` is non-empty. Set `KAFKA_ENABLED=false`
to force-disable, or leave `KAFKA_OUTPUT_TOPIC` empty to consume + log
only (no publish back to Kafka).

| Var | Default | Notes |
|---|---|---|
| `KAFKA_ENABLED` | _(auto)_ | `true`/`false`; defaults to `true` iff `KAFKA_BROKERS` is set |
| `KAFKA_BROKERS` | _(empty)_ | Comma-separated, e.g. `localhost:9092` |
| `KAFKA_CLIENT_ID` | `analyzer` | Producer ID is `<id>-producer` |
| `KAFKA_GROUP_ID` | `analyzer` | Consumer group |
| `KAFKA_INPUT_TOPIC` | `service-logs` | Must match log-shipper's output topic |
| `KAFKA_OUTPUT_TOPIC` | `teams-notifier` | Topic the notifier consumes; empty disables publishing |
| `KAFKA_WORKERS` | `1` | Concurrent batch processors (each holds its own batch) |
| `KAFKA_BATCH_SIZE` | `1` | Log records combined into one analyzer call → one published Issue. Default `1` = strict one-at-a-time. |
| `KAFKA_BATCH_TIMEOUT` | `30s` | Max wait before flushing a partial batch (irrelevant when batch size is 1) |
| `KAFKA_SESSION_TIMEOUT` | `5m` | Consumer-group session |
| `KAFKA_ANALYZE_TIMEOUT` | `5m` | Per-batch pipeline budget |
| `KAFKA_PUBLISH_TIMEOUT` | `10s` | Per-Issue produce budget |
| `KAFKA_SHUTDOWN_TIMEOUT` | `30s` | Final commit + producer flush on shutdown |
| `KAFKA_PUBLISH_MIN_SEVERITY` | `high` | Skip publish when `Issue.severity` rank is below this (`low<medium<high<critical`). Use `low` to publish everything; `critical` for panic-only. |
| `KAFKA_PUBLISH_MIN_CONFIDENCE` | `0.5` | Skip publish when `Issue.confidence < this`. Use `0` to disable the confidence filter. |

**Publish filter.** After the pipeline produces an `Issue`, the consumer
applies the severity/confidence thresholds **before** producing to
`KAFKA_OUTPUT_TOPIC`. Skipped Issues are still committed (offset
advances) and logged as `analyzed batch (publish skipped)` with the
reason — they just don't reach Teams. Defaults (`high` + `0.5`) silence
low-signal noise from info-level logs that the model rates as
`low / confidence<0.5`.

**Published payload (envelope).** The producer wraps each Issue with
the last batched log record's context so downstream consumers can
surface per-tenant identifiers (customer / flow IDs):

```json
{
  "issue": { "title": "...", "severity": "high", ... },
  "log": {
    "stream": { "customer_id": "acme-7", "flow_id": "ingest.v3", "service_name": "grpc-in", "containerId": "cdc_grpc_in" },
    "ts":   "1717500000000000000",
    "line": "level=error customer_id=acme-7 flow_id=ingest.v3 msg=\"...\""
  }
}
```

The envelope is omitted (bare `Issue` is produced) only when the log
record carries no labels, line, or timestamp.

**Batching behavior.** Each worker accumulates up to `KAFKA_BATCH_SIZE`
records (default `1` — one log per analyzer call) or waits up to
`KAFKA_BATCH_TIMEOUT` (default `30s`), whichever comes first. With the
default `KAFKA_BATCH_SIZE=1` each record is immediately processed and
produces exactly one `Issue` on `KAFKA_OUTPUT_TOPIC`. When batching is
enabled (>1) the combined input is formatted as:

```
[log 1/3]
<line from record 1>

[log 2/3]
<line from record 2>

[log 3/3]
<line from record 3>
```

and passed to the analyzer pipeline as a single `Query` (with the
majority `service_name` from the batch as the repo hint). The resulting
`Issue` is published **once** to `KAFKA_OUTPUT_TOPIC` keyed by the last
record's Kafka key (preserving per-service partition affinity), then
all successfully-handled records are mark-committed together so the
offsets advance atomically.

**Failure handling.**

- Poison/decode error → log + mark that single record (rest of batch continues).
- Pipeline or publish error (non-shutdown) → log + mark the whole batch
  (visible loss preferred over silent latency growth).
- Context cancellation (shutdown) → do NOT mark; the batch is
  redelivered on next start.

**Sanity caveats.**

  Don't crank `KAFKA_WORKERS` without sizing Ollama + Weaviate.
- The shipper already filters for `error|panic|fatal` server-side, so
  the analyzer trusts upstream filtering and does not re-filter.
- Increasing `KAFKA_BATCH_SIZE` reduces LLM calls per minute but
  increases the input token count and the time-to-first-Issue.

Inspect the cache:

```bash
redis-cli --scan --pattern 'analyzer:summary:*'
redis-cli get analyzer:summary:<repo-name>
redis-cli del analyzer:summary:<repo-name>      # force a refresh
```

## HTTP API

### `GET /healthz`

```bash
curl -s http://localhost:8081/healthz | jq
```

Returns `200` when both Weaviate and Ollama respond to ping, `503` otherwise:

```json
{ "status": "ok", "weaviate": "ok", "ollama": "ok" }
```

### `POST /analyze`

Request body:

| Field | Type | Required | Notes |
|---|---|---|---|
| `input` | string | yes | Raw log line(s) or free-form prompt |
| `mode` | string | no | `"logs"` (default) or `"prompt"` |
| `repo` | string | no | Force a repo; otherwise auto-resolved from anchors |
| `top_k` | int | no | Override `DEFAULT_TOP_K` |

Response: the `Issue` schema from [internal/issue/issue.go](internal/issue/issue.go)
(`title`, `severity`, `category`, `resolved_repo`, `file`, `line`,
`root_cause`, `grounding_ok`, `evidence[]`, `fixes[]`, `confidence`, `references[]`, …).

### `curl` / Postman examples

All examples assume the server is at `http://localhost:8081`. If you set
`API_KEY=xxx`, add `-H "Authorization: Bearer xxx"`.

**1. Health check**

```bash
curl -s http://localhost:8081/healthz | jq
```

**2. Analyze a raw log line (mode=logs, auto-resolve repo)**

```bash
curl -s -X POST http://localhost:8081/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "input": "2026-05-28T10:14:22Z ERROR repo-indexer/internal/embedder/embedder.go:142 ollama embed batch failed: context deadline exceeded",
    "mode": "logs"
  }' | jq
```

**3. Free-form prompt, pinned repo, custom top_k**

```bash
curl -s -X POST http://localhost:8081/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "input": "Weaviate batch upload is intermittently returning 503; what is the retry strategy and where is it configured?",
    "mode": "prompt",
    "repo": "repo-indexer",
    "top_k": 12
  }' | jq
```

**4. Multi-line stack trace (escape newlines as `\n`)**

```bash
curl -s -X POST http://localhost:8081/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "input": "panic: runtime error: invalid memory address or nil pointer dereference\n\tgoroutine 42 [running]:\n\tgithub.com/infoblox/vibecoder-analyzer/internal/retriever.(*Retriever).Run(0x0, …)\n\t\t/app/internal/retriever/retriever.go:88 +0x2c",
    "mode": "logs"
  }' | jq
```

**5. With API key (when `API_KEY` is set)**

```bash
curl -s -X POST http://localhost:8081/analyze \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-api-key-here' \
  -d '{"input":"ERROR …","mode":"logs"}' | jq
```

**6. Ambiguous-repo response (HTTP 422)**

When the input matches multiple repos within `RESOLVE_REPO_AMBIGUITY`,
the server replies `422` with candidates — re-send with `"repo"` pinned:

```json
{
  "error": "repo is ambiguous",
  "candidates": ["repo-indexer", "analyzer"],
  "request_id": "…"
}
```

### Postman

1. **New Collection** → "vibecoder analyzer".
2. **New Request** → `POST {{baseUrl}}/analyze`.
3. Collection variable `baseUrl` = `http://localhost:8081`.
4. **Headers**: `Content-Type: application/json` (and `Authorization: Bearer {{apiKey}}` if you set one).
5. **Body** → raw → JSON → paste any of the JSON bodies above.
6. Add a second request `GET {{baseUrl}}/healthz`.

Or import this minimal collection as `analyzer.postman_collection.json`:

```json
{
  "info": { "name": "vibecoder analyzer", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json" },
  "variable": [
    { "key": "baseUrl", "value": "http://localhost:8081" },
    { "key": "apiKey", "value": "" }
  ],
  "item": [
    {
      "name": "GET /healthz",
      "request": { "method": "GET", "url": "{{baseUrl}}/healthz" }
    },
    {
      "name": "POST /analyze (log line)",
      "request": {
        "method": "POST",
        "header": [
          { "key": "Content-Type", "value": "application/json" },
          { "key": "Authorization", "value": "Bearer {{apiKey}}", "disabled": true }
        ],
        "url": "{{baseUrl}}/analyze",
        "body": {
          "mode": "raw",
          "raw": "{\n  \"input\": \"ERROR repo-indexer/internal/embedder/embedder.go:142 ollama embed batch failed: context deadline exceeded\",\n  \"mode\": \"logs\"\n}"
        }
      }
    },
    {
      "name": "POST /analyze (free-form, pinned repo)",
      "request": {
        "method": "POST",
        "header": [
          { "key": "Content-Type", "value": "application/json" }
        ],
        "url": "{{baseUrl}}/analyze",
        "body": {
          "mode": "raw",
          "raw": "{\n  \"input\": \"Weaviate batch upload returns 503 intermittently; where is the retry?\",\n  \"mode\": \"prompt\",\n  \"repo\": \"repo-indexer\",\n  \"top_k\": 12\n}"
        }
      }
    }
  ]
}
```

## Pipeline (per `/analyze` request)

```
parse signals
  → resolve repo (hybrid search + multi-repo resolution)
  → retrieve code chunks (Weaviate RepoChunk)
  → fetch service summary (Redis cache ← FileSummary objects)
  → enrich symbols (Weaviate Symbol class — exact match)
  → enrich functions (Weaviate Function class — metadata)
  → enrich call graph (Weaviate CallEdge class — traversal)
  → enrich repo map (Weaviate RepositoryMap class — architecture)
  → build prompt (system + summary + symbols + functions + callgraph + repomap + chunks)
  → Ollama generate → ground-check anchors → calibrate confidence
  → return Issue JSON
```

The service summary is built once per repo per `SUMMARY_TTL` from the
repo-indexer's `FileSummary` objects and prepended as a `## Service context`
block, so the LLM knows _what the service is supposed to do_ before
reasoning about the failure. Intelligence context (symbols, functions,
call graph, architecture) helps the LLM precisely locate definitions
and trace execution flow.

## Known gaps / future work

The v2 analyzer consumes all six repo-indexer intelligence classes but does
**not** yet implement these Cursor/Copilot-style capabilities:

1. **Agentic retrieval loop** — current enrichment is single-shot
   (`Symbols → Functions → CallGraph → RepoMap`). A true agent would
   re-query Weaviate when confidence is low (e.g. "need more info? →
   read callee definitions → re-rank chunks").
2. **Runtime/IDE context** — open tabs, cursor position, git branch,
   recent edits. Not applicable to an HTTP service; requires an IDE
   integration.
3. **Git history retrieval** — blame, recent commits touching a file.
   Not indexed by repo-indexer v2.
4. **Redis caching of intelligence lookups** — repo-indexer spec §"Redis
   Caching" calls for `symbol:*`, `callgraph:*`, `function:*` keys with
   1h TTL. Currently only the per-repo service summary (built from
   `FileSummary`) is cached; symbol/function/callgraph queries hit
   Weaviate on every request.

## Development

```bash
go test ./...              # unit tests
go test -race ./...        # race detector
go vet ./...
```
