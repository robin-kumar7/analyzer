# analyzer

HTTP service (and optional Loki poller) that takes a production log line
or free-form error prompt, retrieves the most relevant code chunks from
Weaviate, primes the LLM with a per-repo documentation summary cached in
Redis, and returns a structured root-cause `Issue` JSON.

The **feeder** indexes repos (including each repo's `site/` Hugo docs)
into Weaviate. The analyzer is read-only against Weaviate and writes
only to Redis (summary cache + optional issue sink).

See [docs/04-analyzer-service.md](../docs/04-analyzer-service.md) for the full design.

## Prerequisites

| Requirement | Purpose | Default URL |
|---|---|---|
| Go 1.23+ | Build | — |
| Weaviate (already populated by `feeder`) | Code + docs search | `http://localhost:8080` |
| Ollama with a chat model | LLM generation | `http://localhost:11434` |
| Ollama with `nomic-embed-text` | Query embeddings | same |
| Redis | Summary cache (`§7a`), Loki checkpoint, issue sink | `redis://localhost:6379/0` |

```bash
# One-time: pull models
ollama pull nomic-embed-text
ollama pull qwen3:30b

# Sanity-check deps
redis-cli ping                                                            # → PONG
curl -sf http://localhost:8080/v1/.well-known/ready && echo weaviate OK
curl -sf http://localhost:11434/api/tags | grep -q qwen3 && echo ollama OK
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
./build/analyzer poll          # Loki poller (placeholder — not implemented)
```

## Configuration (env vars)

All defaults are wired for local development. Override via env.

### Core

| Var | Default | Notes |
|---|---|---|
| `PORT` | `8081` | HTTP listen port |
| `WEAVIATE_URL` | `http://localhost:8080` | |
| `WEAVIATE_CLASS` | `RepoChunk` | Must match the feeder |
| `OLLAMA_URL` | `http://localhost:11434` | |
| `OLLAMA_MODEL` | `qwen3:30b` | Chat model |
| `OLLAMA_TIMEOUT` | `180s` | Chat call timeout |
| `EMBED_MODEL` | `nomic-embed-text` | **Must match the model the feeder used.** Query is embedded locally because the `RepoChunk` class is created with `vectorizer: none`. |
| `EMBED_TIMEOUT` | `30s` | Embed call timeout |
| `DEFAULT_TOP_K` | `8` | Chunks per query |
| `HYBRID_ALPHA` | `0.5` | Weaviate hybrid α (1=vector, 0=BM25) |
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
    "input": "2026-05-28T10:14:22Z ERROR feeder/internal/embedder/embedder.go:142 ollama embed batch failed: context deadline exceeded",
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
    "repo": "feeder",
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
  "candidates": ["feeder", "analyzer"],
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
          "raw": "{\n  \"input\": \"ERROR feeder/internal/embedder/embedder.go:142 ollama embed batch failed: context deadline exceeded\",\n  \"mode\": \"logs\"\n}"
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
          "raw": "{\n  \"input\": \"Weaviate batch upload returns 503 intermittently; where is the retry?\",\n  \"mode\": \"prompt\",\n  \"repo\": \"feeder\",\n  \"top_k\": 12\n}"
        }
      }
    }
  ]
}
```

## Pipeline (per `/analyze` request)

```
parse signals → resolve repo → retrieve chunks (Weaviate)
              → fetch service summary (Redis cache, build on miss)
              → build prompt (system + service-context + signals + chunks)
              → Ollama generate → ground-check anchors → calibrate confidence
              → return Issue JSON
```

The service summary is built once per repo per `SUMMARY_TTL` from that
repo's `site/` Hugo docs and prepended as a `## Service context` block,
so the LLM knows _what the service is supposed to do_ before reasoning
about the failure.

## Development

```bash
go test ./...              # unit tests
go test -race ./...        # race detector
go vet ./...
```
