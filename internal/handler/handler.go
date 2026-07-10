// Package handler implements the Gin HTTP routes and middleware.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/infoblox/vibecoder-analyzer/internal/analyzer"
	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/retriever"
	"github.com/infoblox/vibecoder-analyzer/internal/store"
	"github.com/infoblox/vibecoder-analyzer/internal/summarizer"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// analyzeRequest is the POST /analyze JSON body.
type analyzeRequest struct {
	Input string `json:"input" binding:"required"`
	Mode  string `json:"mode"`
	Repo  string `json:"repo"`
	TopK  int    `json:"top_k"`
}

// errorResponse is a structured error reply.
type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

// New builds the Gin engine with all routes and middleware.
// summ may be nil to disable documentation priming.
// emb may be nil to disable query embedding (BM25-only fallback).
// intel may be nil to disable intelligence enrichment (repo-indexer v2).
// st may be nil to disable analytics persistence (errors swallowed at the call site).
func New(cfg *config.Config, wc weaviate.Searcher, intel weaviate.IntelligenceSearcher, gen ollama.Generator, emb ollama.Embedder, summ summarizer.Summarizer, st store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// Middleware stack.
	r.Use(requestIDMiddleware())
	r.Use(gin.Recovery())
	r.Use(bodySizeMiddleware(cfg.MaxBodyBytes))

	if cfg.APIKey != "" {
		r.Use(apiKeyMiddleware(cfg.APIKey))
	}
	if cfg.RateLimitRPS > 0 {
		r.Use(rateLimitMiddleware(cfg.RateLimitRPS, cfg.RateLimitBurst))
	}

	// Build the analysis pipeline.
	ret := retriever.New(wc, intel, emb, cfg)
	az := analyzer.New(ret, gen, summ, cfg)

	// Routes.
	r.GET("/healthz", healthzHandler(wc, gen))
	r.POST("/analyze", analyzeHandler(az, st))

	return r
}

// healthzHandler checks downstream deps (Weaviate ping, Ollama ping).
func healthzHandler(wc weaviate.Searcher, gen ollama.Generator) gin.HandlerFunc {
	type healthResult struct {
		Status   string `json:"status"`
		Weaviate string `json:"weaviate"`
		Ollama   string `json:"ollama"`
	}
	return func(c *gin.Context) {
		wStatus := "ok"
		oStatus := "ok"

		type ctxPinger interface {
			Ping(context.Context) error
		}

		if p, ok := wc.(ctxPinger); ok {
			if err := p.Ping(c.Request.Context()); err != nil {
				wStatus = err.Error()
			}
		}
		if p, ok := gen.(ctxPinger); ok {
			if err := p.Ping(c.Request.Context()); err != nil {
				oStatus = err.Error()
			}
		}

		status := http.StatusOK
		overall := "ok"
		if wStatus != "ok" || oStatus != "ok" {
			status = http.StatusServiceUnavailable
			overall = "degraded"
		}
		c.JSON(status, healthResult{
			Status:   overall,
			Weaviate: wStatus,
			Ollama:   oStatus,
		})
	}
}

// analyzeHandler runs the analysis pipeline.
func analyzeHandler(az *analyzer.Analyzer, st store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req analyzeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{
				Error:     "invalid request body: " + err.Error(),
				RequestID: c.GetString("request_id"),
			})
			return
		}

		if req.Mode == "" {
			req.Mode = "logs"
		}
		if req.Mode != "logs" && req.Mode != "prompt" {
			c.JSON(http.StatusBadRequest, errorResponse{
				Error:     `mode must be "logs" or "prompt"`,
				RequestID: c.GetString("request_id"),
			})
			return
		}

		q := analyzer.Query{
			Input: req.Input,
			Mode:  req.Mode,
			Repo:  req.Repo,
			TopK:  req.TopK,
		}
		result, err := az.Run(c.Request.Context(), q)
		if err != nil {
			var ambErr *retriever.ErrAmbiguousRepo
			if errors.As(err, &ambErr) {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"error":      err.Error(),
					"candidates": ambErr.Candidates,
					"request_id": c.GetString("request_id"),
				})
				return
			}
			slog.Error("analysis failed",
				"error", err,
				"request_id", c.GetString("request_id"))
			c.JSON(http.StatusInternalServerError, errorResponse{
				Error:     "analysis failed",
				RequestID: c.GetString("request_id"),
			})
			return
		}

		// Persist the analyzed Issue for analytics (best-effort).
		// HTTP path carries no tenant context (account / flow / ophid land as NULL);
		// rows aggregate under service_name only.
		store.SaveOrLog(c.Request.Context(), st, result, nil, store.SourceHTTP)

		c.JSON(http.StatusOK, result)
	}
}

// --- Middleware ---

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.New().String()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func bodySizeMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

func apiKeyMiddleware(key string) gin.HandlerFunc {
	return func(c *gin.Context) {
		provided := c.GetHeader("Authorization")
		if provided == "" {
			provided = c.GetHeader("X-API-Key")
		}
		expected := "Bearer " + key
		if provided != expected && provided != key {
			c.AbortWithStatusJSON(http.StatusUnauthorized, errorResponse{
				Error:     "unauthorized",
				RequestID: c.GetString("request_id"),
			})
			return
		}
		c.Next()
	}
}

// rateLimitMiddleware implements a simple token-bucket rate limiter.
func rateLimitMiddleware(rps float64, burst int) gin.HandlerFunc {
	var mu sync.Mutex
	tokens := float64(burst)
	last := time.Now()

	return func(c *gin.Context) {
		mu.Lock()
		now := time.Now()
		elapsed := now.Sub(last).Seconds()
		last = now
		tokens += elapsed * rps
		if tokens > float64(burst) {
			tokens = float64(burst)
		}
		if tokens < 1 {
			mu.Unlock()
			c.AbortWithStatusJSON(http.StatusTooManyRequests, errorResponse{
				Error:     "rate limit exceeded",
				RequestID: c.GetString("request_id"),
			})
			return
		}
		tokens--
		mu.Unlock()
		c.Next()
	}
}
