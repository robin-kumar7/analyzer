// Package logparse extracts structured signals from error logs or free-text prompts.
package logparse

import (
	"regexp"
	"strings"
)

// FileFrame is a file:line reference extracted from a stack trace.
type FileFrame struct {
	Path string
	Line int
}

// Signals holds structured information extracted from log input.
type Signals struct {
	ErrorType    string
	ErrorMessage string
	HTTPStatus   int
	HTTPMethod   string
	HTTPPath     string
	FileFrames   []FileFrame
	Symbols      []string
	RawQuery     string
}

var (
	// Go panic / runtime patterns.
	panicRe      = regexp.MustCompile(`(?i)^(?:panic|fatal error|goroutine \d+):\s*(.+)`)
	goFileLineRe = regexp.MustCompile(`([a-zA-Z0-9_/.-]+\.go):(\d+)`)
	// General file:line (any extension).
	fileLineRe = regexp.MustCompile(`([a-zA-Z0-9_/.-]+\.[a-zA-Z0-9]+):(\d+)`)
	// HTTP status codes.
	httpStatusRe = regexp.MustCompile(`\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(/[^\s]*)`)
	statusCodeRe = regexp.MustCompile(`\b([1-5]\d{2})\b`)
	// Go function/symbol names.
	goFuncRe = regexp.MustCompile(`([a-zA-Z_]\w*(?:\.\w+)+)\(`)
	// Identifiers: camelCase or snake_case, at least 2 parts.
	camelRe = regexp.MustCompile(`\b([a-z]+[A-Z][a-zA-Z0-9]+)\b`)
	snakeRe = regexp.MustCompile(`\b([a-z]+_[a-z_]+[a-z])\b`)
	// Quoted strings.
	quotedRe = regexp.MustCompile(`"([^"]+)"`)
	// Error type patterns.
	errorTypeRe = regexp.MustCompile(`(?i)\b(panic|nil pointer dereference|index out of range|divide by zero|runtime error|timeout|connection refused|EOF|deadlock)\b`)
)

// Extract parses the input and returns structured Signals. mode is "logs" or "prompt".
func Extract(input, mode string) Signals {
	s := Signals{}
	if mode == "logs" {
		s = extractLogs(input)
	} else {
		s = extractPrompt(input)
	}
	if s.RawQuery == "" {
		s.RawQuery = buildRawQuery(s, input)
	}
	return s
}

func extractLogs(input string) Signals {
	s := Signals{}
	lines := strings.Split(input, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if m := panicRe.FindStringSubmatch(line); len(m) > 1 {
			s.ErrorMessage = m[1]
			s.ErrorType = "panic"
		}
		if m := errorTypeRe.FindStringSubmatch(line); len(m) > 1 && s.ErrorType == "" {
			s.ErrorType = strings.ToLower(m[1])
		}
	}

	// Extract file:line frames.
	for _, m := range goFileLineRe.FindAllStringSubmatch(input, -1) {
		line := 0
		for _, c := range m[2] {
			line = line*10 + int(c-'0')
		}
		s.FileFrames = append(s.FileFrames, FileFrame{Path: m[1], Line: line})
	}

	// Extract HTTP method/path.
	if m := httpStatusRe.FindStringSubmatch(input); len(m) > 2 {
		s.HTTPMethod = m[1]
		s.HTTPPath = m[2]
	}

	// Extract HTTP status code.
	if m := statusCodeRe.FindStringSubmatch(input); len(m) > 1 {
		code := 0
		for _, c := range m[1] {
			code = code*10 + int(c-'0')
		}
		s.HTTPStatus = code
	}

	// Extract function/symbol names.
	seen := map[string]bool{}
	for _, m := range goFuncRe.FindAllStringSubmatch(input, -1) {
		sym := m[1]
		if !seen[sym] {
			s.Symbols = append(s.Symbols, sym)
			seen[sym] = true
		}
	}

	return s
}

func extractPrompt(input string) Signals {
	s := Signals{}

	// Extract HTTP method/path from free text.
	if m := httpStatusRe.FindStringSubmatch(input); len(m) > 2 {
		s.HTTPMethod = m[1]
		s.HTTPPath = m[2]
	}
	if m := statusCodeRe.FindStringSubmatch(input); len(m) > 1 {
		code := 0
		for _, c := range m[1] {
			code = code*10 + int(c-'0')
		}
		s.HTTPStatus = code
	}
	if m := errorTypeRe.FindStringSubmatch(input); len(m) > 1 {
		s.ErrorType = strings.ToLower(m[1])
	}

	// Extract identifiers.
	seen := map[string]bool{}
	for _, m := range camelRe.FindAllStringSubmatch(input, -1) {
		if !seen[m[1]] {
			s.Symbols = append(s.Symbols, m[1])
			seen[m[1]] = true
		}
	}
	for _, m := range snakeRe.FindAllStringSubmatch(input, -1) {
		if !seen[m[1]] {
			s.Symbols = append(s.Symbols, m[1])
			seen[m[1]] = true
		}
	}

	// Extract quoted strings.
	for _, m := range quotedRe.FindAllStringSubmatch(input, -1) {
		if !seen[m[1]] {
			s.Symbols = append(s.Symbols, m[1])
			seen[m[1]] = true
		}
	}

	// Extract file:line if present.
	for _, m := range fileLineRe.FindAllStringSubmatch(input, -1) {
		line := 0
		for _, c := range m[2] {
			line = line*10 + int(c-'0')
		}
		s.FileFrames = append(s.FileFrames, FileFrame{Path: m[1], Line: line})
	}

	// Use the entire input as raw query since it's free text.
	s.RawQuery = input

	return s
}

func buildRawQuery(s Signals, input string) string {
	var parts []string
	if s.ErrorType != "" {
		parts = append(parts, s.ErrorType)
	}
	if s.ErrorMessage != "" {
		parts = append(parts, s.ErrorMessage)
	}
	if s.HTTPMethod != "" && s.HTTPPath != "" {
		parts = append(parts, s.HTTPMethod+" "+s.HTTPPath)
	}
	for _, ff := range s.FileFrames {
		parts = append(parts, ff.Path)
	}
	for _, sym := range s.Symbols {
		parts = append(parts, sym)
	}
	if len(parts) == 0 {
		return input
	}
	return strings.Join(parts, " ")
}

// QueryString returns the combined query for hybrid search.
func (s Signals) QueryString() string {
	return s.RawQuery
}
