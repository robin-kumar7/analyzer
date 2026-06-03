package logparse

import (
	"testing"
)

func TestExtract_GoStackTrace(t *testing.T) {
	input := `goroutine 1 [running]:
main.handleItems(0xc0000b4000, {0xc000124050, 0x3, 0x4})
	/app/internal/handlers/items.go:42 +0x1a5
net/http.HandlerFunc.ServeHTTP(0xc0000b6000, {0x7ff6e0, 0xc0000fe000}, 0xc000128000)
	/usr/local/go/src/net/http/server.go:2220 +0x29
panic: runtime error: index out of range [5] with length 3`

	s := Extract(input, "logs")

	if s.ErrorType != "panic" {
		t.Errorf("ErrorType = %q, want panic", s.ErrorType)
	}
	if len(s.FileFrames) == 0 {
		t.Fatal("expected file frames")
	}
	foundItems := false
	for _, ff := range s.FileFrames {
		if ff.Path == "/app/internal/handlers/items.go" && ff.Line == 42 {
			foundItems = true
		}
	}
	if !foundItems {
		t.Errorf("expected items.go:42 in file frames, got %+v", s.FileFrames)
	}
	if len(s.Symbols) == 0 {
		t.Error("expected symbols extracted from stack trace")
	}
	if s.QueryString() == "" {
		t.Error("QueryString() should not be empty")
	}
}

func TestExtract_HTTPError(t *testing.T) {
	input := `2026-05-13T10:00:00Z ERROR POST /payments/charge returned 500: internal server error`

	s := Extract(input, "logs")

	if s.HTTPMethod != "POST" {
		t.Errorf("HTTPMethod = %q, want POST", s.HTTPMethod)
	}
	if s.HTTPPath != "/payments/charge" {
		t.Errorf("HTTPPath = %q, want /payments/charge", s.HTTPPath)
	}
	if s.HTTPStatus != 500 {
		t.Errorf("HTTPStatus = %d, want 500", s.HTTPStatus)
	}
}

func TestExtract_PromptMode(t *testing.T) {
	input := `I'm getting a 500 from POST /payments/charge but no stack trace. The handlePayment function seems broken.`

	s := Extract(input, "prompt")

	if s.HTTPMethod != "POST" {
		t.Errorf("HTTPMethod = %q, want POST", s.HTTPMethod)
	}
	if s.HTTPPath != "/payments/charge" {
		t.Errorf("HTTPPath = %q, want /payments/charge", s.HTTPPath)
	}
	foundSym := false
	for _, sym := range s.Symbols {
		if sym == "handlePayment" {
			foundSym = true
		}
	}
	if !foundSym {
		t.Errorf("expected handlePayment in symbols, got %v", s.Symbols)
	}
	if s.RawQuery == "" {
		t.Error("RawQuery should contain the full prompt text")
	}
}

func TestExtract_NilPointerDereference(t *testing.T) {
	input := `panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x18 pc=0x6e7a83]

goroutine 6 [running]:
main.getUser(0xc000010200, {0x7ff6e0, 0xc0000fe000}, 0xc000128000)
	/app/internal/handlers/users.go:28 +0x63`

	s := Extract(input, "logs")

	if s.ErrorType != "panic" {
		t.Errorf("ErrorType = %q, want panic", s.ErrorType)
	}
}
