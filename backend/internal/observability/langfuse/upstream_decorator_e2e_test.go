package langfuse

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// fakeUpstream 实现 service.HTTPUpstream，返回预设的响应体。
type fakeUpstream struct {
	statusCode int
	body       string
	isStream   bool // 是否把响应设为 SSE content-type
}

func (f *fakeUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return f.respond(), nil
}

func (f *fakeUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return f.respond(), nil
}

func (f *fakeUpstream) respond() *http.Response {
	code := f.statusCode
	if code == 0 {
		code = 200
	}
	ct := "application/json"
	if f.isStream {
		ct = "text/event-stream"
	}
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{ct}},
		Body:       io.NopCloser(strings.NewReader(f.body)),
	}
}

// captureClient 捕获所有 Enqueue 的事件，便于断言。
type captureClient struct {
	mu     sync.Mutex
	events []Event
}

func (c *captureClient) Enqueue(events ...Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, events...)
}

func (c *captureClient) snapshot() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

// traceBodyOf 从事件体断言出 traceBody（兼容 trace-create / trace-update）。
func traceBodyOf(t *testing.T, e Event) traceBody {
	t.Helper()
	tb, ok := e.Body.(traceBody)
	if !ok {
		t.Fatalf("event %q body is %T, not traceBody", e.Type, e.Body)
	}
	return tb
}

// TestDoEmitsTraceCreateWithInputAndTraceUpdateWithOutput 验证一次成功的上游调用
// 会上报：trace-create（带 input）→ generation-create（带 output）→ trace-update（带 output）。
func TestDoEmitsTraceCreateWithInputAndTraceUpdateWithOutput(t *testing.T) {
	reqBody := `{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://api.anthropic.com/v1/messages", bytes.NewReader([]byte(reqBody)))

	// 上游返回一段非流式 Anthropic JSON
	respBody := `{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"Hello!"}],"usage":{"input_tokens":2,"output_tokens":1}}`

	upstream := &fakeUpstream{body: respBody, isStream: false}
	cli := &captureClient{}

	dec := NewDecorator(upstream, cli, &Config{
		Enabled:         true,
		Host:            "https://lf.example.com",
		PublicKey:       "pk",
		SecretKey:       "sk",
		CaptureMaxBytes: 1 << 20,
	})

	resp, err := dec.Do(req, "", 10, 1)
	if err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	// 业务层会先读完响应体再 Close；此处模拟该顺序，确保 capturingReadCloser tee 到完整内容。
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	events := cli.snapshot()
	var traceCreate, traceUpdate Event
	var genCreate Event
	for _, e := range events {
		switch e.Type {
		case "trace-create":
			traceCreate = e
		case "trace-update":
			traceUpdate = e
		case "generation-create":
			genCreate = e
		}
	}

	// trace-create 必须带 input
	if traceCreate.Type == "" {
		t.Fatal("expected trace-create event")
	}
	tc := traceBodyOf(t, traceCreate)
	if tc.Input == nil {
		t.Error("trace-create input should be non-nil")
	}
	if tc.Output != nil {
		t.Error("trace-create output should be nil at create time")
	}

	// generation-create 必须带 output
	if genCreate.Type == "" {
		t.Fatal("expected generation-create event")
	}
	gb, ok := genCreate.Body.(generationBody)
	if !ok {
		t.Fatalf("generation-create body is %T, not generationBody", genCreate.Body)
	}
	if gb.Output != "Hello!" {
		t.Errorf("generation output = %v, want %q", gb.Output, "Hello!")
	}

	// trace-update 必须带 output（回填），input 应为空（未改动）
	if traceUpdate.Type == "" {
		t.Fatal("expected trace-update event to backfill output")
	}
	tu := traceBodyOf(t, traceUpdate)
	if tu.Output != "Hello!" {
		t.Errorf("trace-update output = %v, want %q", tu.Output, "Hello!")
	}
	if tu.Input != nil {
		t.Error("trace-update should not modify input")
	}
}

// TestDoStreamEmitsTraceUpdateWithOutput 验证流式响应同样回填 trace output。
func TestDoStreamEmitsTraceUpdateWithOutput(t *testing.T) {
	reqBody := `{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://api.anthropic.com/v1/messages", bytes.NewReader([]byte(reqBody)))

	upstream := &fakeUpstream{body: anthropicSSE, isStream: true}
	cli := &captureClient{}

	dec := NewDecorator(upstream, cli, &Config{
		Enabled:         true,
		Host:            "https://lf.example.com",
		PublicKey:       "pk",
		SecretKey:       "sk",
		CaptureMaxBytes: 1 << 20,
	})

	resp, err := dec.Do(req, "", 10, 1)
	if err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	var traceUpdate Event
	for _, e := range cli.snapshot() {
		if e.Type == "trace-update" {
			traceUpdate = e
		}
	}
	if traceUpdate.Type == "" {
		t.Fatal("expected trace-update event for streaming response")
	}
	tu := traceBodyOf(t, traceUpdate)
	if tu.Output != "Hello, world" {
		t.Errorf("trace-update output = %v, want %q", tu.Output, "Hello, world")
	}
}
