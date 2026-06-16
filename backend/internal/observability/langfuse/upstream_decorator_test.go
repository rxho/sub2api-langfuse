package langfuse

import (
	"testing"
)

// 真实截取自 Anthropic SSE 响应的事件片段（message_start / content_block_delta / message_delta）。
const anthropicSSE = `event: message_start
data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-sonnet-4-20250514","usage":{"input_tokens":25,"cache_creation_input_tokens":128,"cache_read_input_tokens":0,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", world"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":15}}

event: message_stop
data: {"type":"message_stop"}

`

func TestParseAnthropicSSE(t *testing.T) {
	usage, output := parseAnthropicSSE([]byte(anthropicSSE))
	if usage == nil {
		t.Fatal("expected non-nil usage")
	}
	if usage.Input != 25 {
		t.Errorf("input_tokens = %d, want 25", usage.Input)
	}
	if usage.Output != 15 {
		t.Errorf("output_tokens = %d, want 15", usage.Output)
	}
	if usage.Total != 40 {
		t.Errorf("total = %d, want 40", usage.Total)
	}
	if usage.CacheCreation != 128 {
		t.Errorf("cache_creation = %d, want 128", usage.CacheCreation)
	}
	wantOut := "Hello, world"
	if output != wantOut {
		t.Errorf("output = %q, want %q", output, wantOut)
	}
}

const anthropicNonStream = `{
  "id": "msg_01",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-4-20250514",
  "content": [{"type": "text", "text": "Hi there"}],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3}
}`

func TestParseAnthropicJSON(t *testing.T) {
	usage, output := parseAnthropicJSON([]byte(anthropicNonStream))
	if usage == nil {
		t.Fatal("expected non-nil usage")
	}
	if usage.Input != 10 {
		t.Errorf("input_tokens = %d, want 10", usage.Input)
	}
	if usage.Output != 5 {
		t.Errorf("output_tokens = %d, want 5", usage.Output)
	}
	if usage.Total != 15 {
		t.Errorf("total = %d, want 15", usage.Total)
	}
	if usage.CacheRead != 3 {
		t.Errorf("cache_read = %d, want 3", usage.CacheRead)
	}
	if output != "Hi there" {
		t.Errorf("output = %v, want %q", output, "Hi there")
	}
}

func TestParseAnthropicResponseEmpty(t *testing.T) {
	usage, output := parseAnthropicResponse(nil, false)
	if usage != nil {
		t.Errorf("expected nil usage, got %+v", usage)
	}
	if output != nil {
		t.Errorf("expected nil output, got %v", output)
	}
}

func TestExtractModelFromBody(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"model":"claude-sonnet-4","messages":[]}`, "claude-sonnet-4"},
		{`{"messages":[]}`, ""},
		{``, ""},
	}
	for _, c := range cases {
		if got := extractModelFromBody([]byte(c.body)); got != c.want {
			t.Errorf("extractModelFromBody(%q) = %q, want %q", c.body, got, c.want)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("LANGFUSE_ENABLED", "true")
	t.Setenv("LANGFUSE_HOST", "https://langfuse.example.com/")
	t.Setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
	t.Setenv("LANGFUSE_SECRET_KEY", "sk-lf-test")

	cfg := LoadConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled=true")
	}
	if cfg.Host != "https://langfuse.example.com" {
		t.Errorf("Host = %q, want trailing slash trimmed", cfg.Host)
	}
	if !cfg.Usable() {
		t.Error("expected Usable()=true with all fields set")
	}
}

func TestLoadConfigDisabled(t *testing.T) {
	t.Setenv("LANGFUSE_ENABLED", "false")
	cfg := LoadConfig()
	if cfg.Usable() {
		t.Error("expected Usable()=false when disabled")
	}
}
