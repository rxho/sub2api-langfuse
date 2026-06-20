package langfuse

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// 编译期断言：langfuseUpstream 实现 service.HTTPUpstream 接口。
// 接口签名变更时此处立即编译失败，暴露给维护者。
var _ service.HTTPUpstream = (*langfuseUpstream)(nil)

// NewDecorator 用 Langfuse 装饰器包裹原始 HTTPUpstream。
// 返回值可直接替换 wire 中的 NewHTTPUpstream。
func NewDecorator(inner service.HTTPUpstream, client EventEnqueuer, cfg *Config) service.HTTPUpstream {
	return &langfuseUpstream{
		inner:  inner,
		client: client,
		cfg:    cfg,
	}
}

// EventEnqueuer 是 langfuseUpstream 对上报客户端的最小依赖（仅 Enqueue）。
// 抽成接口便于在不启动后台 flush 的情况下做单测。
// *Client 天然实现该接口。
type EventEnqueuer interface {
	Enqueue(events ...Event)
}

type langfuseUpstream struct {
	inner  service.HTTPUpstream
	client EventEnqueuer
	cfg    *Config
}

// Do 实现 service.HTTPUpstream.Do。
func (u *langfuseUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return u.intercept(req, accountID, func() (*http.Response, error) {
		return u.inner.Do(req, proxyURL, accountID, accountConcurrency)
	})
}

// DoWithTLS 实现 service.HTTPUpstream.DoWithTLS。
func (u *langfuseUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.intercept(req, accountID, func() (*http.Response, error) {
		return u.inner.DoWithTLS(req, proxyURL, accountID, accountConcurrency, profile)
	})
}

// intercept 统一处理 Do / DoWithTLS 的捕获逻辑。
//
// 步骤：
//  1. peek 请求体（input），重放 req.Body
//  2. 记录开始时间、业务维度（从 req.Context() 取）
//  3. 调用 inner 发请求
//  4. 失败 → 直接上报 ERROR generation
//  5. 成功 → 在 resp.Body 外层包 capturingReadCloser，Close 时解析 + 上报
func (u *langfuseUpstream) intercept(req *http.Request, accountID int64, call func() (*http.Response, error)) (*http.Response, error) {
	start := time.Now()

	// 1. peek 请求体（input）
	inputBytes := peekRequestBody(req, u.cfg.CaptureMaxBytes)
	// model 优先从请求体解析（最可靠，反映实际打上游的模型）
	model := extractModelFromBody(inputBytes)

	ctx := req.Context()
	// 业务维度：req 是用 http.NewRequestWithContext(ctx,...) 创建的，ctx 来自 c.Request.Context()
	accountIDStr := strconv.FormatInt(accountID, 10)
	if v := ctx.Value(ctxkey.AccountID); v != nil {
		// 优先用 ctx 里的 accountID（更准，覆盖入参）
		if s, ok := toString(v); ok && s != "" {
			accountIDStr = s
		}
	}
	if m := ctx.Value(ctxkey.Model); m != nil {
		if s, ok := toString(m); ok && s != "" {
			// ctx.Model 是客户端请求的模型；body 里的 model 是映射后真实上游模型
			// 若两者不同，metadata 记录客户端模型，model 字段用真实上游模型
			if model == "" {
				model = s
			}
		}
	}

	traceID := uuid.NewString()
	input := decodeInput(inputBytes)
	metadata := buildRequestMetadata(ctx, accountIDStr, proxyFromReq(req), model)

	// 上报 trace-create（先于 generation，建立 trace 骨架）。
	// Input 在此填入（请求体已在手）；Output 待响应解析后用 trace-update 回填，
	// 这样 Langfuse 的 preview/overview 标签页能直接展示 input/output。
	u.client.Enqueue(Event{
		Type: "trace-create",
		Body: traceBody{
			ID:       traceID,
			Name:     "sub2api.upstream",
			UserID:   "account:" + accountIDStr,
			Input:    input,
			Metadata: metadata,
		},
	})

	// 2. 调用 inner
	resp, err := call()
	if err != nil {
		// 网络层失败：直接上报 ERROR generation（无响应体）
		u.client.Enqueue(u.buildGenerationEvent(traceID, generationInput{
			Model:         model,
			Input:         decodeInput(inputBytes),
			Output:        nil,
			Usage:         nil,
			StartTime:     start,
			EndTime:       time.Now(),
			Metadata:      metadata,
			Status:        "ERROR",
			StatusMessage: err.Error(),
			StatusCode:    0,
		}))
		return resp, err
	}

	// 3. 成功：在 trackedBody 外层包捕获 reader
	//    注意：resp.Body 此时已是 trackedBody{decompressedBody{...}}（由 httpUpstreamService 包装）。
	//    我们必须包在它之外，Close 时穿透到内层，保住 inFlight 计数。
	isStream := isSSEResponse(resp)
	contentType := resp.Header.Get("Content-Type")

	cap := &capturingReadCloser{
		inner:      resp.Body,
		maxCapture: u.cfg.CaptureMaxBytes,
		onClose: func(captured []byte) {
			gen := u.parseAndBuild(traceID, captured, isStream, contentType, generationInput{
				Model:      model,
				Input:      input,
				StartTime:  start,
				EndTime:    time.Now(),
				Metadata:   metadata,
				StatusCode: resp.StatusCode,
			})
			u.client.Enqueue(gen)
			// 回填 trace 的 Output（trace-create 时只有 Input）。
			// 使 preview/overview 标签页能直接展示完整 I/O。
			if gb, ok := gen.Body.(generationBody); ok && gb.Output != nil {
				u.client.Enqueue(Event{
					Type: "trace-update",
					Body: traceBody{
						ID:     traceID,
						Output: gb.Output,
					},
				})
			}
		},
	}
	resp.Body = cap
	return resp, nil
}

// capturingReadCloser 在 trackedBody 之外包一层，tee 每个 Read 到 buf。
// Close 时穿透关闭内层（保 inFlight），然后触发 onClose 回调（解析 + 上报）。
//
// 关键正确性：
//   - chunk 级 tee，不缓冲到 EOF → 不破坏 SSE 实时性
//   - Close 穿透 → trackedBody.onClose 仍执行一次 → 不泄漏 inFlight
type capturingReadCloser struct {
	inner      io.ReadCloser
	maxCapture int
	buf        bytes.Buffer
	mu         sync.Mutex
	closed     bool
	onClose    func(captured []byte)
}

func (c *capturingReadCloser) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	if n > 0 {
		c.mu.Lock()
		if c.buf.Len() < c.maxCapture {
			remaining := c.maxCapture - c.buf.Len()
			if n <= remaining {
				_, _ = c.buf.Write(p[:n])
			} else {
				_, _ = c.buf.Write(p[:remaining]) // 截断，超出部分不捕获
			}
		}
		c.mu.Unlock()
	}
	return n, err
}

func (c *capturingReadCloser) Close() error {
	err := c.inner.Close() // 穿透到 trackedBody，保 inFlight 计数
	c.mu.Lock()
	alreadyClosed := c.closed
	c.closed = true
	captured := append([]byte(nil), c.buf.Bytes()...) // 拷贝，回调可在异步路径用
	c.mu.Unlock()
	if !alreadyClosed && c.onClose != nil {
		// 同步执行：解析很快（gjson + 字符串拼接），不上 goroutine 避免生命周期复杂度
		c.onClose(captured)
	}
	return err
}

// ============================ 请求体 peek ============================

// peekRequestBody 读取 req.Body 副本（不超过 maxBytes），并重放 req.Body 使其可被下游再次消费。
//
// sub2api 的 buildUpstreamRequest 用 bytes.NewReader(body) 构造 req.Body，
// 实际经过 http 包后可能被包装；优先检测 io.ReadSeeker 无损重置，
// 否则读全量后用 NopCloser(bytes.Reader) 重放。
func peekRequestBody(req *http.Request, maxBytes int) []byte {
	if req == nil || req.Body == nil {
		return nil
	}
	// 快速路径：若 body 可 Seek（*bytes.Reader 即可），无损 peek 后重置
	if seeker, ok := req.Body.(io.ReadSeeker); ok {
		buf := make([]byte, maxBytes)
		n, _ := io.ReadFull(req.Body, buf)
		_, _ = seeker.Seek(0, io.SeekStart) // 重置，供下游消费
		return buf[:n]
	}
	// 慢路径：读全量（受 maxBytes 限制）后用 NopCloser 重放
	body, err := io.ReadAll(io.LimitReader(req.Body, int64(maxBytes)+1))
	if err != nil {
		logger.L().Debug("langfuse.peek_body_failed", zap.Error(err))
		return nil
	}
	if len(body) > maxBytes {
		// 超限：为保持原始语义不完整重放，这里截断捕获但丢弃（罕见路径）
		req.Body = io.NopCloser(bytes.NewReader(body))
		return body[:maxBytes]
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	return body
}

func extractModelFromBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	m := gjson.GetBytes(body, "model")
	if m.Exists() && m.Type == gjson.String {
		return m.String()
	}
	return ""
}

// ============================ 响应解析 ============================

func isSSEResponse(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
}

// generationInput 是构造 generation-create 事件的输入参数。
type generationInput struct {
	Model         string
	Input         any
	Output        any
	Usage         *usageBody
	StartTime     time.Time
	EndTime       time.Time
	Metadata      map[string]any
	Status        string // 空=DEFAULT，ERROR/WARNING/DEBUG
	StatusMessage string
	StatusCode    int
}

// parseAndBuild 解析捕获的响应字节，提取 usage/output，构造单个 generation-create 事件。
func (u *langfuseUpstream) parseAndBuild(traceID string, captured []byte, isStream bool, contentType string, in generationInput) Event {
	usage, output := parseAnthropicResponse(captured, isStream)
	in.Usage = usage
	in.Output = output

	// 状态：HTTP >= 400 标 ERROR
	if in.Status == "" {
		if in.StatusCode >= 400 {
			in.Status = "ERROR"
		}
	}
	return u.buildGenerationEvent(traceID, in)
}

// parseAnthropicResponse 从捕获的字节中提取 usage 和 output 文本。
// 兼容流式（SSE）和非流式（JSON）。
func parseAnthropicResponse(captured []byte, isStream bool) (*usageBody, any) {
	if len(captured) == 0 {
		return nil, nil
	}
	if isStream {
		return parseAnthropicSSE(captured)
	}
	return parseAnthropicJSON(captured)
}

// parseAnthropicJSON 解析非流式 Anthropic 响应。
// usage 在顶层 usage 字段；output 在 content[].text。
func parseAnthropicJSON(captured []byte) (*usageBody, any) {
	result := gjson.ParseBytes(captured)

	var usage *usageBody
	if u := result.Get("usage"); u.Exists() {
		usage = &usageBody{
			Input:         int(u.Get("input_tokens").Int()),
			Output:        int(u.Get("output_tokens").Int()),
			Total:         int(u.Get("input_tokens").Int()) + int(u.Get("output_tokens").Int()),
			Unit:          "TOKENS",
			CacheCreation: int(u.Get("cache_creation_input_tokens").Int()),
			CacheRead:     int(u.Get("cache_read_input_tokens").Int()),
		}
	}

	// 提取 content[].text 拼成 output（保持原始 content 结构更通用，但拼接文本更易读）
	var texts []string
	result.Get("content").ForEach(func(_, block gjson.Result) bool {
		if block.Get("type").String() == "text" {
			if t := block.Get("text").String(); t != "" {
				texts = append(texts, t)
			}
		}
		return true
	})
	var output any
	if len(texts) > 0 {
		output = strings.Join(texts, "\n")
	}
	return usage, output
}

// parseAnthropicSSE 解析流式 Anthropic 响应（SSE 事件流）。
//
// 关键事件：
//   - message_start: { message: { usage: { input_tokens, cache_creation_input_tokens, ... } } }
//   - message_delta: { usage: { output_tokens, ... } }
//   - content_block_delta: { delta: { text: "..." } }
func parseAnthropicSSE(captured []byte) (*usageBody, any) {
	usage := &usageBody{Unit: "TOKENS"}
	var outputText strings.Builder

	// SSE 事件以 "\n\n" 分隔，每行 "data: {...}"
	lines := strings.Split(string(captured), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		// 用 gjson 增量解析，避免反复 unmarshal
		parsed := gjson.Parse(data)
		switch parsed.Get("type").String() {
		case "message_start":
			u := parsed.Get("message.usage")
			if u.Exists() {
				usage.Input = int(u.Get("input_tokens").Int())
				usage.CacheCreation = int(u.Get("cache_creation_input_tokens").Int())
				usage.CacheRead = int(u.Get("cache_read_input_tokens").Int())
			}
		case "message_delta":
			u := parsed.Get("usage")
			if u.Exists() {
				if v := u.Get("output_tokens").Int(); v > 0 {
					usage.Output = int(v)
				}
				if v := u.Get("input_tokens").Int(); v > 0 {
					usage.Input = int(v)
				}
				if v := u.Get("cache_creation_input_tokens").Int(); v > 0 {
					usage.CacheCreation = int(v)
				}
				if v := u.Get("cache_read_input_tokens").Int(); v > 0 {
					usage.CacheRead = int(v)
				}
			}
		case "content_block_delta":
			t := parsed.Get("delta.text")
			if t.Exists() && t.Type == gjson.String {
				_, _ = outputText.WriteString(t.String())
			}
		}
	}
	usage.Total = usage.Input + usage.Output

	var output any
	if outputText.Len() > 0 {
		output = outputText.String()
	}
	return usage, output
}

// ============================ 事件构造 ============================

// traceBody 对应 trace-create / trace-update 的 body。
// trace-create 时填 Input（请求体在手）；Output 留空，待响应解析完后
// 通过 trace-update 事件回填，使 Langfuse preview/overview 标签页能直接展示 I/O。
type traceBody struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	UserID   string         `json:"userId,omitempty"`
	Input    any            `json:"input,omitempty"`
	Output   any            `json:"output,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// usageBody 是 generation 的 usage 对象。
type usageBody struct {
	Input         int    `json:"input"`
	Output        int    `json:"output"`
	Total         int    `json:"total"`
	Unit          string `json:"unit"`
	CacheCreation int    `json:"cache_creation,omitempty"`
	CacheRead     int    `json:"cache_read,omitempty"`
}

// generationBody 对应 generation-create 的 body。
type generationBody struct {
	ID            string         `json:"id"`
	TraceID       string         `json:"traceId"`
	Name          string         `json:"name,omitempty"`
	Model         string         `json:"model,omitempty"`
	StartTime     string         `json:"startTime"`
	EndTime       string         `json:"endTime"`
	Input         any            `json:"input,omitempty"`
	Output        any            `json:"output,omitempty"`
	Usage         *usageBody     `json:"usage,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Level         string         `json:"level,omitempty"` // DEFAULT/DEBUG/WARNING/ERROR
	StatusMessage string         `json:"statusMessage,omitempty"`
}

func (u *langfuseUpstream) buildGenerationEvent(traceID string, in generationInput) Event {
	body := generationBody{
		ID:        uuid.NewString(),
		TraceID:   traceID,
		Name:      "llm.upstream",
		Model:     in.Model,
		StartTime: in.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		EndTime:   in.EndTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		Input:     in.Input,
		Output:    in.Output,
		Usage:     in.Usage,
		Metadata:  in.Metadata,
		Level:     "DEFAULT",
	}
	if in.Status == "ERROR" {
		body.Level = "ERROR"
		body.StatusMessage = in.StatusMessage
	}
	if in.StatusCode > 0 {
		body.Level = mapLogLevel(in.StatusCode)
		if in.StatusCode >= 400 && body.StatusMessage == "" {
			body.StatusMessage = "HTTP " + strconv.Itoa(in.StatusCode)
		}
	}
	return Event{Type: "generation-create", Body: body}
}

func mapLogLevel(statusCode int) string {
	if statusCode >= 500 {
		return "ERROR"
	}
	if statusCode >= 400 {
		return "WARNING"
	}
	return "DEFAULT"
}

// ============================ 辅助 ============================

func buildRequestMetadata(ctx interface{ Value(any) any }, accountID, proxyURL, model string) map[string]any {
	m := map[string]any{
		"account_id": accountID,
	}
	if proxyURL != "" {
		m["proxy"] = proxyURL
	}
	if model != "" {
		m["upstream_model"] = model
	}
	// 从 ctx 捞更多业务维度（best-effort，缺失不算错）
	if v := ctx.Value(ctxkey.Group); v != nil {
		m["group"] = coerceMap(v)
	}
	if v := ctx.Value(ctxkey.IsClaudeCodeClient); v != nil {
		m["is_claude_code_client"] = v
	}
	if v := ctx.Value(ctxkey.AccountSwitchCount); v != nil {
		m["account_switch_count"] = v
	}
	if v := ctx.Value(ctxkey.RequestID); v != nil {
		if s, ok := toString(v); ok {
			m["request_id"] = s
		}
	}
	return m
}

// decodeInput 将请求体字节解码为更友好的结构（失败则原样返回字节）。
func decodeInput(body []byte) any {
	if len(body) == 0 {
		return nil
	}
	var obj any
	if err := json.Unmarshal(body, &obj); err == nil {
		return obj
	}
	return string(body)
}

func proxyFromReq(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	return req.URL.Host // 上游 host，非代理；仅作参考
}

func toString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case int64:
		return strconv.FormatInt(t, 10), true
	case int:
		return strconv.Itoa(t), true
	default:
		return "", false
	}
}

// coerceMap 把任意值转为可序列化形式（group 结构体等）。
// 简化：直接透传，json.Marshal 会处理；不可序列化的字段会被 omit。
func coerceMap(v any) any {
	return v
}
