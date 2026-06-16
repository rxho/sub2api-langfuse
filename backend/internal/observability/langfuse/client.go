package langfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Event 是 Langfuse ingestion API 的单条事件信封。
// type IngestionBatch 包装多个 Event 一次 POST。
type Event struct {
	// ID 事件唯一 ID（UUID），服务端用于去重。
	ID string `json:"id"`
	// Type 事件类型：trace-create / generation-create / span-create / event-create / score-create。
	Type string `json:"type"`
	// Timestamp 事件发生时间（ISO-8601）。
	Timestamp string `json:"timestamp"`
	// Body 事件体（具体结构随 Type 变化，直接用任意 JSON 结构）。
	Body any `json:"body"`
}

// ingestionBatch 是 POST /api/public/ingestion 的请求体。
type ingestionBatch struct {
	Batch    []Event    `json:"batch"`
	Metadata *batchMeta `json:"metadata,omitempty"`
}

type batchMeta struct {
	BatchSize int `json:"batch_size"`
}

// Client 是 Langfuse ingestion API 的异步批量客户端。
//
// 线程安全：调用方只需调用 Enqueue，内部后台 goroutine 负责 flush。
// 故障隔离：队列满 / HTTP 失败均丢弃 + 计数，绝不 panic、绝不阻塞调用方。
type Client struct {
	host     string
	auth     string // "Basic base64(pk:sk)" 的值部分（不含 "Basic " 前缀也是完整 header value）
	httpC    *http.Client
	endpoint string // {host}/api/public/ingestion

	batchSize int
	queue     chan Event

	done    chan struct{}
	wg      sync.WaitGroup
	stopMu  sync.Mutex
	stopped atomic.Bool

	// 可观测计数（仅日志输出，不接入 metrics 以免增加耦合）
	droppedTotal atomic.Uint64
	sentTotal    atomic.Uint64
	failedTotal  atomic.Uint64
}

// NewClient 创建并启动后台 flush goroutine。
// 调用方应在进程退出时调用 Close 优雅停止。
func NewClient(cfg *Config) *Client {
	c := &Client{
		host:      cfg.Host,
		auth:      basicAuth(cfg.PublicKey, cfg.SecretKey),
		httpC:     &http.Client{Timeout: cfg.RequestTimeout},
		endpoint:  cfg.Host + "/api/public/ingestion",
		batchSize: cfg.BatchSize,
		queue:     make(chan Event, cfg.QueueSize),
		done:      make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop(time.Duration(cfg.FlushIntervalMs) * time.Millisecond)
	return c
}

// Enqueue 非阻塞入队。队列满则丢弃并计数。
// 必须永不阻塞调用方（热路径）。
func (c *Client) Enqueue(events ...Event) {
	if c == nil || c.stopped.Load() {
		return
	}
	for _, e := range events {
		if e.ID == "" {
			e.ID = uuid.NewString()
		}
		if e.Timestamp == "" {
			e.Timestamp = nowISO()
		}
		select {
		case c.queue <- e:
		default:
			c.droppedTotal.Add(1)
		}
	}
}

// Close 停止后台 goroutine 并等待剩余 flush（最多等待 5 秒）。
func (c *Client) Close() {
	c.stopMu.Lock()
	defer c.stopMu.Unlock()
	if c.stopped.Swap(true) {
		return // 已停止
	}
	close(c.done)
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}

func (c *Client) flushLoop(interval time.Duration) {
	defer c.wg.Done()
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([]Event, 0, c.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		out := make([]Event, len(batch))
		copy(out, batch)
		batch = batch[:0]
		c.post(context.Background(), out)
	}

	for {
		select {
		case <-c.done:
			// drain 剩余
			for {
				select {
				case e := <-c.queue:
					batch = append(batch, e)
					if len(batch) >= c.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case <-ticker.C:
			flush()
		case e := <-c.queue:
			batch = append(batch, e)
			if len(batch) >= c.batchSize {
				flush()
			}
		}
	}
}

// post 发送一批事件，失败重试一次（指数退避）。
func (c *Client) post(ctx context.Context, batch []Event) {
	payload := ingestionBatch{
		Batch:    batch,
		Metadata: &batchMeta{BatchSize: len(batch)},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		// 不应发生（结构已知），记录后丢弃
		logger.L().Warn("langfuse.marshal_failed", zap.Int("batch_size", len(batch)), zap.Error(err))
		c.failedTotal.Add(uint64(len(batch)))
		return
	}

	if c.doPost(ctx, body, 0) {
		c.sentTotal.Add(uint64(len(batch)))
		return
	}
	// 重试一次
	backoff := 500 * time.Millisecond
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		c.failedTotal.Add(uint64(len(batch)))
		return
	}
	if c.doPost(ctx, body, 1) {
		c.sentTotal.Add(uint64(len(batch)))
		return
	}
	c.failedTotal.Add(uint64(len(batch)))
}

func (c *Client) doPost(ctx context.Context, body []byte, attempt int) bool {
	reqCtx, cancel := context.WithTimeout(ctx, c.httpC.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.auth)

	resp, err := c.httpC.Do(req)
	if err != nil {
		if attempt == 0 {
			logger.L().Debug("langfuse.post_failed_retrying", zap.Error(err))
		} else {
			logger.L().Warn("langfuse.post_failed", zap.Error(err), zap.Int("attempt", attempt))
		}
		return false
	}
	// 丢弃响应体（ingestion 响应对业务无意义）
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true
	}
	if attempt == 0 {
		logger.L().Debug("langfuse.post_non2xx_retrying", zap.Int("status", resp.StatusCode))
	} else {
		logger.L().Warn("langfuse.post_non2xx", zap.Int("status", resp.StatusCode), zap.Int("attempt", attempt))
	}
	return false
}

// basicAuth 返回 "Basic <base64(pk:sk)>"。
func basicAuth(publicKey, secretKey string) string {
	// 直接用 net/http 的基础鉴权工具，避免手动 base64
	// 这里手写以减少 import，但 net/http 已是依赖，复用更稳妥：
	req := &http.Request{Header: http.Header{}}
	req.SetBasicAuth(publicKey, secretKey)
	return req.Header.Get("Authorization")
}

func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}
