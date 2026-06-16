// Package langfuse 提供非侵入式的 LLM Call trace 上报能力。
//
// 设计目标：
//   - 通过装饰 service.HTTPUpstream 接口，在上游调用处透明捕获请求/响应
//   - 不修改任何 handler / service 业务逻辑，仅通过 wire 包一层装饰器
//   - 上报失败绝不影响业务热路径（异步队列、丢弃 + 计数）
//
// 数据流向：
//
//	sub2api 上游调用 ─► langfuseUpstream（装饰器）
//	                       ├─ tee 捕获 req/resp body
//	                       ├─ 解析 Anthropic SSE usage
//	                       └─ Enqueue 到 Client（异步）
//	                                └─ POST {host}/api/public/ingestion ─► 自建 Langfuse
package langfuse

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// 默认值：在环境变量未设置时使用
	defaultFlushIntervalMs = 1000            // 1 秒 flush 一次
	defaultBatchSize       = 50              // 每批最多 50 个事件
	defaultCaptureMaxBytes = 8 * 1024 * 1024 // 8 MiB，与 sub2api 非流式响应读取上限一致
	defaultQueueSize       = 4096            // 异步队列容量
	defaultRequestTimeout  = 10 * time.Second
)

// Config 是 Langfuse 上报的配置。
//
// 全部通过 LANGFUSE_* 环境变量读取，不侵入 config.Config 结构体，
// 这样升级时 sub2api 的配置结构变更不会影响本包。
type Config struct {
	// Enabled 是否启用 trace 上报。false 时 wire 直接返回原始 HTTPUpstream，零开销。
	Enabled bool
	// Host 自建 Langfuse 地址，如 https://langfuse.example.com（不带尾斜杠）。
	Host string
	// PublicKey Langfuse project public key（pk-lf-...）。
	PublicKey string
	// SecretKey Langfuse project secret key（sk-lf-...）。
	SecretKey string

	// FlushIntervalMs 后台 flush 间隔（毫秒）。
	FlushIntervalMs int
	// BatchSize 单次 POST 最多打包的事件数。
	BatchSize int
	// CaptureMaxBytes 单个响应体最大捕获字节数，超出截断。
	CaptureMaxBytes int
	// QueueSize 异步队列容量，满则丢弃。
	QueueSize int
	// RequestTimeout 单次 ingestion HTTP 请求超时。
	RequestTimeout time.Duration
}

// Enabled 校验配置是否完整可用。
// Enabled=true 但缺少必要字段视为不可用（降级为不上报）。
func (c *Config) Usable() bool {
	if c == nil {
		return false
	}
	return c.Enabled &&
		c.Host != "" &&
		c.PublicKey != "" &&
		c.SecretKey != ""
}

// LoadConfig 从环境变量加载配置。
// 在 wire provider 中调用一次。
func LoadConfig() *Config {
	c := &Config{
		Enabled:         boolEnv("LANGFUSE_ENABLED", false),
		Host:            strings.TrimRight(os.Getenv("LANGFUSE_HOST"), "/"),
		PublicKey:       os.Getenv("LANGFUSE_PUBLIC_KEY"),
		SecretKey:       os.Getenv("LANGFUSE_SECRET_KEY"),
		FlushIntervalMs: intEnv("LANGFUSE_FLUSH_INTERVAL_MS", defaultFlushIntervalMs),
		BatchSize:       intEnv("LANGFUSE_BATCH_SIZE", defaultBatchSize),
		CaptureMaxBytes: intEnv("LANGFUSE_CAPTURE_MAX_BYTES", defaultCaptureMaxBytes),
		QueueSize:       intEnv("LANGFUSE_QUEUE_SIZE", defaultQueueSize),
		RequestTimeout:  durationEnv("LANGFUSE_REQUEST_TIMEOUT", defaultRequestTimeout),
	}
	return c
}

func boolEnv(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func intEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func durationEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	// 支持 "10s" / "500ms" 这类
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	// 兼容纯数字（毫秒）
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Millisecond
	}
	return def
}
