package repository

import (
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/observability/langfuse"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// ProvideLangfuseConfig 加载 Langfuse 配置（从 LANGFUSE_* 环境变量）。
// 不侵入 config.Config 结构体，独立加载，保证升级无感。
func ProvideLangfuseConfig() *langfuse.Config {
	return langfuse.LoadConfig()
}

// ProvideLangfuseClient 创建 Langfuse 上报客户端。
// 仅在配置可用时创建（启动后台 flush goroutine）；否则返回 nil。
func ProvideLangfuseClient(cfg *langfuse.Config) *langfuse.Client {
	if cfg == nil || !cfg.Usable() {
		return nil
	}
	return langfuse.NewClient(cfg)
}

// ProvideHTTPUpstream 是 NewHTTPUpstream 的装饰包装。
//
// 设计：替换 ProviderSet 中的 NewHTTPUpstream，使所有依赖 service.HTTPUpstream 的
// 服务自动获得 Langfuse 装饰。配置未启用时透传原始实现，零开销。
//
// 这是本方案唯一的 wire 注入点；升级 rebase 时此处为新增行，冲突概率极低。
func ProvideHTTPUpstream(cfg *config.Config, lfCfg *langfuse.Config, lfClient *langfuse.Client) service.HTTPUpstream {
	base := NewHTTPUpstream(cfg)
	if lfCfg == nil || !lfCfg.Usable() || lfClient == nil {
		return base // 未启用 trace，透传原始实现
	}
	return langfuse.NewDecorator(base, lfClient, lfCfg)
}
