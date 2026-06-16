# Sub2API 接入自建 Langfuse（LLM Call Trace）

本方案在不侵入业务逻辑的前提下，通过装饰 `HTTPUpstream` 接口，把每一次**上游 LLM 调用**（含 OAuth 订阅账号直连 Anthropic）的 trace 上报到自建 Langfuse。

上报内容：model、input（完整 prompt）、output（完整 completion）、token usage、latency、account/proxy/group 维度、HTTP 状态、失败原因。**不是 session trace，是每次上游调用一条 generation。**

---

## 1. 工作原理（30 秒理解）

```
Claude Code ──► Caddy ──► sub2api ──► [langfuseUpstream 装饰器] ──► Anthropic
                                  │              │ tee 捕获 req/resp
                                  │              ▼
                                  │      解析 SSE usage + 拼接 output
                                  │              │
                                  │              ▼ 异步队列
                                  └─────► POST {langfuse}/api/public/ingestion ──► Langfuse
```

- 装饰器包在 `trackedBody` **之外**，`Close()` 穿透 → 不泄漏 `inFlight`，连接池正常回收。
- chunk 级 tee，不缓冲到 EOF → **不破坏 SSE 实时性**。
- 异步队列（默认 4096），满则丢弃 + 计数 → **绝不阻塞请求热路径**。
- `LANGFUSE_ENABLED=false` 时 wire 直接返回原始 `HTTPUpstream`，零开销。

---

## 2. Fork + 镜像发布（一次性配置）

### 2.1 Fork 仓库
在 GitHub 上 Fork `Wei-Shaw/sub2api`，然后把本分支（含 langfuse 装饰器改动）作为 `langfuse-patch` 分支推上去。

### 2.2 启用 GitHub Actions
Fork 后进入 **Settings → Actions → General**，确保：
- Workflow permissions = **Read and write permissions**（推镜像、强推分支需要）。
- 允许 `GITHUB_TOKEN` 写 packages（默认开启）。

### 2.3 触发首次构建
两个 workflow 已就位：
- `fork-rebase.yml`：每日 UTC 02:00 自动把 `langfuse-patch` rebase 到上游最新 tag。冲突时自动开 issue。
- `fork-release.yml`：`langfuse-patch` 被 push 时自动构建多架构镜像，推到 `ghcr.io/<你的用户名>/sub2api:latest`。

手动触发 `fork-release.yml`（Actions 页面 → Run workflow）即可产出第一个镜像。

---

## 3. 部署 Langfuse（自建）

在 sub2api 同一套 compose 里加一个 Langfuse 服务。最小可用配置：

```yaml
# docker-compose.yml 追加
services:
  langfuse:
    image: langfuse/langfuse:latest
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@langfuse-db:5432/langfuse
      - NEXTAUTH_SECRET=your-nextauth-secret          # openssl rand -base64 32
      - SALT=your-salt                                 # openssl rand -base64 32
      - NEXTAUTH_URL=http://localhost:3000
      - TELEMETRY_ENABLED=false
    depends_on: [langfuse-db]

  langfuse-db:
    image: postgres:15-alpine
    restart: unless-stopped
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=langfuse
    volumes:
      - langfuse_db:/var/lib/postgresql/data

volumes:
  langfuse_db:
```

启动后访问 `http://localhost:3000`，注册账号 → 新建 Organization → 新建 Project → 在 **Project Settings → API Keys** 创建一组 key，得到：
- **Public Key**（`pk-lf-...`）
- **Secret Key**（`sk-lf-...`）

---

## 4. 让 sub2api 用上 fork 镜像 + 开启 trace

修改 sub2api 的 `docker-compose.yml`：

```yaml
services:
  sub2api:
    image: ghcr.io/<你的用户名>/sub2api:latest   # ← 从 weishaw/sub2api:latest 改成你的 fork
    environment:
      # ... 原有配置不动 ...

      # ===== Langfuse LLM Call Trace =====
      - LANGFUSE_ENABLED=true
      - LANGFUSE_HOST=http://langfuse:3000        # 容器内互访；若分机部署用实际地址
      - LANGFUSE_PUBLIC_KEY=pk-lf-xxxxxxxx
      - LANGFUSE_SECRET_KEY=sk-lf-xxxxxxxx
      # 可选调参（不设用默认值）
      # - LANGFUSE_FLUSH_INTERVAL_MS=1000
      # - LANGFUSE_BATCH_SIZE=50
      # - LANGFUSE_CAPTURE_MAX_BYTES=8388608      # 单响应最大捕获字节，默认 8MiB
      # - LANGFUSE_QUEUE_SIZE=4096

      # ===== 让后台"检查更新"指向你的 fork（可选）=====
      # 不设则检查官方仓库；设了显示你 fork 的版本
      - UPDATE_GITHUB_REPO=<你的用户名>/sub2api
```

然后：
```bash
docker compose pull sub2api
docker compose up -d
```

发几个请求，几秒后到 Langfuse 的 **Tracing** 页面就能看到每次调用的 generation（带 model / token / input / output）。

---

## 5. 升级流程（核心诉求：跟进上游）

```
上游发新 tag
   │
   ▼
fork-rebase.yml 自动 rebase langfuse-patch（每日 / 手动）
   ├── 成功 → 强推 → 触发 fork-release.yml → 新镜像 ghcr.io/.../sub2api:latest
   └── 冲突 → 开 issue 通知你手动解（通常只在 wire_gen.go / update_service.go）
            │
            ▼
你在机器上：docker compose pull sub2api && docker compose up -d
```

- **零手动编译、零手动 rebase**（CI 全包）。
- 冲突时 CI 会开 issue 列出文件，改动面只有 ~4 处，通常 5 分钟内解决。
- 降级：`docker compose pull` 回旧 tag，或 `LANGFUSE_ENABLED=false` 重启即可完全关闭 trace。

---

## 6. 配置项参考

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `LANGFUSE_ENABLED` | `false` | 总开关。`false` 时装饰器不生效，零开销 |
| `LANGFUSE_HOST` | （空） | 自建 Langfuse 地址，带 scheme，不带尾斜杠 |
| `LANGFUSE_PUBLIC_KEY` | （空） | Project public key（`pk-lf-...`） |
| `LANGFUSE_SECRET_KEY` | （空） | Project secret key（`sk-lf-...`） |
| `LANGFUSE_FLUSH_INTERVAL_MS` | `1000` | 后台批量 flush 间隔（毫秒） |
| `LANGFUSE_BATCH_SIZE` | `50` | 单次 POST 最多打包事件数 |
| `LANGFUSE_CAPTURE_MAX_BYTES` | `8388608` | 单个响应最大捕获字节，超出截断 |
| `LANGFUSE_QUEUE_SIZE` | `4096` | 异步队列容量，满则丢弃（保护热路径） |
| `LANGFUSE_REQUEST_TIMEOUT` | `10s` | 单次 ingestion HTTP 请求超时 |
| `UPDATE_GITHUB_REPO` | `Wei-Shaw/sub2api` | 后台"检查更新"查询的仓库，设为你的 fork |

> **安全提醒**：本方案记录完整 input/output 文本。自建 Langfuse 的库会存储全部 prompt/completion，请确保该库加密落盘 + 严格的访问控制。

---

## 7. 上报字段说明

每个上游调用在 Langfuse 里产生：
- **1 条 Trace**：`name=sub2api.upstream`，`userId=account:<id>`，metadata 带 account_id/group/request_id 等。
- **1 条 Generation**（挂在 trace 下）：model、startTime/endTime、input、output、usage（input/output/total + cache 明细）、level（DEFAULT/WARNING/ERROR）、statusMessage。

失败的上游调用（含 failover 中途失败的账号）level=ERROR，statusMessage 带错误信息。每次上游尝试独立一条 generation，能看到"某账号 503 触发切换"的链路。

---

## 8. 改动文件清单（Fork 相对上游的 diff）

| 文件 | 类型 | 作用 |
|---|---|---|
| `backend/internal/observability/langfuse/config.go` | 新增 | 配置 + 环境变量 |
| `backend/internal/observability/langfuse/client.go` | 新增 | ingestion 客户端（异步/批量/重试） |
| `backend/internal/observability/langfuse/upstream_decorator.go` | 新增 | 装饰器（tee 捕获 + SSE 解析） |
| `backend/internal/observability/langfuse/upstream_decorator_test.go` | 新增 | 单测 |
| `backend/internal/repository/langfuse_wire.go` | 新增 | wire provider |
| `backend/internal/repository/wire.go` | 改 | ProviderSet 注册 |
| `backend/cmd/server/wire_gen.go` | 改 | 注入装饰器 + cleanup |
| `backend/cmd/server/wire_gen_test.go` | 改 | 测试签名对齐 |
| `backend/internal/service/update_service.go` | 改 | `UPDATE_GITHUB_REPO` 覆盖 |
| `.github/workflows/fork-rebase.yml` | 新增 | 自动 rebase |
| `.github/workflows/fork-release.yml` | 新增 | 构建推 ghcr 镜像 |

升级时 rebase 冲突几乎只会落在 `wire_gen.go`（官方若新增 provider，行号会变）——这是纯机械冲突，接受任意一方后补齐即可。
