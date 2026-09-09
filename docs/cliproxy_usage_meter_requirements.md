# cliproxy Usage Meter Sidecar - 探索/MVP需求文档

更新时间：2026-08-17

## 1. 背景

CLIProxyAPI 可以为 Codex / OpenAI-compatible 客户端提供代理能力。一个常见部署会有多个 Codex 订阅账号，shell alias 采用数字命名：`codex-1`、`codex-2`、...，每个 alias 对应独立 `CODEX_HOME`。

现状判断：当前本机 `cliproxyapi` 未发现开箱即用的跨 session / 跨订阅账号 token、价格、额度统计插件。因此采用 **方案 A：在 cliproxyapi 前面加一个本地轻量 sidecar proxy**。

目标链路：

```text
Codex / OpenClaw / curl / 其他 OpenAI-compatible client
    -> http://127.0.0.1:<meter_port>/v1/...
    -> cliproxy_usage_meter sidecar 记录 usage/cost/quota/call count
    -> http://127.0.0.1:8317/v1/...
    -> 原样返回客户端
```

## 2. 总体目标

实现一个最小可用、本地运行、安全脱敏的 usage-meter sidecar，用于跨 session / 跨订阅账号统计：

1. token 消耗
2. API 等价估算价格
3. 每账号额度周期内的 API 等价满额度估计
4. 总调用次数 / 成功调用 / 失败调用 / streaming 调用
5. 按账号、alias、模型、session、日期聚合
6. 简单 HTML 看板
7. CLI 查询与手动标记 reset / quota-hit

## 3. 安全边界与稳定性约束

硬要求：

- 不修改 `cliproxyapi` 源码。
- 不重启 `cliproxyapi`。
- 不停止 `cliproxyapi`。
- 不修改 `~/.cli-proxy-api/config.yaml`。
- 不修改用户 shell 配置或现有 `codex-1..N` alias。
- 不占用、不替换 `cliproxyapi` 现有端口 `8317`。
- sidecar 使用独立端口，例如 `8327`。
- 现有项目继续直连 `http://127.0.0.1:8317/v1/...`，不得被改到 sidecar。
- 测试优先使用 fake upstream；真实联调只允许显式测试请求走 `8327`，不得改变任何现有项目的 `base_url`。
- 不删除、不改写 `~/.cli-proxy-api/*.cds` cooldown 文件；如需参考只能只读扫描。
- 不打印、不落盘、不泄露：
  - `access_token`
  - `refresh_token`
  - `id_token`
  - `Authorization` header 原文
  - API key 原文
- 如果需要识别账号，只能保存 hash / 尾号 / 脱敏标识。
- 默认只在 workspace 内新增脚本、文档、SQLite 数据库。
- 如需让 Codex 走 meter，只给可选 patch/说明，不自动修改配置。
- 价格未知时宁可 `NULL`，不要伪精确。
- 透明代理优先，不得为了统计破坏正常请求或 streaming。

稳定性解释：

```text
现有项目:
client -> http://127.0.0.1:8317/v1/... -> cliproxyapi

新测试链路:
test client -> http://127.0.0.1:8327/v1/... -> usage-meter -> http://127.0.0.1:8317/v1/... -> cliproxyapi
```

只要不修改现有客户端 `base_url`，现有项目不会经过 sidecar，因此 sidecar 开发/测试不应影响 cliproxyapi 稳定性。真实联调请求会消耗少量账号额度，所以应尽量用 fake upstream，自测通过后再做极少量真实请求。

## 4. 工作目录与环境

工作目录：

```text
<repo>
```

Python 优先使用：

```text
python3
```

长时间命令必须放 tmux 前台运行。

建议文件：

```text
scripts/cliproxy_usage_meter.py
scripts/start_cliproxy_usage_meter.sh
docs/cliproxy_usage_meter.md
docs/cliproxy_usage_meter_requirements.md
datas/cliproxy_usage.sqlite
```

## 5. 侦察任务

实现前先只读侦察：

1. 查看 `~/.cli-proxy-api/config.yaml`。
2. 查看 `cliproxyapi` 进程、端口、版本。
3. 读取用户 shell 配置中的 `codex-1..N` alias 到 `CODEX_HOME` 的映射。
4. 查看各 `~/.codex-*/auth.json` 是否能提取 `account_id`，但不要输出 token。
5. 从现有 `~/.cli-proxy-api/logs/` 中采样请求结构，确认 Codex CLI 是否带有：
   - `client_metadata.session_id`
   - `client_metadata.thread_id`
   - `client_metadata.turn_id`
   - `x-codex-installation-id`
   - `x-codex-window-id`
6. 所有日志采样必须脱敏。

## 6. 请求识别策略

优先级：

1. 从 request JSON 的 `client_metadata` 读取：
   - `session_id`
   - `thread_id`
   - `turn_id`
   - `x-codex-installation-id`
   - `x-codex-window-id`
2. 从可选 header 读取：
   - `X-Usage-Alias`
   - `X-Usage-Session`
   - `X-Usage-Project`
3. 从 `Authorization: Bearer ...` 计算短 hash：
   - `auth_fingerprint = sha256(token)[:16]`
   - 不保存 token 原文。
4. 如果能将 token / account 映射到 `~/.cli-proxy-api/codex-*.json` 或 `~/.codex-*/auth.json`，只保存：
   - `account_id_hash`
   - `account_id_tail`
   - `usage_alias`
   - 不保存完整敏感字段。

## 7. 需要支持的 endpoint

MVP 必须支持：

- `/v1/responses`
- `/v1/chat/completions`
- 其他 `/v1/...` 路径透明转发
- `/usage/timeline` 返回只读、每分钟的 HTTP 200/非 200 连续时间轴 JSON

行为要求：

- non-streaming：解析响应 JSON 的 `usage` 字段。
- streaming：第一版可以原样流式转发，尽量从最终 chunk 解析 usage；若没有 usage，记录 `usage_missing=true`，但不得破坏流式响应。

## 8. Token 字段归一化

### 8.1 Responses API

可能字段：

```text
usage.input_tokens
usage.output_tokens
usage.total_tokens
usage.input_tokens_details.cached_tokens
usage.output_tokens_details.reasoning_tokens
```

归一化为：

```text
input_tokens
output_tokens
cached_tokens
reasoning_tokens
total_tokens
```

### 8.2 Chat Completions API

可能字段：

```text
usage.prompt_tokens
usage.completion_tokens
usage.total_tokens
usage.prompt_tokens_details.cached_tokens
usage.completion_tokens_details.reasoning_tokens
```

归一化为：

```text
input_tokens = prompt_tokens
output_tokens = completion_tokens
cached_tokens
reasoning_tokens
total_tokens
```

## 9. 总调用次数统计

必须记录每一次经过 sidecar 的调用，包括成功、失败、usage 缺失、streaming、非 streaming。

记录层同时区分 API 调用与账号尝试：如果网关在选择订阅账号前就拒绝请求（例如没有可识别
账号选择器的 401），该响应仍须进入 API 响应观测，但不得占用最近账号尝试列表或账号成功/失败计数。

每条请求至少记录：

- `call_count = 1`，聚合时求和即可。
- `ok`：成功为 1，失败为 0。
- `status_code`。
- `stream`：是否 streaming。
- `usage_missing`：是否未拿到 usage。
- `error_type` / `error_message_redacted`。

HTML dashboard 必须展示：

1. 今日总调用次数。
2. 今日成功调用次数。
3. 今日失败调用次数。
4. 今日 streaming 调用次数。
5. 近 7 日总调用次数。
6. 按账号 / alias 调用次数排行。
7. 按模型调用次数排行。
8. 最近 50 次账号尝试；账号选择前的网关拒绝只保留在调用/失败历史，不占用列表名额，也不进入 API 响应时间轴。

## 10. SQLite Schema 建议

### 10.1 usage_events

```sql
CREATE TABLE IF NOT EXISTS usage_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  endpoint TEXT,
  method TEXT,
  model TEXT,
  status_code INTEGER,
  ok INTEGER,
  duration_ms INTEGER,
  stream INTEGER,
  session_id TEXT,
  thread_id TEXT,
  turn_id TEXT,
  installation_id TEXT,
  window_id TEXT,
  usage_alias TEXT,
  usage_project TEXT,
  auth_fingerprint TEXT,
  account_id_hash TEXT,
  account_id_tail TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cached_tokens INTEGER,
  reasoning_tokens INTEGER,
  total_tokens INTEGER,
  estimated_api_cost_usd REAL,
  subscription_amortized_cost_usd REAL,
  api_equivalent_quota_usd REAL,
  usage_missing INTEGER,
  error_type TEXT,
  error_message_redacted TEXT,
  request_bytes INTEGER,
  response_bytes INTEGER,
  call_count INTEGER DEFAULT 1,
  account_attempt INTEGER NOT NULL DEFAULT 1
);
```

### 10.2 model_prices

```sql
CREATE TABLE IF NOT EXISTS model_prices (
  model_pattern TEXT PRIMARY KEY,
  input_per_million REAL,
  output_per_million REAL,
  cached_input_per_million REAL,
  reasoning_per_million REAL,
  currency TEXT,
  source_note TEXT,
  updated_at TEXT
);
```

### 10.3 quota_events

```sql
CREATE TABLE IF NOT EXISTS quota_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  account_id_hash TEXT,
  account_id_tail TEXT,
  usage_alias TEXT,
  event_type TEXT,
  source TEXT,
  raw_message_redacted TEXT
);
```

`event_type` 示例：

```text
quota_hit
cooldown_hit
usage_limit_hit
rate_limit_hit
reset_detected
manual_reset
manual_quota_hit
```

### 10.4 account_quota_cycles

```sql
CREATE TABLE IF NOT EXISTS account_quota_cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id_hash TEXT,
  account_id_tail TEXT,
  usage_alias TEXT,
  cycle_start_ts TEXT,
  cycle_end_ts TEXT,
  reset_detected_by TEXT,
  quota_hit_detected_by TEXT,
  total_calls INTEGER,
  successful_calls INTEGER,
  failed_calls INTEGER,
  streaming_calls INTEGER,
  total_input_tokens INTEGER,
  total_cached_tokens INTEGER,
  total_output_tokens INTEGER,
  total_reasoning_tokens INTEGER,
  total_tokens INTEGER,
  estimated_api_cost_usd REAL,
  observed_floor_usd REAL,
  api_equivalent_quota_usd REAL,
  is_complete_cycle INTEGER,
  notes TEXT
);
```

## 11. API 等价成本计算

`estimated_api_cost_usd` 用模型价格表估算：

```text
non_cached_input_tokens = max(input_tokens - cached_tokens, 0)

estimated_api_cost_usd =
  non_cached_input_tokens / 1_000_000 * input_price
  + cached_tokens / 1_000_000 * cached_input_price
  + output_tokens / 1_000_000 * output_price
```

注意：

- 如果价格未知，`estimated_api_cost_usd = NULL`。
- `reasoning_tokens` 默认只作为 output 子拆分记录，不单独重复计费，除非价格表明确配置。
- `gpt-5.x` / `codex` / `gpt-5.6-sol` 如果没有公开可信价格，第一版不要硬编码假价格。
- 可以预留 configurable pricing profile：
  - `official`
  - `conservative`
  - `standard`
  - `aggressive`

## 12. 每账号“满额度 USD”估计

必须命名为：

```text
api_equivalent_quota_usd
```

不要命名为真实 `quota_usd`，避免误读。它表示“按 API 价格折算的等价额度”，不是 OpenAI 官方真实美元余额。

### 12.1 单周期估计

对于某个账号：

```text
api_equivalent_quota_usd =
  从 reset/cycle_start 后第一条成功请求
  到首次 quota/cooldown/usage-limit 事件前
  所有请求 estimated_api_cost_usd 总和
```

如果周期内没有观测到 quota / cooldown / usage-limit，则只能输出：

```text
observed_floor_usd = 当前累计 estimated_api_cost_usd
```

含义：该账号当前周期至少已经消耗了这么多 API 等价美元，但尚未确认打满。

### 12.2 多周期统计

Dashboard / CLI 应支持历史周期统计：

- min
- max
- p20
- p50 / median
- p80
- 最近一次完整周期
- 当前周期 observed_floor_usd

示例输出：

```text
codex-3:
  current_cycle_observed_floor_usd: $18.42
  last_complete_cycle_api_equivalent_quota_usd: $31.20
  historical_p50_api_equivalent_quota_usd: $30.70
  historical_p20_p80: $28.90 - $34.10
```

## 13. Quota / Reset 识别

### 13.1 quota-hit 自动识别

从 response error message / status / cliproxyapi 行为中识别关键词：

```text
usage limit
quota
rate limited
temporarily rate-limited
cooldown
insufficient_quota
limit reached
额度
```

注意：

- `auth_unavailable` 要谨慎，可能是 token / auth 问题，不一定是 quota 打满。
- 所有原始错误消息必须脱敏保存。

### 13.2 cooldown 文件参考

可选扫描：

```text
~/.cli-proxy-api/*.cds
```

作为 cooldown-hit 参考，不要删除或修改这些文件。

### 13.3 reset 识别

reset 事件来源：

1. 手工 CLI 标记：`--mark-reset <alias>`。
2. `.cds` 被清理后账号重新成功请求。
3. 账号 quota error 后重新成功请求。
4. 用户执行官方 reset 后的 cooldown 清理脚本时，可手动记录 reset。

## 14. CLI 要求

`scripts/cliproxy_usage_meter.py` 至少支持：

```bash
--serve
--port 8327
--upstream http://127.0.0.1:8317
--db datas/cliproxy_usage.sqlite
--summary today
--summary 7d
--by-account 7d
--by-model 7d
--recent 20
--quota-summary 30d
--quota-summary-by-account
--mark-reset <alias>
--mark-quota-hit <alias>
```

查询示例：

```bash
python3 scripts/cliproxy_usage_meter.py --summary today
python3 scripts/cliproxy_usage_meter.py --quota-summary 30d
```

## 15. HTML Dashboard 要求

HTML dashboard **不集成进 cliproxyapi 本体**，也不修改 cliproxyapi 源码。它由 `cliproxy_usage_meter` sidecar 单独提供。

推荐访问方式：

```text
http://127.0.0.1:8327/usage
或
http://127.0.0.1:8327/__usage
```

也就是说：

```text
cliproxyapi:          http://127.0.0.1:8317/v1/...
usage-meter sidecar: http://127.0.0.1:8327/v1/...   # 代理 API 请求
usage dashboard:     http://127.0.0.1:8327/usage    # 单独看板
```

必须展示：

### 15.1 今日概览

- 今日总调用次数
- 今日成功调用次数
- 今日失败调用次数
- 今日 streaming 调用次数
- 今日 input tokens
- 今日 output tokens
- 今日 cached tokens
- 今日 reasoning tokens
- 今日 total tokens
- 今日 estimated API cost USD

### 15.2 近 7 日概览

- 近 7 日总调用次数
- 近 7 日总 tokens
- 近 7 日 estimated API cost USD

### 15.3 按账号 / alias 聚合

- alias
- account tail / hash
- calls
- successful calls
- failed calls
- tokens
- estimated API cost USD
- current cycle observed_floor_usd
- last complete cycle api_equivalent_quota_usd

### 15.4 按模型聚合

- model
- calls
- tokens
- estimated API cost USD
- usage_missing count

### 15.5 API 响应时间轴

- 同一张折线图展示精确 HTTP 200 与非 200 调用次数，不使用柱状图。
- 聚合精度为每分钟，空分钟补 0；支持近 1、6、24 小时切换。
- 固定合并 Sub2API、Cockpit Tools、8327 sidecar 和 8317 usage queue 中已进入账号选择阶段的 API，不提供来源筛选；账号选择前的网关拒绝排除在外。
- 响应分钟使用不含账号信息的独立观测，账号明细退役不得删除或改变历史响应曲线。
- 8327 上游连接失败/超时产生的 502 计入非 200；页面明确说明“无请求”不能等同于“服务正常”。
- 图表适配现有明暗主题、移动端、键盘浏览和悬停明细。

### 15.6 quota 看板

- 每账号当前周期累计 tokens / estimated USD
- 是否已打满
- 上次打满时估算 USD
- 历史周期 p50 / p20-p80
- 当前周期 observed_floor_usd

### 15.7 最近账号尝试

最近 50 次账号尝试（已到达订阅账号选择阶段）：

- ts
- Sub2API session_id 的末 4 位字符（完整 ID 不落库；缺失显示 `—`）
- alias/account
- model
- 采集来源；Sub2API/本地成功用量显示推定“成功”，真实 HTTP 来源显示状态码
- 计费档保留实际服务等级、请求等级和等级来源
- endpoint
- status
- duration_ms
- stream
- total_tokens
- estimated cost
- error redacted

页面简单即可，不引入复杂前端。

## 16. 测试要求

必须做最小自测：

1. 用 fake upstream 启本地测试服务器，返回带 usage 的 JSON。
2. 测 `/v1/responses` non-streaming 能记录 usage。
3. 测 `/v1/chat/completions` non-streaming 能记录 usage。
4. 测未知 usage 时 `usage_missing=true`。
5. 测失败响应能记录调用次数与错误，但错误脱敏。
6. 测 `Authorization` header 不落库、不打印，只保存 hash。
7. 如实现 streaming，至少测试 SSE 透传不被破坏。
8. 测 HTML dashboard 可访问并显示总调用次数。
9. 测 `/usage/timeline` 空分钟补零、精确 200/非 200 分类、账号选择前拒绝排除、多 API 来源合并与账号退役后曲线保留。
10. 测 CLI summary / recent / by-account / by-model / quota-summary。

## 17. 启动方式建议

```bash
tmux new-session -s cliproxy_usage_meter 'cd <repo> && PORT=8327 UPSTREAM=http://127.0.0.1:8317 scripts/start_cliproxy_usage_meter.sh'
```

或者直接：

```bash
cd <repo>
PORT=8327 UPSTREAM=http://127.0.0.1:8317 scripts/start_cliproxy_usage_meter.sh
```

临时让客户端走 meter：

```text
base_url 从 http://127.0.0.1:8317/v1
改为     http://127.0.0.1:8327/v1
```

不要自动修改 Codex 配置；先给用户最小修改建议。

## 18. 最终交付格式

Codex 完成后应输出：

1. 新增/修改文件列表。
2. 实现能力摘要。
3. 启动命令。
4. Codex 临时接入方法。
5. 查询命令。
6. HTML dashboard 地址。
7. 测试命令与结果。
8. 未解决项：
   - streaming usage 是否完整
   - alias/account 自动映射是否可靠
   - 模型真实价格是否需要人工维护
   - quota 估计是否已有完整打满周期样本

## 19. 实现原则

- 先做可运行 MVP，不要过度设计。
- 透明代理不破坏请求优先。
- 安全脱敏优先于统计完整性。
- 价格未知时返回 `NULL`。
- 额度估计命名为 `api_equivalent_quota_usd`。
- 总调用次数是一级指标，必须进数据库、CLI 和 HTML dashboard。
- 代码要可读、可维护，有文档。
