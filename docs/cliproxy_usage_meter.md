# cliproxy Usage Meter sidecar（MVP）

这是一个独立的、本机绑定 `127.0.0.1` 的 Python 标准库 sidecar：

```text
client -> http://127.0.0.1:8327/v1/... -> meter -> http://127.0.0.1:8317/v1/...
```

默认模式不修改 cliproxyapi、8317 端口、认证文件、`.cds`、`.zshrc` 或任何现有
客户端的 `base_url`。只有显式开启“确认耗尽路由守卫”时，才会通过官方 management
接口临时写入/恢复认证文件的 `weight` 字段；它仍不直接改写 `.cds`。默认数据库为
`datas/cliproxy_usage.sqlite`，运行时数据库及 WAL 文件均被忽略。

除了显式经过 8327 的请求，sidecar 还可以只读消费 CLIProxyAPI v7.2.125
提供的 `GET /v0/management/usage-queue`。这条路径能统计仍然直连 8317
的客户端；队列中的 `api_key` 字段会被完全丢弃，只保存 token hash/脱敏账号信息。
对于直接访问本机 Sub2API 的 `codex-s2a`，sidecar 还会通过 Sub2API 管理 API
只读同步账号清单、逐请求用量与 Codex 额度快照。

## 启动

先用 fake upstream 验证，再在用户明确选择时指向现有 8317：

```bash
cd <repo>
PORT=8327 UPSTREAM=http://127.0.0.1:8317 scripts/start_cliproxy_usage_meter.sh
```

启动脚本使用 `umask 077`；在 POSIX 上即使直接运行 Python 入口，sidecar 也会尝试在
打开时把 SQLite 主库及 WAL/SHM sidecar 修复为 owner-only `0600`。Windows 依赖 ACL。
运行库仍只属于本机，不会提交到仓库。

长时间运行可放在 tmux 前台：

```bash
tmux new-session -s cliproxy_usage_meter \
  'cd <repo> && PORT=8327 UPSTREAM=http://127.0.0.1:8317 scripts/start_cliproxy_usage_meter.sh'
```

### 直连 8317 的 usage-queue 采集

CLIProxyAPI 的 management API 会对错误认证实施 IP 失败保护。不要把 key
写进命令行或提交到仓库；将它放入 owner-only 文件（权限必须是 `600`），例如：

这里的 key 必须是打开 CLIProxyAPI management panel 时使用的 management key，
不是 `api-keys` 下给 `/v1` 客户端用的代理 API key，也不是配置文件里以 `$2a$`
开头的 bcrypt 哈希；后两者都会得到 401。

```bash
chmod 600 "$HOME/.config/cliproxyapi-management.key"
CLIPROXY_MANAGEMENT_KEY_FILE="$HOME/.config/cliproxyapi-management.key" \
  PORT=8327 UPSTREAM=http://127.0.0.1:8317 \
  scripts/start_cliproxy_usage_meter.sh
```

也可使用 `CLIPROXY_MANAGEMENT_KEY` 环境变量，但文件方式更不容易出现在进程
列表中。没有 key 时 poller 保持 idle，不会猜 key；收到 401/403/429 后会长退避，
不会连续重试触发 CLIProxyAPI 封禁。可用参数为 `--usage-queue-count`、
`--usage-queue-poll-seconds`、`--usage-queue-timeout`，必要时用
`--no-usage-queue` 明确关闭。

### Sub2API / `codex-s2a` 只读导入（默认开启、无 key 时 idle）

`codex-s2a` 若直接请求 `http://127.0.0.1:8080`，流量不会经过 8327、8317 usage queue
或 Cockpit 日志，因此原有采集路径无法看到这些调用。Sub2API importer 使用：

```text
codex-s2a -> Sub2API:8080
Sub2API /api/v1/admin/accounts + /api/v1/admin/usage --read-only--> meter -> 8327/usage
```

先在 Sub2API 中启用 `admin_api_key`，再把同一个明文 key 放在仓库外 owner-only 文件中。
它不是 Sub2API 给普通 `/v1` 客户端使用的 API key：

```bash
chmod 600 /path/to/sub2api-admin.key
SUB2API_BASE_URL=http://127.0.0.1:8080 \
SUB2API_ADMIN_KEY_FILE=/path/to/sub2api-admin.key \
  PORT=8327 UPSTREAM=http://127.0.0.1:8317 \
  scripts/start_cliproxy_usage_meter.sh
```

也可通过 `SUB2API_ADMIN_KEY` 环境变量提供，但外部 `0600` 文件更容易控制暴露范围。
Sub2API admin key 权限很高，因此 importer 只接受 loopback HTTP(S) origin，路径只能为空或
`/api/v1`；远端部署应先建立本地 tunnel，不能把 key 直接发送到任意 URL。

首次完整导入默认回填最近 30 天；后续按上次完整扫描时间重叠回扫一天，并用 keyed import key
幂等更新，所以不会因轮询重复计数，也能吸收延迟写入。账号与用量都逐页读取并核对总数；分页
中途变化、响应过大或 schema 不兼容时整轮失败，不会用局部账号清单推进账号退役。调节项为：

- `SUB2API_POLL_SECONDS` / `--sub2api-poll-seconds`；
- `SUB2API_TIMEOUT` / `--sub2api-timeout`；
- `SUB2API_PAGE_SIZE` / `--sub2api-page-size`；
- `SUB2API_BACKFILL_DAYS` / `--sub2api-backfill-days`；
- `--no-sub2api-import`（或 `SUB2API_IMPORT_ENABLED=0`）完全关闭。

Sub2API 的 `usage_logs.input_tokens` 是非缓存输入；meter 会重建为
`input_tokens + cache_creation_tokens + cache_read_tokens`，并把 cache read/write 分别保留。
逐请求的 input/cache/output 成本快照与 `total_cost` 一并导入，不跟随 meter 之后的价格表变化。
完整账号清单会为每个账号写入状态变化，因此新增账号即使尚无调用，也会在 `/usage` 出现；
`extra.codex_5h_*`、`extra.codex_7d_*` 可用时同步为 5 小时/周额度。

邮箱只在运行内存中用于页面显示与跨来源唯一匹配。SQLite 只保存域隔离 HMAC 身份，不保存
Sub2API admin key、原始账号 ID、账号名称、邮箱、request ID、用户信息或响应原文。
`/healthz` 只返回脱敏的 configured/key-loaded、HTTP 状态、账号/调用数量、最后成功时间和
错误类型。401/403/429 会进入长退避，不会形成认证重试风暴。

不要让同一批请求既经过 8327 再进入 Sub2API，又同时开启 Sub2API importer；透明代理事件和
Sub2API usage row 会分别被视为一次调用。`codex-s2a` 直连 Sub2API、meter 仅做管理 API
只读导入时不会发生这类双计数。

### Cockpit Tools `request_logs` 只读导入（默认开启）

迁移到 Cockpit Tools 后，不必再让业务流量经过 8327 才能进入同一个用量看板。meter
默认启动一个只读 importer，从 Cockpit 的 SQLite `request_logs` 导入调用；落库事件的
`source` 固定为 `cockpit_tools`。数据文件按以下顺序选择：

1. `--cockpit-tools-data-dir /path/to/cockpit-data`；
2. `COCKPIT_TOOLS_DATA_DIR` 指向的数据目录；
3. 默认的 `~/.antigravity_cockpit/codex_local_access_logs.sqlite`。

前两种方式都在所选目录下读取 `codex_local_access_logs.sqlite`。importer 不修改 Cockpit
数据库；数据库或 `request_logs` 表暂时为空时只表示当前没有可导入记录，是正常状态，
不会阻止 8327 看板启动。轮询间隔可用 `COCKPIT_TOOLS_USAGE_POLL_SECONDS` 或
`--cockpit-tools-poll-seconds SECONDS` 调整，完全关闭则使用
`--no-cockpit-tools-import`。

macOS 上还会自动发现 Cockpit 的 WebKit accounts cache，用于把允许的 identity/quota
字段接入既有 keyed identity 与额度视图。需要显式选择测试副本或非标准位置时，可传：

```bash
python3 scripts/cliproxy_usage_meter.py --serve \
  --cockpit-tools-data-dir /path/to/cockpit-data \
  --cockpit-tools-localstorage-db /path/to/cockpit-webkit/localstorage.sqlite3 \
  --cockpit-tools-poll-seconds 30
```

LocalStorage 覆盖也可通过 `COCKPIT_TOOLS_LOCALSTORAGE_DB` 设置。

迁移期间默认仍以 CLIProxyAPI 与 Cockpit 完整清单的并集为有效账号。确认 Cockpit 已成为唯一
账号所有者后，可用 `--cockpit-tools-authoritative-accounts` 或环境变量
`COCKPIT_TOOLS_AUTHORITATIVE_ACCOUNTS=1` 启动；此时仅在 Cockpit 索引与凭据 cache 都完整时，
由 Cockpit 当前清单决定 8327 可见账号，CLIProxyAPI 独有的残留 auth 会按已删除处理。任一
来源不完整时该模式 fail closed，不会凭局部清单授权删除。

WebKit cache 只按 allowlist 提取安全的身份与额度字段；OAuth/API token、cookie、授权头、
原始账号 cache 记录和其他凭据不会写入 meter SQLite。可读账号信息仍遵守既有隐私边界：
只在本机运行时用于映射，持久关联使用 keyed identity。

迁移期间 resolver 会优先用 Cockpit cache 中的 workspace + email/user 复用原
CLIProxyAPI canonical identity；缺少 workspace 时只在邮箱唯一匹配时沿用旧身份，否则使用
Cockpit selector 的域隔离 HMAC fallback。CLIProxyAPI 与 Cockpit 的完整账号清单按并集进行
退役核对，避免删掉旧 auth 文件后误把已迁移账号的历史匿名化；索引或 cache 不完整时核对
会 fail closed，不执行账号退役。

Cockpit 已在每条请求上冻结当时的 input/cache/output 价格快照，并记录 cache-read、
cache-write 与 reasoning 等 token breakdown。importer 沿用这些逐请求快照与分项，
不用 meter 当前价格表重新解释历史请求；若 Cockpit 提升 `model_pricing_version` 并重写其
历史快照，importer 会原位同步同一事件，不会重复计数。`/usage`
因此可以把 Cockpit 用量并入 token 与 API 等价成本统计，同时保留历史计费口径。

兼容性按实际能力/schema 探测，不按 Cockpit Tools 应用版本或账号索引中的 `version`、
`detail_schema_version` 拒收。升级后仅增加版本号、数据库列或索引字段时会自动继续导入，
未知新增字段会被忽略；只有必需字段被删除、改名或改变语义时才会在 importer health 中
报告结构不兼容，且不会拖垮 `8327` 看板。

Cockpit 完整凭据 cache 与其中的结构化终止错误也是账号清单的一部分：索引账号若已从完整
cache 消失，或明确返回 `deactivated_workspace` / `token_invalidated`，即使 CLIProxyAPI
目录仍残留旧 auth 文件、`codex_accounts.json` 仍有陈旧摘要，meter 也会把它从有效账号并集
中剔除。该账号的额度卡与账号累计会在下一次完整导入时立即隐藏，历史明细继续按既有确认期
匿名汇总；`usage_limit_reached` 等单纯额度耗尽不属于账号终止。实现只读取严格白名单内的
错误码，Cockpit 的错误正文不会进入 meter SQLite 或日志。任一清单缺失或损坏时仍 fail
closed；重新登录并恢复凭据后，后续完整清单可以重新激活该账号。

推荐的迁移拓扑是：

```text
client -> Cockpit Tools
Cockpit request_logs --read-only import--> meter -> 127.0.0.1:8327/usage
```

不要把同一批流量先经 8327 代理到 Cockpit，同时又保持 Cockpit importer 开启；这样一条
请求会分别以代理事件和 `source=cockpit_tools` 导入事件出现，造成双计数。业务流量直连
Cockpit、meter 只负责导入和 8327 看板时无需关闭 importer；如果确实把 8327 当作 Cockpit
的代理入口，则必须加 `--no-cockpit-tools-import`。脱敏后的 importer 运行状态、所选来源是否
可读及最近轮询结果统一查看 `http://127.0.0.1:8327/healthz`，健康检查不会返回凭据或真实账号。

### CLIProxyAPI → Cockpit 增量账号迁移脚本

可复用脚本为 `scripts/migrate_cliproxyapi_to_cockpit.py`。默认只读盘点，确认后才执行：

```bash
python3 scripts/migrate_cliproxyapi_to_cockpit.py --json
python3 scripts/migrate_cliproxyapi_to_cockpit.py --apply --yes --json
```

脚本比较 CLIProxyAPI auth 文件与 Cockpit 的凭据 cache，只导入新增账号或 token chain
发生变化的账号；相同 workspace/account ID 但邮箱或用户身份不同的成员不会被错误合并，
Cockpit 独有账号也不会删除。执行时通过 Cockpit 官方 `cockpit-tools://import` 入口和一次性
loopback bundle 传递凭据，不直接改写两边的账号数据库。缓存不可读时，已有账号默认
fail-closed，不会盲目覆盖。

迁移记录位于 `~/.antigravity_cockpit/cliproxyapi_migration_history.json`，只保存时间、
版本、数量、状态和 keyed 不可逆指纹，不保存邮箱、账号 ID、token、源文件名或本地绝对路径。
每次实际执行前还会在 `~/.antigravity_cockpit/backups/cliproxyapi_migration_*` 保存加密账号
文件的本地回滚快照；这些快照含凭据，只应保留在本机并限制文件权限。
Cockpit 的 WAL/SHM 短暂切换不会触发版本拒收：脚本会优先用正常只读快照，只有在没有未提交
日志时才使用 immutable 只读视图；检测到活动日志则 fail-closed，稍后重复运行即可。

### 用 `codex-cockpit` 连接 Cockpit Tools API

`scripts/codex-cockpit` 复用 `codex-cliproxyapi` 的 `cliproxy` 基础 profile，启动时只覆盖
当前 Cockpit API Service 的 loopback base URL 和动态 auth command。相邻的
`scripts/cockpit-tools-api` 从 `codex_local_access.json` 读取端口与 key，并在内存中探测
`/v1/models`；key 不会写入仓库、profile 或日志。Cockpit 变更端口、轮换 key 或增加状态
字段时，下一次启动会自动读取新能力，不按应用版本号拒收。
启动器还显式把动态 auth command 的 `refresh_interval_ms` 设为 300000（5 分钟）。
如果基础 profile 使用 `refresh_interval_ms = 0`，Codex 会等到鉴权失败后才取 token，
从而在每次 Cockpit 请求前先产生一个无账号选择的 401；主动刷新可避免这类伪失败。

```bash
install -m 755 scripts/codex-cockpit scripts/cockpit-tools-api \
  scripts/cockpit_tools_api.py /opt/homebrew/bin/
cd /absolute/path/to/project
codex-cockpit
```

`codex-cockpit` 会在当前终端直接启动 Codex，保留调用者的工作目录并原样转发 Codex 参数。
它不会自动创建 tmux、注入任务 prompt，也不会在退出后自动恢复 thread。

默认读取 `~/.antigravity_cockpit`，可用 `COCKPIT_TOOLS_DATA_DIR` 覆盖；仅在排查服务启动
竞态时使用 `COCKPIT_CODEX_SKIP_HEALTHCHECK=1`。helper 只接受 loopback host，避免把本地
API key 发送到 LAN 或远程地址。

### 确认耗尽后的持久路由锁（可选）

CLIProxyAPI 7.2.130 在 `usage_limit_reached` 没有携带 reset hint 时，会从 1 秒开始
指数冷却，最高 30 分钟，之后仍会探测同一账号。要让真实耗尽账号直接等到 WHAM
窗口恢复，可先配置：

```yaml
max-retry-credentials: 0
save-cooldown-status: true
routing:
  strategy: weighted-round-robin
```

再显式启用守卫：

```bash
CLIPROXY_QUOTA_ROUTING_GUARD=1 \
CLIPROXY_MANAGEMENT_KEY_FILE="$HOME/.config/cliproxyapi-management.key" \
  scripts/start_cliproxy_usage_meter.sh
```

守卫只消费执行队列中 `status_code=429` 且错误类型精确为
`usage_limit_reached` 的记录。WHAM 的 `0%`、普通 429、瞬时
`rate_limit_error` 都不会触发锁。确认后它将对应 credential 的 weight 设为 0，优先用
错误体的 `resets_at`/`resets_in_seconds`，缺失时只接受 24 小时内、确实报告用满的
WHAM 窗口 reset 时间；到期先调用官方 `reset-quota` 再恢复原 weight。

状态文件默认为 `~/.config/cliproxy-usage/quota-routing-locks.json`，权限 `0600`，只含
opaque auth index、keyed subscription identity、时间和原 weight，不含邮箱、认证文件名、
token 或 management key。提前恢复额度时运行：

```bash
python3 scripts/cliproxyapi_reset_quota.py --list
python3 scripts/cliproxyapi_reset_quota.py codex-1
python3 scripts/cliproxyapi_reset_quota.py --all --dry-run
python3 scripts/cliproxyapi_reset_quota.py --all
```

`--all` 只处理官方 cooldown 或守卫已经确认的锁，并在任何写入前核对完整 guard inventory；
它也能覆盖没有 `codex-N` alias 的凭证。该工具会同时清除官方 cooldown 和守卫 weight，
不删除 `.cds`。额度卡分别显示“上游报告 0%”“上游 0% · 仍允许调用”和“已确认耗尽 · 冷却中”，
并保留“上游 0% · 实测可用”作为没有结构化 gate 时、且成功发生在 0% 快照之后的兼容状态，
避免把百分比取整误认为真实封禁。

如果 Chrome 已经保持 management 页面登录状态，可用本机专用启动器（凭据只在内存中
传给 sidecar，不写入 key 文件）：

```bash
cd <repo>
PORT=8327 UPSTREAM=http://127.0.0.1:8317 \
  python3 \
  scripts/start_cliproxy_usage_meter_from_chrome.py
```

如果本机的 CLIProxyAPI 实例已开启 `usage-statistics-enabled`，则不需要改写其配置文件
或重启 8317。CLIProxyAPI v7.2.125 的 queue 源码默认只保留 60 秒、最大 3600 秒，
`GET /usage-queue` 会 destructive pop；本机未另设 retention，因此 poller 尚未启用时
已经过期的历史请求无法从该队列补回。对应源码见
[`internal/redisqueue/queue.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v7.2.125/internal/redisqueue/queue.go)
和 [`management/usage.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v7.2.125/internal/api/handlers/management/usage.go)。

也可以直接指定固定解释器和数据库：

```bash
python3 \
  scripts/cliproxy_usage_meter.py --serve --port 8327 \
  --upstream http://127.0.0.1:8317 --db datas/cliproxy_usage.sqlite
```

`--host` 默认 `127.0.0.1`；`--upstream-timeout` 默认 900 秒。sidecar 不会自行启动、停止或重启 8317 服务。

## 计量与身份

每次经过 8327 的 `/v1/...` 请求、从 8317 usage queue 读到的每条调用，以及从 Cockpit
Tools 导入的每条 `request_logs` 调用，都会落一条 `usage_events`，不论成功、失败、
streaming 或 usage 缺失。队列事件的 `source` 为 `usage_queue`，显式 sidecar 请求的
`source` 为 `sidecar`，Cockpit 导入事件的 `source` 为 `cockpit_tools`。支持：

### ChatGPT App 直登 Codex 的本地监控

个人 ChatGPT/Pro 直登模式不经过 8327/8317，因此代理无法从网络流量中看到请求；也没有面向个人订阅的公开 token 账单 API。当前版本会额外只读扫描 `CODEX_APP_HOME`（默认 `~/.codex`）下 Codex 会话 JSONL 的 `event_msg.token_count` 元数据，并默认从 `~/.antigravity_cockpit/codex_instances.json` 自动发现 Cockpit Codex 多开实例的 `userDataDir`。候选目录在解析符号链接后必须仍位于当前用户 home 内；重复目录会去重，一个实例无 auth、损坏或越界不会阻断其他实例。

默认动态模式直接从每个 home 当前的 `auth.json` 解析 canonical 订阅身份。首次发现 home、首次发现此前没有游标的 JSONL，或检测到账号切换时，导入器先把对应现有 JSONL 推进到文件尾，建立安全边界而不回填；只有边界后新增的记录才归入当前账号。这样不会把切换前尚未扫描的旧会话、后来进入 500 文件扫描窗口的历史文件或外部复制进来的会话猜给当前账号。绑定时间线在 SQLite 中只保存 home 路径 HMAC、身份绑定 HMAC 和时间，不保存真实路径、邮箱、账号 ID 或 token。扫描期间若 auth 文件变化，本轮结果会丢弃并在下一轮重新建立边界。

仅保存 input/cached/output/reasoning/total token、模型、时间和额度窗口；提示词、代码、工具输出、JWT/API key 永不写入数据库。能由结构化邮箱/JWT principal 形成 canonical 订阅身份时才写入对应额度卡；仅有 workspace/account ID 的稀疏旧 auth 会把 token 记为匿名 `unknown`，并跳过额度快照。导入事件的 `source` 为 `codex_app_local`，重复扫描是幂等的。

启动 sidecar 时默认开启动态模式：

```bash
CODEX_APP_HOME="$HOME/.codex" \
scripts/start_cliproxy_usage_meter.sh
```

只有需要旧版的固定账号严格匹配时才显式配置 alias；此模式只扫描指定的 `CODEX_APP_HOME`，并继续拒绝 workspace 或成员不一致的 auth：

```bash
CODEX_APP_HOME="$HOME/.codex" \
CODEX_APP_USAGE_ALIAS=codex-13 \
scripts/start_cliproxy_usage_meter.sh
```

为避免每次轮询重读很大的历史目录，默认只扫描最近修改的 500 个 JSONL，并持久化每个文件的读取 offset；未变化文件不会重读，增长中的活动会话只读取追加部分。可用 `CODEX_APP_USAGE_MAX_FILES` 或 `--codex-app-max-files` 调整范围。

若需要关闭本地日志导入，可增加 `--no-codex-app-import`。跨设备或日志缺失时，固定 alias 模式下 `/usage` 页面中的“手动补录用量”折叠表单会写入 `manual_codex_app` 事件；动态模式没有安全、可读的固定 alias 选择器，因此隐藏该表单。手动补录只接受已经形成 canonical 订阅身份的 alias，避免凭 workspace 或单一历史账号猜归属。页面中的美元数字是按已配置 OpenAI API 单价计算的 API 等价估算，不代表 Pro 订阅实际扣款或剩余额度；订阅额度仍以 ChatGPT/Codex 产品界面为准。

- Responses：`input_tokens` / `output_tokens` / `cached_tokens` / `reasoning_tokens` / `total_tokens`
- Chat Completions：`prompt_tokens` / `completion_tokens` 及对应 details
- 非 streaming JSON，以及不改写字节的 SSE streaming（尽量读取最终 usage）
- 其他 `/v1/...` 路径透明转发并计为一次调用

账号身份只从本地 `~/.codex-*/auth.json`、`~/.cli-proxy-api/*.json` 或 management 返回的结构化
JSON/JWT 字段读取。邮箱是 loopback 页面唯一可读的账号标识；`.cpa.时间.json` 文件名里的文本
绝不被猜成邮箱。邮箱 local-part（包括 `+tag` 和点）完整保留，只规范化 Unicode 与域名大小写。

Team/工作区的 `chatgpt_account_id` 只标识 workspace，并不是成员级唯一键。持久身份使用本机随机
0600 密钥计算 `HMAC(workspace + email)`，所以同一 Team 的不同成员不会串号，同邮箱加入不同
workspace 也不会合并；同一 workspace + 邮箱发生 token 或时间戳文件轮换时则保持同一身份。
如果旧 RT 文件尚未删除且返回 401，quota poller 会继续尝试同邮箱的其他结构化记录；
重复文件不会产生第二张账号卡或第二个额度窗口。
只有结构化邮箱缺失时，才依次退化到 JWT user principal 或结构化 provider subscription ID，且仍
只保存 keyed digest。management 的 `name`/`id`、`auth_index`、原始 workspace/user ID、邮箱、
token、token digest 和认证文件名都只在必要的本机内存映射中使用。

所有写入入口统一只保留订阅 keyed ID、时间、模型、状态、token、成本、streaming/usage-missing
等统计字段；不持久化 alias、account hash/tail、Authorization fingerprint、endpoint、project、
session/thread/turn/request ID、上游错误正文/自由文本错误类型、请求/响应大小或本地日志绝对路径。
模型名也必须符合收窄的 model-ID 字符规则，邮箱、token、账号 ID 或路径样式不会借该列落库。usage queue 暂时无法
解析到结构化订阅时记为匿名/unknown，不会把 token fingerprint 变成临时账号。不会删除或改写任何
认证 token。

keyed ID 使用的 owner-only 密钥默认位于 `~/.config/cliproxy-usage/identity.key`。备份数据库时必须
同时安全备份该密钥；删除或轮换它会让所有新 identity key 改变，旧账号历史因无法证明归属只能匿名化，
不能按邮箱猜回去。

### Token 与调用口径

看板同时保留三种互不混淆的 token 口径：

- `实际消耗 Tokens` / `codex_status_tokens` =
  `max(input_tokens - cached_tokens, 0) + output_tokens`。它最接近 Codex `/status`
  的 session token 用量。
- `缓存命中 Tokens` = `cached_tokens`，是 input 的子集。
- `API 原始处理量` / `api_processed_tokens` = `input_tokens + output_tokens`，其中
  input 已包含缓存命中。它用于观察模型处理吞吐，也是旧 `total_tokens` 所表达的量。
- `reasoning_tokens` 是 output 的子集，只展示，不再加到任何 total 或成本中。

看板把 `实际消耗 Tokens` 进一步拆成“非缓存输入”和“输出”两个一级指标；两者以及
缓存输入均展示独立 USD 成本。拆分成本不是按 token 数比例分配总成本，而是逐条按事件
模型匹配 `input_per_million`、`cached_input_per_million`、`output_per_million` 后分别求和；
所以混合使用不同模型时也能保留输入/输出真实价差。三项拆分之和应与已定价事件的
`estimated_api_cost_usd` 一致，未知或不完整价格继续保持 `NULL`，不会猜算。

CLIProxyAPI 账号池可能用多个订阅账号处理同一个逻辑请求。因此调用数也分两层：

- `logical_requests`：历史上有 request id 的行可按其去重；新写入为避免跨表关联，不持久化
  request id，因此按实际调用/聚合 `call_count` 计数。
- `account_attempts`：真正进入订阅账号选择后的 SQLite 调用行数；网关在账号选择前拒绝的
  请求（例如 Cockpit `auth_failed` 且没有 `account_id` 的 401）仍保留在调用/失败历史中，
  但不进入 HTTP 响应时间轴，且 `account_attempts` 为 0。`retry_attempts = attempts - logical_requests`，
  看板文案称为“额外调用”，不等同于失败数。

每个订阅卡也会单独显示总调用、成功调用、失败调用和额外调用，便于识别某个账号是否
频繁失败或被账号池反复使用。已映射本机 `CODEX_HOME` 的卡片还会显示对应订阅邮箱；
邮箱只从本机 `auth.json` 的身份声明读入内存用于 loopback 页面展示，不写入 SQLite、
日志、额度快照或健康检查响应。额度快照可能由历史 JSONL 乱序回填，因此“当前额度”
按 `fetched_at`（同时间再按数据库 id）选择，而不是把最后插入的历史行误认为最新。

Cockpit Tools 的窗口名称不是稳定语义：当前版本可能把 7 天（甚至月）窗口放在名为
`hourly` 的字段中。导入器以 `window_minutes`/`window_seconds` 为准分类：18,000 秒为
`five_hour`、604,800 秒为 `weekly`、约 28～31 天为 `monthly`；启动迁移也会修正旧版
错误写成 `five_hour` 的 Cockpit 快照。额度百分比只是显示信号，不能单独证明账号已被
封禁；Cockpit `rate_limit.allowed` 与 `rate_limit.limit_reached` 的结构化 gate 才用于
区分“上游 0% · 仍允许调用”和“已确认耗尽 · 冷却中”。

失败调用的状态与 token/cost 仍按对应 API 调用如实累计；只有已选择账号的失败才进入账号
尝试/账号卡失败计数。错误正文和请求关联 ID 不落库；系统不会
猜测哪次成功 token 可以代表整组。

## 价格与 API 等价额度

未知价格时 `estimated_api_cost_usd` 为 `NULL`，不会伪造价格。推荐从
[OpenAI 官方 API 定价页](https://developers.openai.com/api/docs/pricing)显式同步
Standard 的 short/long-context input、cached input、cache write、output 单价
（USD / 每百万 token）：

```bash
PY=python3
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite \
  --sync-official-prices
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite \
  --price-sync-status
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite \
  --list-prices
```

同步会记录官方 URL、抓取时间、页面 SHA256、parser 版本、同步状态、模型数和历史
补价条数。抓取或解析失败时，已生效价格原子保留，代理请求不受影响；server 启动时
不会自行联网。新同步的官方价格会用于后续请求，并对价格仍为 `NULL` 且 token 拆分
完整的已有记录补价，并安全校正能够证明原先按短档冻结的历史长上下文记录。每次调用按
完整 `input_tokens` 判断：`<=272,000` 使用短档，`>272,000` 使用长档；`cached_tokens`
是 input 的子集，因此既计入阈值，又按 cached-input 单价计算。当前官方页明确给出
`gpt-5.6-sol` Standard 短档 input `$5.00/M`、cached input `$0.50/M`、output `$30.00/M`，
长档分别为 `$10.00/M`、`$1.00/M`、`$45.00/M`。若官方页以后删除或未列出
某个模型，meter 不猜价。

也可以显式维护本地价格：

```bash
PY=python3
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite \
  --set-price 'fake-*' 1.00 2.00 --cached-input-price 0.25 \
  --price-source-note 'local test profile; replace with a reviewed source'
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite --list-prices
```

`api_equivalent_quota_usd` 只表示按 API 价格折算的观测额度，不是真实订阅余额。遇到 quota/cooldown/rate-limit
错误时自动封存周期；成功请求跟在 quota 事件之后会自动记录 reset。也可手工标记：

```bash
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite --mark-reset codex-1
$PY scripts/cliproxy_usage_meter.py --db datas/cliproxy_usage.sqlite --mark-quota-hit codex-1
```

未观察到打满时只报告 `current_cycle_observed_floor_usd`；完整周期才报告
`api_equivalent_quota_usd` 及 p20/p50/p80。

## 查询与 dashboard

```bash
$PY scripts/cliproxy_usage_meter.py --summary today
$PY scripts/cliproxy_usage_meter.py --summary 7d
$PY scripts/cliproxy_usage_meter.py --summary all
$PY scripts/cliproxy_usage_meter.py --by-account 7d
$PY scripts/cliproxy_usage_meter.py --by-account all
$PY scripts/cliproxy_usage_meter.py --by-model 7d
$PY scripts/cliproxy_usage_meter.py --by-session 7d
$PY scripts/cliproxy_usage_meter.py --by-date 30d
$PY scripts/cliproxy_usage_meter.py --by-date all
$PY scripts/cliproxy_usage_meter.py --recent 20
$PY scripts/cliproxy_usage_meter.py --quota-summary 30d
$PY scripts/cliproxy_usage_meter.py --quota-summary-by-account
```

机器读取可在上述查询中追加 `--json`。看板地址：

```text
http://127.0.0.1:8327/usage
```

看板展示今日/近 7 日/SQLite 全量累计、成功/失败/streaming、各 token 与估算成本、全量按日历史、
alias/account 与模型排行、quota 周期统计以及最近 50 次账号尝试。页面顶部明确标出 SQLite 第一条和最后一条
记录时间，并显示 direct-8317 collector 是 active、backoff 还是因缺少 key 而 idle；
Cockpit 与 Sub2API importer 的脱敏状态也由 `/healthz` 提供。页面只读取 meter SQLite
和各 importer 的内存健康状态，不直接把管理凭据交给浏览器。`/usage` 与
`/healthz` 都是动态本机状态，响应带 `Cache-Control: no-store`，避免浏览器复用旧额度或身份映射。
额度卡通过运行时 resolver 补回邮箱/alias；SQLite 中的额度快照本身不含这些可读身份信息。
其中“最近 50 次账号尝试”只取已到达账号选择阶段的调用；账号选择前的网关 401 不占用列表名额，
也不进入 HTTP 响应时间轴，而只保留在调用/失败历史中。该列表是历史完成记录，时间精确到秒；账号进入
冷却不会删除冷却前的成功行，当前可调用性以账号卡最新 provider gate/执行信号为准。

新版主视图额外提供：

- 累计、今日、近 7 日的实际消耗（非缓存输入 + 输出）、缓存输入、API 原始处理量、
  输出、推理 token、缓存命中率与 API 等价成本。
- 近 1/6/24 小时的每分钟 HTTP 200 与非 200 双折线时间轴，固定合并 Codex 本地、
  Sub2API、Cockpit Tools、8327 sidecar 与 8317 usage queue 的请求记录；Codex 本地成功用量
  以推定的 200 计入。账号选择前的网关拒绝排除在外，无上游响应时 sidecar 写入的 502 归入非 200 线。
- 近 7 天 token/cost 柱状趋势。
- 每个 Codex 订阅按实际窗口时长归类的 5 小时、周或月剩余百分比、重置时间与 provider gate 状态。
- 每账号当前周期已观测美元下限，以及基于 provider 周/月窗口的 API 等价满额度估值。

### 每分钟响应时间轴 API

页面折线图使用同源、只读 JSON 接口：

```text
GET /usage/timeline?minutes=1440
```

`minutes` 可取 `1`～`10080`（最多 7 天），返回连续的 UTC 分钟桶；没有调用的分钟也会返回
零值。接口固定包含 `sidecar`、`usage_queue`、`cockpit_tools`、`sub2api` 和 `codex_app_local`
五类来源，不提供单来源筛选。账号选择前的网关拒绝和手动 token 汇总不计入响应曲线。每个 point 的
`status_200` 统计状态码为 200 的记录，含 Codex 本地成功用量的推定 200；本地日志不提供完整的
HTTP 失败记录，因此不能据此推断直连 Codex 的完整失败率。`status_non_200` 统计其余状态，`total` 为两者之和。
顶层 `totals` 提供同一窗口汇总。接口带 `Cache-Control: no-store`，不返回账号、模型、错误正文
或其他身份字段。

响应状态另存为只包含“分钟、状态码、调用数、来源”的匿名观测，不依赖可被账号退役流程删除的
`usage_events` 明细。启动时从已有请求明细幂等回填，包括先前未进入时间轴的 Codex 本地记录。
升级后 Cockpit importer 会对原始 `request_logs` 做一次幂等回扫，补回此前
已导入但随账号明细一同清理的历史响应分钟；后续账号退役不会再造成时间轴断层。

两条线同时为 0 只表示该分钟没有采集到请求，不能据此证明服务可用；当请求经过 8327 且上游
连接失败或超时时，meter 会返回并记录 502，因此可在非 200 线上看到该次无响应。

### 额度估算口径（2026-08-12）

API 等价额度不是订阅现金余额，而是把已观测 token 按官方 API 价折成 USD。估算优先级为：

1. 读取 Codex/WHAM 的周/月 `used_percent` 与 reset 时间，并在同一窗口内汇总逐事件冻结成本。
2. `used_percent == 100%`：使用窗口累计成本作为高置信度满额度估值。
3. `5% <= used_percent < 100%`：按 `窗口累计成本 / used_percent * 100` 做比例投影；其中
   5%～10% 标记为 `initial` 初步估算，后续快照会自动更新；例如
   已用 95%（剩余 5%）时可以反推出约 100% 的额度，并明确标注中置信度。
4. 刚重置或 `< 5%`：只显示当前已观测下限；但如果本地留有上一完整窗口（使用率至少
   50%，且对应事件消费完整），则使用上一窗口实测满额度作为 `previous_window_transfer`
   先验。完全没有这类证据时才保持“观测中”，不输出伪精确满额度。

成功请求不再自动证明 reset。若最新 provider 周/月 snapshot 仍显示接近满额，quota 后的零星
成功不会切割周期；这样 transient 429、重试和账号池路由变化不会制造 `$0.05` 一类的伪周期。
同理，成功请求发生在 0% 快照之前时只作为历史事件保留，不会把较新的 provider 冷却信号
显示成“实测可用”。

订阅卡的 `C/A` 标识也有语义：`C` 表示已绑定本机 Codex alias/`CODEX_HOME`，`A` 表示只
能由 auth/account 身份识别、没有稳定 alias。

页面视觉参考用户偏好的 `codex-resets.com` 信息语言，但为本项目重新实现：奶油纸张背景、
墨色粗边框、硬阴影、黄/粉/蓝/绿色块与圆润标题；右上角可在明暗主题之间切换，选择只保存在
浏览器 `localStorage`。页面不引用该站的字体、头像、CSS、JavaScript 或网络资源。

订阅百分比来自 Codex usage 响应里的 `used_percent`；官方 Codex 文档说明本地消息和
cloud chats 共享五小时窗口，并可能有周限额：
<https://developers.openai.com/codex/pricing>。sidecar 默认每 300 秒经 8317 的只读
`/v0/management/api-call` 刷新快照，8317 负责把所选 `auth_index` 的 token 仅在内存中
替换为 `$TOKEN$`；sidecar 不读取、打印或落盘 OAuth token。可用
`CLIPROXY_QUOTA_POLL_SECONDS` 调整刷新周期（最小 60 秒）。

额度轮询会把 management auth-file 的 `name`/`id` 与本地身份只在内存中关联；`auth_index`
仅作为 `/v0/management/api-call` 的不透明选择器，不作为订阅身份，也不会持久化。

额度轮询还把一次完整 auth-files 返回当作订阅 inventory。首次完整读取只建立基线；订阅缺失后立即
标为 `suspect_missing` 并从页面隐藏，连续 3 次完整缺失且至少 10 分钟后才确认清理。若完整清单
突然变空，或账号数降到上次基线的一半及以下，则提高到连续 5 次且至少 30 分钟；某订阅一旦在
这类高风险缩减中进入 missing，即使其他账号随后部分恢复，它也会保持 5 次/30 分钟门槛，直到在
权威清单中重新出现。HTTP/JSON 错误、缺少 `files`、未读完分页、任一 Codex 条目无法形成稳定
keyed identity 都不会推进删除；disabled 条目仍视为存在。

确认清理时，逐调用行按本地日期、模型、状态和 token/cost 维度合并到不含 identity/source/request
字段的 `anonymous_usage_daily`，随后删除该订阅的 quota snapshot、计划、续期、周期、账号关联和
逐调用身份明细。匿名统计继续计入 `today`/`7d`/`all` 总量，但不会再出现在账号卡、最近账号尝试或
quota summary 中。匿名桶保留 calls、成功/失败、streaming 总数、token 与成本总量，但舍弃逐调用
时间、逐调用 streaming 标志和请求/重试关联。退休 tombstone 只含 keyed digest；迟到 queue、proxy
或 import 记录仍可贡献匿名 token，却不能重建账号卡，只有后续完整 inventory 明确重新出现才解除。
库存基线建立后，未出现在 active registry 的未知订阅 key 也按 fail-closed 处理。SQLite 连接启用
`secure_delete=ON`，确认清理后执行 WAL truncate checkpoint。

除上述订阅删除隐私生命周期外，SQLite 没有通用的 7 天 retention。`7d` 只是看板排行的展示窗口；
`all` 查询会读取全部已落库 token 统计。
但 collector 只能从启用时刻开始持久化：CLIProxyAPI 的 usage queue 是短期内存队列，collector 启用前已从
队列消失的历史无法从 8317 反向补回；当前 v7.2.125 也没有另一份内建持久化 token 历史可供回填。

为了最小化关联数据，即使 8317 usage queue 将来提供 `session_id`，当前版本也不会把它写入 SQLite。
页面是跨 session 的 token 汇总，只把 token 算法对齐 `/status`，不把统计范围冒充为单 session。

## 临时接入 Codex（不改持久配置）

先确认 meter 正在监听，再只对一次 Codex 进程使用命令行覆盖。官方 Codex 配置参考说明 `openai_base_url` 是内置
`openai` provider 的 base URL override，`-c key=value` 只覆盖本次运行；见
[Codex config reference](https://developers.openai.com/codex/config-reference/) 和
[Codex CLI reference](https://developers.openai.com/codex/cli/reference/)。例如：

```bash
CODEX_HOME="$HOME/.codex-c" codex \
  -c 'openai_base_url="http://127.0.0.1:8327/v1"'
```

这不会改写 `config.toml`；若 Authorization 与本地 auth 能匹配，meter 会自动显示账号。要强制给某个临时进程打
alias 标签，可用自定义 provider 的一次性覆盖（Codex 版本需支持该配置）：

```bash
CODEX_HOME="$HOME/.codex-c" codex \
  -c 'model_provider="meter"' \
  -c 'model_providers.meter.name="Local cliproxy meter"' \
  -c 'model_providers.meter.base_url="http://127.0.0.1:8327/v1"' \
  -c 'model_providers.meter.wire_api="responses"' \
  -c 'model_providers.meter.requires_openai_auth=true' \
  -c 'model_providers.meter.http_headers={"X-Usage-Alias"="codex-1"}'
```

先用短请求确认记录，再决定是否继续；不要自动批量改 alias 或任何现有项目的 base URL。显式测试请求会消耗真实
额度，优先使用 fake upstream。

## 安全与运维

- sidecar 默认只监听 loopback；不要把 `--host` 改为公网地址。
- 代理不会打印 headers/body，错误消息入库前会做 Bearer、JWT、`sk-` 和长 token 脱敏。
- 数据库写失败时仍优先完成透明转发，并在本地日志中只报告异常类型。
- 通过 `CLIPROXY_USAGE_ACCOUNT_SCAN=0` 或 `--no-account-scan` 可关闭本地 auth 只读扫描。
- 8317 仍由原 cliproxyapi 管理；停止 meter 只需结束自己的进程/tmux session。

## 自测

测试会在临时目录启动 fake upstream 与随机 sidecar 端口，覆盖 Responses/Chat、未知 usage、失败脱敏、SSE
原样透传、慢流式首 chunk、quota/reset、dashboard、CLI 查询、启动脚本，以及带假 management key 的
usage-queue 采集和 `api_key` 丢弃；不会请求真实 8317。官方价格解析/失败保留旧价也使用
fake HTML 和 mock fetch，不依赖测试时联网：

```bash
cd <repo>
tmux new-session -s cliproxy_usage_meter_test \
  'python3 -m unittest discover -s tests -p "test_*.py" -v'
```

## MVP 未解决项

1. streaming upstream 不提供最终 usage 时只能记录 `usage_missing=1`；不会猜 token。
2. alias/account 自动映射依赖 bearer 与本地 auth 相同，或客户端显式提供 `X-Usage-Alias`；共享代理 key 无法自动拆分。
3. 官方价格同步解析 Standard 短/长上下文及 cache-write 费率；官方未列出的代理别名仍保持 `NULL`，
   不做推断。usage 未提供 `cache_write_tokens` 时无法单独展示写入 token，但不影响已上报
   input/cached/output 的长上下文阈值判断。建议定期显式运行 `--sync-official-prices`。
4. quota 等价额度只有观察到完整 quota/cooldown 事件才会封存；当前周期只提供 observed floor。
5. 8317 queue 当前没有 session id，所以无法从历史数据库精确还原单个 Codex/tmux session；
   只能比较同口径 token，不能比较同范围总量。
