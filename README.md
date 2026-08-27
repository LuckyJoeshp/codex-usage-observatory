# Codex Usage Observatory

[![Public repository](https://img.shields.io/badge/repository-public-2ea44f?style=flat-square)](https://github.com/LuckyJoeshp/codex-usage-observatory)
[![Python](https://img.shields.io/badge/python-3.10%2B-3776ab?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-yellow?style=flat-square)](LICENSE)

**Local-first usage, cost, quota, and account observability for Codex.**

Codex Usage Observatory brings request usage, local session metadata, account
status, and provider-reported quota windows into one private-by-default
dashboard. It normalizes non-cached input, cache, output, and reasoning tokens,
then estimates API-equivalent cost without storing prompts or credentials.

Run it as a read-only collector for Cockpit Tools, Sub2API, and local Codex
sessions, or place its optional transparent proxy in front of any
OpenAI-compatible endpoint. CLIProxyAPI support remains available as one
optional adapter; **CLIProxyAPI is not required.**

![Usage Observatory demo — real aggregates with account IDs masked](assets/usage-dashboard-demo.png)

_The screenshot is a real dashboard render: token totals, cost estimates,
quota percentages, trends, model usage, and every account card remain visible.
Only account names/identifiers and the account column in recent tables are
replaced with neutral demo labels._

> This is an observability tool, not a billing API. It cannot read an official
> ChatGPT subscription balance. Dollar figures are API-price equivalents, and
> quota figures are observed/provider-reported windows, not invoices or
> guaranteed remaining balances.

## Why use it

Codex usage can be split across local JSONL metadata, proxy responses, request
databases, and provider management APIs. Each source answers only part of the
question: **what was consumed, which account handled it, how much was cached,
what did it cost at API rates, and when does quota reset?** The observatory
normalizes those observations into one local SQLite model and dashboard.

## Supported sources

| Source | Collection mode | Required? |
| --- | --- | --- |
| Local Codex sessions | Read-only `token_count` and rate-limit metadata from discovered Codex homes | No |
| Cockpit Tools | Read-only request-log, account, pricing, and quota import | No |
| Sub2API | Authenticated loopback import from admin account and usage endpoints | No |
| OpenAI-compatible APIs | Optional transparent proxy for Responses and Chat Completions traffic | No |
| CLIProxyAPI | Optional usage-queue, account, quota, and routing-guard integration | No |

Collectors can be enabled independently. If the same request is visible through
more than one path, enable only one of those paths to avoid double counting.

## What it gives you

| Capability | What is tracked |
| --- | --- |
| Token accounting | Non-cached input, cached input, output, reasoning subset, and raw API processing |
| Cost estimation | Collector-frozen per-request prices or official OpenAI short/long-context prices, split by token type |
| Account behavior | Per-subscription calls, success/failure, models, dates, and token totals |
| Quota visibility | Read-only windows classified by duration (5-hour/week/month), reset times, provider gate state, cooldowns, and observed floors |
| Collection paths | Local Codex metadata, read-only Cockpit Tools, loopback Sub2API, transparent proxy, and optional CLIProxyAPI management integration |
| Dashboard | Inline, dependency-free `/usage` HTML with a per-minute HTTP 200/non-200 line timeline, token trend, account, model, and recent-call views |
| Privacy boundary | Loopback by default; credentials and request metadata discarded; email is memory-only |

## Quick start

```bash
git clone https://github.com/LuckyJoeshp/codex-usage-observatory.git
cd codex-usage-observatory

# Collector-only mode: no CLIProxyAPI service or management key required.
umask 077
python3 scripts/codex_usage_observatory.py --serve --no-usage-queue
```

Open <http://127.0.0.1:8327/usage>.

Collector-only mode auto-discovers available local Codex and Cockpit Tools data.
The Sub2API collector stays idle until an admin key is configured. Missing
sources are treated as unavailable, not as startup failures.

To observe traffic inline, set any OpenAI-compatible upstream and point the
selected client's base URL at `http://127.0.0.1:8327/v1`:

```bash
umask 077
UPSTREAM=http://127.0.0.1:8080 \
  python3 scripts/codex_usage_observatory.py --serve --no-usage-queue
```

On POSIX the meter attempts to keep SQLite, WAL, and SHM files at `0600` even
when opened directly. Runtime databases remain local data and are intentionally
not part of the repository (Windows relies on ACLs).

`scripts/codex_usage_observatory.py` is the canonical CLI. The original
`cliproxy_usage_meter.py` entry point and `CLIPROXY_*` environment variables are
kept as compatibility interfaces for existing installations.

## Collection details

### Local Codex session metadata

The dashboard also monitors direct ChatGPT App Codex sessions when local Codex
JSONL history is available. By default it follows the current account in
`CODEX_APP_HOME` (`~/.codex`) and auto-discovers Cockpit Codex instance homes
from `~/.antigravity_cockpit/codex_instances.json`. Each home is resolved and
must remain inside the current user's home directory. Only token-count and
rate-limit metadata is read; prompts, code, tool output, and credentials are
never persisted.

The first dynamic scan of a home or newly discovered JSONL establishes an
end-of-file safety boundary. An account change establishes a new boundary
before collection resumes, so unread historical records cannot be guessed to
belong to the newly selected account. New `token_count` records after that
boundary are attributed to the canonical subscription when structured member
evidence is available; sparse workspace-only auth remains anonymous and
produces no quota card. Set
`CODEX_APP_USAGE_ALIAS` or pass `--codex-app-alias` only when strict legacy
single-home matching is desired. The `/usage` manual import form is exposed in
that fixed-alias mode only. Dollar values are API-equivalent estimates, not Pro
subscription billing.

### Sub2API

The default-on Sub2API importer reads the paginated management endpoints
`GET /api/v1/admin/accounts` and `GET /api/v1/admin/usage`. It stays idle until
an admin API key is configured.

Enable `admin_api_key` in Sub2API, then put the matching plaintext key in an
owner-only file outside this checkout. Do not use a normal client API key:

```bash
chmod 600 /path/to/sub2api-admin.key
SUB2API_BASE_URL=http://127.0.0.1:8080 \
SUB2API_ADMIN_KEY_FILE=/path/to/sub2api-admin.key \
  python3 scripts/codex_usage_observatory.py --serve --no-usage-queue
```

The admin key is highly privileged, so the importer accepts only loopback
HTTP(S) origins; use a local tunnel for a remote deployment. The environment
variable `SUB2API_ADMIN_KEY` is supported, but an external `0600` file is
preferred. Initial history defaults to 30 days, followed by an overlapping,
idempotent incremental scan. Configure it with `SUB2API_POLL_SECONDS`,
`SUB2API_TIMEOUT`, `SUB2API_PAGE_SIZE`, and `SUB2API_BACKFILL_DAYS`, or the
matching `--sub2api-*` flags. Use `--no-sub2api-import` to disable it.

Sub2API's non-cached input, cache creation, and cache read counters are rebuilt
into the meter's input/cache split, while the request's frozen cost components
are retained. A complete account inventory also creates account-status cards,
so a newly added account appears before its first request; Codex 5-hour and
7-day quota snapshots are imported when present. Email is available only to
the running dashboard. SQLite stores domain-separated keyed identities, not
the admin key, account/request IDs, account names, or email. Sanitized state is
shown on `/usage` and `/healthz`.

> **Double-count warning:** leave `codex-s2a` pointed directly at Sub2API when
> this importer is enabled. If the same request is also proxied through `8327`,
> the transparent proxy event and Sub2API usage row are two independent
> observations of one call.

### Cockpit Tools

The Cockpit Tools collector is enabled by default. It opens Cockpit's request
log database read-only and imports `request_logs` rows into the same dashboard
with `source=cockpit_tools`. Set `COCKPIT_TOOLS_DATA_DIR` or pass
`--cockpit-tools-data-dir /path/to/cockpit-data` to select the data directory;
otherwise it looks for
`~/.antigravity_cockpit/codex_local_access_logs.sqlite`. An existing database
with an empty `request_logs` table is a normal initial state, not a startup
error.

On macOS the meter also auto-discovers Cockpit's WebKit accounts cache for
identity and quota mapping. Use
`COCKPIT_TOOLS_LOCALSTORAGE_DB` or
`--cockpit-tools-localstorage-db /path/to/cockpit-webkit/localstorage.sqlite3`
to override that discovery. Only allowlisted identity/quota fields are
extracted: credentials and raw account-cache records are never copied into the
meter database. Imported requests retain Cockpit's cache-read/cache-write token
breakdown and frozen input/cache/output price snapshot, so historical totals do
not follow later meter price syncs. If Cockpit increments its pricing version
and rewrites those snapshots, the same imported rows are updated in place
without being counted twice. Compatibility is capability-based rather than tied
to a Cockpit Tools application or accounts-index version: newer version values
and additional database/index fields continue to import automatically. A
breaking removal, rename, or semantic change to a required field is isolated in
importer health instead of taking down the `8327` dashboard.

Cockpit's complete credential cache and exact structured terminal account
errors are authoritative for the active-account union. An indexed account is
treated as deleted when that complete cache no longer contains it, or when the
cache reports `deactivated_workspace` or `token_invalidated`, even if a stale
CLIProxyAPI auth file or `codex_accounts.json` summary still exists. Its
dashboard card is hidden on the next complete import, and the normal privacy
grace period later folds its history into anonymous totals. Quota exhaustion
such as `usage_limit_reached` is not a terminal account error. Free-form error
messages are never copied out of Cockpit's cache or persisted by the meter. A
later successful re-login that restores the credential can reactivate the
account. Missing or malformed inventory sources remain fail-closed.

For collector-only use, point request traffic directly at Cockpit Tools and
keep the observatory running to import Cockpit data and serve
<http://127.0.0.1:8327/usage>. Tune the importer with
`COCKPIT_TOOLS_USAGE_POLL_SECONDS` or
`--cockpit-tools-poll-seconds SECONDS`, or disable it with
`--no-cockpit-tools-import`. Sanitized importer state is available at
<http://127.0.0.1:8327/healthz>.

During migration the default active-account inventory remains the safe union
of CLIProxyAPI and Cockpit. After Cockpit becomes the sole account owner, start
the meter with `--cockpit-tools-authoritative-accounts` (or
`COCKPIT_TOOLS_AUTHORITATIVE_ACCOUNTS=1`). A complete Cockpit index/cache pair
then defines the visible accounts and CLI-only stale credentials are treated as
deleted. If either Cockpit source becomes incomplete, the mode fails closed and
does not authorize deletion from that partial view.

> **Double-count warning:** if clients send the same requests through `8327`
> with Cockpit Tools configured as its upstream while Cockpit import remains
> enabled, the meter sees both the proxied request and Cockpit's log row. Route
> clients directly to Cockpit, or add `--no-cockpit-tools-import` when using
> `8327` as that proxy path.

## Companion tools

The dashboard and its collectors do not require the helpers below. They remain
available for existing CLIProxyAPI and Cockpit Tools workflows.

### Incremental CLIProxyAPI → Cockpit account migration

Keep the reusable migration helper in `scripts/migrate_cliproxyapi_to_cockpit.py`.
It is read-only by default and compares the local CLIProxyAPI auth directory
with Cockpit's credential cache, importing only new accounts or changed token
chains on an explicit apply:

```bash
python3 scripts/migrate_cliproxyapi_to_cockpit.py --json
python3 scripts/migrate_cliproxyapi_to_cockpit.py --apply --yes --json
```

The apply path uses Cockpit's supported `cockpit-tools://import` flow and a
one-shot loopback bundle; it does not rewrite either account store. Shared
workspace/account IDs are not treated as the same member when the emails or
user identities differ, and Cockpit-only accounts are preserved. A private
keyed migration history is kept at
`~/.antigravity_cockpit/cliproxyapi_migration_history.json`; it contains only
counts, status, and non-reversible fingerprints. Before an apply, encrypted
Cockpit account files receive a local rollback snapshot under
`~/.antigravity_cockpit/backups/cliproxyapi_migration_*`.

The helper reopens Cockpit's WAL-backed SQLite cache/log files without taking
write locks. If an active journal has uncheckpointed bytes, it waits/fails
closed rather than importing a stale snapshot; rerunning the same command is
safe and idempotent.

### Start Codex through CLIProxyAPI from any folder

The repository includes [`scripts/codex-cliproxyapi`](scripts/codex-cliproxyapi),
which follows the verified `codex-quant` profile (`cliproxy`, Responses API,
and the local CLIProxyAPI account pool) without changing the working directory.
Install it once on macOS so it is available from every folder:

```bash
install -m 755 scripts/codex-cliproxyapi /opt/homebrew/bin/codex-cliproxyapi
cd /path/to/any/project
codex-cliproxyapi
```

The command uses the current directory as Codex's workspace. It does not embed
`QuantSystem` or any other project path. CLIProxyAPI must be running on
`127.0.0.1:8317`; the existing `~/.codex/cliproxy.config.toml` profile supplies
the endpoint and reads the bearer key at runtime.

### Start Codex through Cockpit Tools API Service

The repository also includes [`scripts/codex-cockpit`](scripts/codex-cockpit).
It layers the same `cliproxy` profile used above, then replaces only the
provider endpoint and auth command for the current Cockpit Tools API Service.
At each launch it reads `codex_local_access.json`, probes the authenticated
`/v1/models` endpoint with proxy bypass for loopback, and reads the current key
again whenever Codex refreshes credentials. Port changes, API-key rotation,
and additional Cockpit state fields therefore do not require editing a Codex
profile or this repository; no application-version comparison is used. The
launcher also restores the documented five-minute proactive token refresh, so
a base profile with `refresh_interval_ms = 0` does not create a spurious 401
before every request.

Install the launcher and its sibling helper:

```bash
install -m 755 scripts/codex-cockpit scripts/cockpit-tools-api \
  scripts/cockpit_tools_api.py /opt/homebrew/bin/
cd /path/to/any/project
codex-cockpit
```

Cockpit Tools' Codex API Service must be enabled and listening on its local
loopback endpoint. `codex-cockpit` starts Codex directly in the caller's
terminal, keeps the caller's working directory, and forwards all Codex
arguments unchanged. It does not create tmux sessions, inject a task prompt,
or automatically resume a thread after exit.

`COCKPIT_TOOLS_DATA_DIR` selects a non-default Cockpit data directory,
`COCKPIT_CODEX_PROFILE` selects another base profile, and
`COCKPIT_CODEX_SKIP_HEALTHCHECK=1` is available for troubleshooting only.
`COCKPIT_TOOLS_API_URL` can override the endpoint for a loopback-only test; the
helper refuses LAN/remote URLs so the Cockpit key is not sent elsewhere.

### Avoid repeated probes of confirmed exhausted credentials

Treat a provider-reported `0%` as telemetry, not a routing lock: Codex can
continue returning successful responses for some time after WHAM rounds or
reports the remaining percentage as zero. CLIProxyAPI should lock a credential
only after the execution endpoint itself returns a confirmed
`usage_limit_reached` 429.

For a large account pool, keep cooldown persistence enabled and let one logical
request continue through every distinct currently eligible credential. Use
weighted round-robin so a confirmed-exhaustion guard can exclude one credential
with weight zero without disabling or moving its auth file:

```yaml
max-retry-credentials: 0
save-cooldown-status: true
routing:
  strategy: weighted-round-robin
```

`max-retry-credentials: 0` means “no per-request credential-count cap”; it does
not disable cooldowns or cause an already cooled credential to be selected.
This avoids returning 429 merely because the only healthy credential happened
to be ninth in a pool whose old cap was eight. Equal default weights preserve
normal round-robin distribution.

Enable the sidecar guard when starting the direct-8317 queue collector:

```bash
CLIPROXY_QUOTA_ROUTING_GUARD=1 \
CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/owner-only-management.key \
  scripts/start_cliproxy_usage_meter.sh
```

CLIProxyAPI already persists a long cooldown when the 429 includes
`resets_at`/`resets_in_seconds`. If an exact `usage_limit_reached` 429 arrives
without that hint, the guard uses the matching fresh WHAM reset deadline and
sets only that credential's weight to zero. It restores the previous explicit
weight (or removes the temporary field) after the deadline. A provider-reported
`0%`, a generic 429, and a transient `rate_limit_error` never trigger this
guard. The owner-only guard state stores no token, email, auth filename, or
management key.

If Codex grants an early reset, clear only that credential's routing lock with
the official management endpoint through the token-safe helper:

```bash
CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/owner-only-management.key \
  python3 scripts/cliproxyapi_reset_quota.py --list

CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/owner-only-management.key \
  python3 scripts/cliproxyapi_reset_quota.py codex-1

# After confirming the whole Codex pool reset early, preview and clear every
# confirmed official/guard lock, including credentials without a codex-N alias.
CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/owner-only-management.key \
  python3 scripts/cliproxyapi_reset_quota.py --all --dry-run
CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/owner-only-management.key \
  python3 scripts/cliproxyapi_reset_quota.py --all
```

The helper clears both CLIProxyAPI's official quota cooldown and any guard-owned
weight-zero lock, so it is also the early-reset path. The key file must be mode
`0600`. An explicitly requested `--from-chrome`
fallback can reuse the existing local management session without printing or
persisting its key. The helper refuses remote management origins, ambiguous
aliases, and credentials that are not currently in a confirmed quota cooldown.
Batch mode prevalidates the complete guard inventory before changing anything,
then calls `POST /v0/management/reset-quota` only for confirmed locks; it never
deletes `.cds` files directly.

The usage dashboard labels these states separately: `上游报告 0%` is only a
percentage signal, `上游 0% · 仍允许调用` means Cockpit explicitly returned
`allowed=true`, and `已确认耗尽 · 冷却中` requires the provider gate to report
`allowed=false` or `limit_reached=true` (or a quota-classified execution 429).
A successful request recorded before a later provider snapshot remains a
historical event; it does not override the newer cooldown signal.

The “最近 50 次账号尝试” table is a historical completion ledger. It includes
seconds in its timestamps and excludes gateway authentication failures that
occurred before an account was selected; entering cooldown does not delete
successful rows recorded before that cooldown.

For a direct-8317 queue collector, use an external owner-only key file (never
put a management credential in this checkout):

```bash
chmod 600 /path/to/cliproxy-management.key
CLIPROXY_MANAGEMENT_KEY_FILE=/path/to/cliproxy-management.key \
  PORT=8327 UPSTREAM=http://127.0.0.1:8317 \
  scripts/start_cliproxy_usage_meter.sh
```

If Chrome already has the local CLIProxyAPI management page open,
`scripts/start_cliproxy_usage_meter_from_chrome.py` can pass its key in memory
without writing or printing it.

## Identity and privacy

A Team workspace's `chatgpt_account_id` identifies the workspace, not a unique
member. The meter reads the email only from structured auth JSON or JWT claims;
text embedded in a timestamped `.cpa...json` filename is never treated as an
email. At runtime it derives a keyed identity from workspace + email, using a
random owner-only local key. Tagged mailbox names are preserved. A structured
provider subscription ID or JWT principal is used only when email is absent.
This keeps Team members separate while treating token/file rotation for the
same mailbox as one subscription.

If an old refresh-token file remains after rotation and returns 401, the quota
poller tries the other structured record for that same mailbox before counting
the subscription as unavailable; duplicate files therefore do not create a
second card or a second quota window.

Email and optional `codex-N` aliases exist only in memory for the loopback
dashboard. SQLite stores the keyed subscription ID plus token/model/status/cost
statistics; it does not store email, auth filenames, tokens or token digests,
workspace/user IDs or tails, request/session/thread/turn IDs, project names,
endpoints, error bodies, or local source paths. Quota polling uses management
`name`/`id` only for the in-memory match and `auth_index` only as the opaque
selector for that call. Free-form upstream error types are discarded, and
model labels must match a narrow model-ID grammar before they are stored.

The owner-only identity key defaults to
`~/.config/cliproxy-usage/identity.key`. Back it up together with the SQLite
database and keep both private. Deleting or rotating that key changes every
derived subscription ID, so existing per-account history can no longer be
safely reattached; the meter will preserve uncertain history only anonymously.

The first complete auth inventory establishes a baseline. A missing
subscription is hidden immediately; after three complete misses spanning at
least ten minutes, its call rows are folded into identity-free daily token
statistics and its quota/plan/renewal/account details are deleted. A suddenly
empty inventory, or a drop to half the previous account count or less, requires
five confirmations spanning at least thirty minutes. That stricter policy stays
attached to each missing subscription until it authoritatively reappears.
Malformed, partial, paginated or failed inventories never advance deletion,
and disabled auth files still count as present. A keyed tombstone blocks late
queue/import rows from recreating a retired account; only a later complete
inventory can reactivate it. Dynamic `/usage` and `/healthz` responses use
`Cache-Control: no-store`.

## Safety boundary

- Read-only collectors do not modify or restart Codex, Cockpit Tools, Sub2API,
  CLIProxyAPI, shell aliases, or existing client base URLs. Traffic changes
  only when a client is explicitly pointed at the optional transparent proxy.
- It listens on loopback by default. Do not expose it publicly without adding
  an authentication boundary of your own.
- Authorization, API keys, OAuth tokens and management keys are never printed
  or persisted. The only durable account linkage is an owner-keyed subscription
  ID. The loopback dashboard may display each mapped subscription email from a
  structured local auth claim; email is kept in memory and is not written to
  the usage/quota database or logs.
- Management credentials must come from an owner-only (`0600`) external file
  or environment variable. They are never committed here.
- SQLite files, WAL files, `.env` files, key files, caches and local paths are
  ignored. Tests use fake upstream servers and fixture credentials only.
- Unknown pricing remains `NULL`; the project does not invent a subscription
  price or balance.

## CLI examples

```bash
PYTHON_BIN="${QLAB_PYTHON_BIN:-python3}"

"$PYTHON_BIN" scripts/codex_usage_observatory.py --summary today
"$PYTHON_BIN" scripts/codex_usage_observatory.py --summary all --json
"$PYTHON_BIN" scripts/codex_usage_observatory.py --by-account 7d
"$PYTHON_BIN" scripts/codex_usage_observatory.py --by-model 7d
"$PYTHON_BIN" scripts/codex_usage_observatory.py --quota-summary 30d
"$PYTHON_BIN" scripts/codex_usage_observatory.py --list-prices
"$PYTHON_BIN" scripts/codex_usage_observatory.py --sync-official-prices
```

Official-price sync accepts only the documented OpenAI pricing hosts and
atomically keeps the previous table when fetch/parsing fails. Manual prices can
be set with `--set-price MODEL_PATTERN INPUT_PER_M OUTPUT_PER_M` plus
`--cached-input-price` and `--price-source-note`.

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

The suite covers Responses and Chat Completions usage normalization, streaming
byte transparency, error redaction, account mapping, source importers,
usage-queue polling, quota/reset logic, official-price parsing, dashboard
rendering and all CLI queries. It uses synthetic fixtures and does not contact
configured live services.

The original CLIProxyAPI-era design and requirements are retained for historical
context in
[`docs/cliproxy_usage_meter.md`](docs/cliproxy_usage_meter.md) and
[`docs/cliproxy_usage_meter_requirements.md`](docs/cliproxy_usage_meter_requirements.md).

## Privacy and data safety

The runtime SQLite database lives under `datas/` and is ignored by Git,
including WAL files. Raw authorization headers, OAuth tokens, refresh tokens,
API keys, and management keys are never persisted. The public repository
contains source, tests, documentation, and a screenshot with only account
identities masked—never local usage history or machine-specific paths.

## License

MIT. See [LICENSE](LICENSE).
