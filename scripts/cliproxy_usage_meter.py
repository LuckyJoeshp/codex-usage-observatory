#!/usr/bin/env python3
"""Local, token-safe usage meter in front of an OpenAI-compatible proxy.

The proxy deliberately uses only Python's standard library. Request and response
bodies are inspected in memory for metadata/usage, but are never persisted.
Authorization values are forwarded upstream and used only for an in-memory
subscription lookup.  Tokens and token fingerprints are never persisted.
"""

from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import hmac
import html
from html.parser import HTMLParser
import http.client
import json
import logging
import math
import os
import re
import secrets
import signal
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import unicodedata
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cliproxyapi_quota_guard import (  # noqa: E402
    DEFAULT_STATE_FILE as DEFAULT_QUOTA_GUARD_STATE_FILE,
    QuotaRoutingGuard,
)


LOG = logging.getLogger("cliproxy_usage_meter")
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8327
DEFAULT_UPSTREAM = "http://127.0.0.1:8317"
DEFAULT_DB = PROJECT_ROOT / "datas" / "cliproxy_usage.sqlite"
OFFICIAL_PRICING_URL = "https://developers.openai.com/api/docs/pricing"
OFFICIAL_PRICING_HOSTS = {"developers.openai.com", "platform.openai.com"}
OFFICIAL_PRICE_PARSER_VERSION = "openai-html-table-v4-astra-flat-context"
LONG_CONTEXT_THRESHOLD_TOKENS = 272_000
DEFAULT_USAGE_QUEUE_PATH = "/v0/management/usage-queue"
DEFAULT_USAGE_QUEUE_COUNT = 100
DEFAULT_USAGE_QUEUE_POLL_SECONDS = 5.0
DEFAULT_QUOTA_POLL_SECONDS = 300.0
DEFAULT_QUOTA_POLL_TIMEOUT = 20.0
CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
DEFAULT_CODEX_APP_HOME = Path.home() / ".codex"
DEFAULT_CODEX_APP_ALIAS: str | None = None
DEFAULT_CODEX_APP_POLL_SECONDS = 15.0
MAX_CODEX_APP_JSONL_BYTES = 128 * 1024 * 1024
DEFAULT_CODEX_APP_MAX_FILES = 500
DEFAULT_COCKPIT_TOOLS_DATA_DIR = Path.home() / ".antigravity_cockpit"
COCKPIT_TOOLS_LOG_DB_NAME = "codex_local_access_logs.sqlite"
COCKPIT_TOOLS_ACCOUNTS_INDEX_NAME = "codex_accounts.json"
COCKPIT_TOOLS_CODEX_INSTANCES_NAME = "codex_instances.json"
COCKPIT_TOOLS_ACCOUNT_CACHE_KEY = "agtools.codex.accounts.cache"
DEFAULT_COCKPIT_TOOLS_POLL_SECONDS = 15.0
MAX_COCKPIT_TOOLS_CACHE_BYTES = 16 * 1024 * 1024
COCKPIT_TOOLS_REQUEST_SOURCE = "cockpit_tools"
COCKPIT_TOOLS_QUOTA_SOURCE = "cockpit_tools_quota"
DEFAULT_SUB2API_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_SUB2API_POLL_SECONDS = 15.0
DEFAULT_SUB2API_TIMEOUT = 20.0
DEFAULT_SUB2API_PAGE_SIZE = 1000
DEFAULT_SUB2API_BACKFILL_DAYS = 30
MAX_SUB2API_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_SUB2API_PAGES = 10_000
IMPORTED_EVENT_SYNC_BATCH_SIZE = 500
SUB2API_REQUEST_SOURCE = "sub2api"
SUB2API_QUOTA_SOURCE = "sub2api_quota"
SUB2API_ACCOUNT_SOURCE = "sub2api_account"
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
COCKPIT_TOOLS_TERMINAL_ACCOUNT_ERROR_CODES = frozenset(
    {"deactivated_workspace", "token_invalidated"}
)
# Request observations, including local Codex completions whose successful
# usage imports carry a synthetic 200. Manual token totals are not requests.
RESPONSE_TIMELINE_SOURCES = (
    "sidecar",
    "usage_queue",
    COCKPIT_TOOLS_REQUEST_SOURCE,
    SUB2API_REQUEST_SOURCE,
    "codex_app_local",
)
# Version 2 removes pre-account gateway responses from the HTTP health
# timeline while retaining them in the usage/failure history.
RESPONSE_OBSERVATION_BACKFILL_VERSION = 2
DEFAULT_RESPONSE_TIMELINE_MINUTES = 24 * 60
MAX_RESPONSE_TIMELINE_MINUTES = 7 * 24 * 60
MAX_MANUAL_IMPORT_BYTES = 64 * 1024
DEFAULT_MANAGEMENT_BACKOFF_SECONDS = 300.0
MAX_MANAGEMENT_BACKOFF_SECONDS = 1800.0
MAX_INSPECT_BYTES = 8 * 1024 * 1024
MAX_ERROR_BYTES = 64 * 1024
MAX_SSE_EVENT_BYTES = 2 * 1024 * 1024
QUOTA_ESTIMATE_MIN_USED_PERCENT = 5.0
QUOTA_ESTIMATE_STABLE_USED_PERCENT = 10.0
# Only a provider-reported 100% is treated as a hard cap.  At 95% used
# (5% remaining) we still project to 100% from the observed spend.
QUOTA_ESTIMATE_CAP_CONFIDENCE_PERCENT = 100.0
ALL_TIME_PERIODS = {"all", "all-time", "all_time", "ever", "total"}
PERIOD_START_SENTINEL = "0001-01-01T00:00:00.000000Z"
SUBSCRIPTION_RETIRE_MISSES = 3
SUBSCRIPTION_RETIRE_GRACE_SECONDS = 600
EMPTY_INVENTORY_RETIRE_MISSES = 5
EMPTY_INVENTORY_RETIRE_GRACE_SECONDS = 1800

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
METER_ONLY_HEADERS = {"x-usage-alias", "x-usage-session", "x-usage-project"}
RESET_EVENT_TYPES = {"reset_detected", "manual_reset"}
QUOTA_EVENT_TYPES = {
    "quota_hit",
    "cooldown_hit",
    "usage_limit_hit",
    "rate_limit_hit",
    "manual_quota_hit",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_timestamp(value: Any) -> str:
    """Normalize upstream RFC3339 timestamps to the DB's UTC ``Z`` dialect."""

    if isinstance(value, datetime):
        parsed = value
    else:
        text = safe_text(value, 128) if value is not None else None
        if not text:
            return utc_now()
        parsed = None
        candidate = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is None:
            return utc_now()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_optional_timestamp(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        raw = float(value)
        if raw <= 0:
            return None
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc).isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )
        except (OSError, OverflowError, ValueError):
            return None
    text = safe_text(value, 128)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def short_hash(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    raw = value if isinstance(value, bytes) else value.encode("utf-8", "ignore")
    if not raw:
        return None
    return hashlib.sha256(raw).hexdigest()[:16]


def safe_text(value: Any, limit: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("\x00", "")
    return text[:limit] if text else None


def safe_alias(value: Any) -> str | None:
    text = safe_text(value, 128)
    if text and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", text):
        return text
    return None


def safe_model_identifier(value: Any) -> str | None:
    """Keep a useful model label without accepting arbitrary identity text."""

    text = safe_text(value, 200)
    if not text or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:+/-]{0,199}", text):
        return None
    lowered = text.lower()
    if ".." in text or lowered.startswith(
        (
            "sk-",
            "bearer",
            "eyj",
            "acct-",
            "account-",
            "user-",
            "org-",
            "workspace-",
            "session-",
            "thread-",
            "turn-",
            "request-",
        )
    ):
        return None
    if "/" in text and any(
        segment.lower() in {"users", "home", "private", "var", "tmp", "etc"}
        for segment in text.split("/")
    ):
        return None
    return text


def safe_plan_type(value: Any) -> str | None:
    """Normalize only known subscription plan labels, never arbitrary text."""

    text = (safe_text(value, 64) or "").casefold().replace("-", "_")
    aliases = {
        "chatgpt_free": "free",
        "chatgpt_plus": "plus",
        "chatgpt_pro": "pro",
        "chatgpt_team": "team",
        "chatgpt_business": "business",
        "chatgpt_enterprise": "enterprise",
        "chatgpt_edu": "edu",
    }
    text = aliases.get(text, text)
    return text if text in {
        "free",
        "go",
        "plus",
        "pro",
        "team",
        "business",
        "enterprise",
        "edu",
    } else None


def open_sqlite_readonly(path: Path | str, timeout: float = 2.0) -> sqlite3.Connection:
    """Open an external SQLite database without creating or modifying it."""

    target = Path(path).expanduser().absolute()
    timeout_seconds = max(0.1, float(timeout))
    busy_timeout_ms = max(100, int(timeout_seconds * 1000))

    def connect(uri: str) -> sqlite3.Connection:
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=timeout_seconds,
            )
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
            connection.execute("PRAGMA query_only=ON")
            # Opening a WAL database can succeed before SQLite has acquired a
            # usable read snapshot.  Touch the schema now so the immutable
            # fallback below can handle that transient state too.
            connection.execute(
                "SELECT 1 FROM sqlite_master LIMIT 1"
            ).fetchone()
            return connection
        except sqlite3.Error:
            if connection is not None:
                connection.close()
            raise

    # A WAL database normally needs its ``-shm`` sidecar to be present even
    # for a read-only connection.  Cockpit opens/closes its log database in
    # short-lived commands and can remove that sidecar between polls.  Try
    # the normal read-only VFS first so active WAL frames are included.
    errors: list[sqlite3.OperationalError] = []
    for query in ("mode=ro", "mode=ro&cache=shared"):
        try:
            return connect(f"{target.as_uri()}?{query}")
        except sqlite3.OperationalError as exc:
            errors.append(exc)

    # If no WAL or rollback journal contains uncheckpointed frames, an
    # immutable read-only view is a safe fallback: it never creates sidecars,
    # takes a writer lock, or changes the external database.  Refuse this
    # fallback while either journal has content so we cannot silently import a
    # stale/incomplete snapshot.
    try:
        journal_pending = any(
            candidate.is_file() and candidate.stat().st_size > 0
            for candidate in (
                Path(f"{target}-wal"),
                Path(f"{target}-journal"),
            )
        )
    except OSError:
        journal_pending = True
    if not journal_pending:
        try:
            return connect(f"{target.as_uri()}?mode=ro&immutable=1")
        except sqlite3.OperationalError as exc:
            errors.append(exc)

    if errors:
        raise errors[-1]
    raise sqlite3.OperationalError("unable to open database file")


class _ClosingSQLiteConnection(sqlite3.Connection):
    """Commit or roll back a context-managed transaction, then close it.

    ``sqlite3.Connection.__exit__`` only finishes the transaction; it does not
    close the file descriptor.  Normal request traffic usually lets cyclic GC
    catch up, but a full Cockpit replay can open tens of thousands of
    short-lived connections and exhaust the process descriptor limit first.
    Repository callers already scope every connection with ``with``, so make
    that scope own the underlying descriptor as well.
    """

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool | None:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def canonical_subscription_key(value: Any) -> str | None:
    """Accept only the opaque HMAC identity format produced by this meter."""

    if not isinstance(value, str):
        return None
    return value if re.fullmatch(r"subscription:[0-9a-f]{32}", value) else None


def safe_email(value: Any) -> str | None:
    """Return a bounded local identity email suitable for escaped display."""

    text = safe_text(value, 320)
    if not text or text.count("@") != 1 or any(character.isspace() for character in text):
        return None
    local, domain = text.rsplit("@", 1)
    if not local or not domain or "." not in domain:
        return None
    return text


def normalize_email_identity(value: Any) -> str | None:
    """Normalize a structured email without changing its mailbox identity.

    The local part is deliberately not Gmail-normalized: ``+tag`` and dots
    remain significant because CLIProxyAPI users commonly use tagged mailbox
    aliases as distinct subscriptions.  Only Unicode representation and the
    case-insensitive domain are normalized.
    """

    email = safe_email(value)
    if not email:
        return None
    local, domain = email.rsplit("@", 1)
    return f"{unicodedata.normalize('NFC', local)}@{unicodedata.normalize('NFC', domain).casefold()}"


def is_auth_fallback_alias(value: Any) -> bool:
    """Return whether an alias is the queue's temporary auth-fingerprint label.

    Queue records can arrive while CLIProxyAPI is still writing a refreshed
    auth file.  During that small race the resolver has no named ``codex-N``
    alias and persists ``auth:<fingerprint>`` instead.  This is a provisional
    label, not a user alias, and may be safely re-bound later.
    """

    text = safe_text(value, 128)
    return bool(text and re.fullmatch(r"auth:[0-9a-f]{16}", text, re.IGNORECASE))


def numeric_alias_key(value: Any) -> tuple[int, str]:
    text = safe_text(value, 128) or ""
    match = re.fullmatch(r"codex-(\d+)", text, re.IGNORECASE)
    return (int(match.group(1)), text) if match else (10**9, text)


_SECRET_KEY_VALUE = re.compile(
    r'(?i)("?(?:authorization|access_token|refresh_token|id_token|api[_-]?key)"?\s*[:=]\s*)'
    r'("?)[^",\s}]+\2'
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_SK_TOKEN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_JWT = re.compile(r"\b[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}\b")
_LONG_SECRET = re.compile(r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{40,}(?![A-Za-z0-9_-])")


def redact_text(value: Any, limit: int = 600) -> str | None:
    """Redact common credential shapes before text reaches SQLite or logs."""

    text = safe_text(value, max(limit * 4, 2048))
    if not text:
        return None
    text = _BEARER.sub("Bearer <redacted>", text)
    text = _SECRET_KEY_VALUE.sub(r"\1<redacted>", text)
    text = _SK_TOKEN.sub("<redacted-key>", text)
    text = _JWT.sub("<redacted-jwt>", text)
    text = _LONG_SECRET.sub("<redacted-secret>", text)
    return text[:limit]


def as_nonnegative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return max(result, 0)


def first_present(mapping: Mapping[str, Any] | None, names: Sequence[str]) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return None


def pagination_value_indicates_more(value: Any) -> bool:
    """Conservatively interpret management pagination continuation flags."""

    if value is None or value is False:
        return False
    if isinstance(value, str):
        return value.strip().casefold() not in {"", "false", "0"}
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    # Unknown objects/containers are not proof of a complete inventory.
    return True


def find_named_mapping(value: Any, key_name: str, depth: int = 0) -> Mapping[str, Any] | None:
    if depth > 5:
        return None
    if isinstance(value, Mapping):
        candidate = value.get(key_name)
        if isinstance(candidate, Mapping):
            return candidate
        for child in value.values():
            found = find_named_mapping(child, key_name, depth + 1)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value[:100]:
            found = find_named_mapping(child, key_name, depth + 1)
            if found is not None:
                return found
    return None


def find_model(value: Any, depth: int = 0) -> str | None:
    if depth > 5:
        return None
    if isinstance(value, Mapping):
        model = value.get("model")
        if isinstance(model, str) and model.strip():
            return safe_text(model, 200)
        for child in value.values():
            found = find_model(child, depth + 1)
            if found:
                return found
    elif isinstance(value, list):
        for child in value[:100]:
            found = find_model(child, depth + 1)
            if found:
                return found
    return None


@dataclass
class NormalizedUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_tokens: int | None = None
    cache_write_tokens: int | None = None
    reasoning_tokens: int | None = None
    total_tokens: int | None = None

    @property
    def missing(self) -> bool:
        return all(value is None for value in asdict(self).values())


@dataclass(frozen=True)
class PriceComponents:
    """Per-event API-equivalent costs frozen when the event is priced."""

    non_cached_input_cost_usd: float
    cached_input_cost_usd: float
    output_cost_usd: float
    long_context_pricing_applied: bool = False

    @property
    def total_cost_usd(self) -> float:
        return (
            self.non_cached_input_cost_usd
            + self.cached_input_cost_usd
            + self.output_cost_usd
        )


def normalize_usage(value: Any) -> NormalizedUsage:
    usage = value
    if isinstance(value, Mapping) and isinstance(value.get("usage"), Mapping):
        usage = value["usage"]
    if not isinstance(usage, Mapping):
        return NormalizedUsage()

    input_tokens = as_nonnegative_int(first_present(usage, ("input_tokens", "prompt_tokens")))
    output_tokens = as_nonnegative_int(first_present(usage, ("output_tokens", "completion_tokens")))
    total_tokens = as_nonnegative_int(usage.get("total_tokens"))

    input_details = first_present(usage, ("input_tokens_details", "prompt_tokens_details"))
    output_details = first_present(usage, ("output_tokens_details", "completion_tokens_details"))
    cached_tokens = as_nonnegative_int(
        input_details.get("cached_tokens") if isinstance(input_details, Mapping) else None
    )
    cache_write_tokens = as_nonnegative_int(
        input_details.get("cache_write_tokens") if isinstance(input_details, Mapping) else None
    )
    reasoning_tokens = as_nonnegative_int(
        output_details.get("reasoning_tokens") if isinstance(output_details, Mapping) else None
    )
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens
    return NormalizedUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        cache_write_tokens=cache_write_tokens,
        reasoning_tokens=reasoning_tokens,
        total_tokens=total_tokens,
    )


def find_usage(value: Any, depth: int = 0) -> NormalizedUsage:
    if depth > 6:
        return NormalizedUsage()
    if isinstance(value, Mapping):
        direct = normalize_usage(value)
        if not direct.missing:
            return direct
        for child in value.values():
            found = find_usage(child, depth + 1)
            if not found.missing:
                return found
    elif isinstance(value, list):
        for child in value[:200]:
            found = find_usage(child, depth + 1)
            if not found.missing:
                return found
    return NormalizedUsage()


def parse_json_bytes(body: bytes) -> Any:
    if not body or len(body) > MAX_INSPECT_BYTES:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def extract_error(body: bytes, status_code: int) -> tuple[str | None, str | None]:
    if 200 <= status_code < 300:
        return None, None
    parsed = parse_json_bytes(body[:MAX_ERROR_BYTES])
    error_type: Any = None
    message: Any = None
    if isinstance(parsed, Mapping):
        error = parsed.get("error")
        if isinstance(error, Mapping):
            error_type = first_present(error, ("type", "code"))
            message = first_present(error, ("message", "detail", "error"))
        else:
            error_type = first_present(parsed, ("type", "code", "error_type"))
            message = first_present(parsed, ("message", "detail", "error"))
    if message is None and body:
        message = body[:MAX_ERROR_BYTES].decode("utf-8", "replace")
    return redact_text(error_type, 120) or f"http_{status_code}", redact_text(message)


class SSEInspector:
    """Incrementally inspect SSE data without delaying or rewriting the stream."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._data_lines: list[bytes] = []
        self._event_size = 0
        self.usage = NormalizedUsage()
        self.model: str | None = None

    def feed(self, data: bytes) -> None:
        self._buffer.extend(data)
        while True:
            newline = self._buffer.find(b"\n")
            if newline < 0:
                if len(self._buffer) > MAX_SSE_EVENT_BYTES:
                    self._buffer.clear()
                    self._data_lines.clear()
                    self._event_size = 0
                return
            line = bytes(self._buffer[:newline]).rstrip(b"\r")
            del self._buffer[: newline + 1]
            self._consume_line(line)

    def finish(self) -> None:
        if self._buffer:
            self._consume_line(bytes(self._buffer).rstrip(b"\r"))
            self._buffer.clear()
        self._dispatch()

    def _consume_line(self, line: bytes) -> None:
        if not line:
            self._dispatch()
            return
        if line.startswith(b"data:"):
            payload = line[5:]
            if payload.startswith(b" "):
                payload = payload[1:]
            self._event_size += len(payload)
            if self._event_size <= MAX_SSE_EVENT_BYTES:
                self._data_lines.append(payload)

    def _dispatch(self) -> None:
        if not self._data_lines:
            self._event_size = 0
            return
        data = b"\n".join(self._data_lines)
        self._data_lines.clear()
        self._event_size = 0
        if data.strip() == b"[DONE]":
            return
        parsed = parse_json_bytes(data)
        if parsed is None:
            return
        usage = find_usage(parsed)
        if not usage.missing:
            self.usage = usage
        self.model = find_model(parsed) or self.model


@dataclass(frozen=True)
class AccountIdentity:
    usage_alias: str | None
    account_id_hash: str | None
    account_id_tail: str | None
    # Read from the local Codex auth identity only for loopback dashboard
    # display.  Email is deliberately never copied into SQLite usage/quota
    # rows, logs, queue payloads or health responses.
    account_email: str | None = None
    # Email is the only human-readable identity retained, and remains in
    # process memory.  The persisted subscription key is a keyed composite of
    # the structured email and workspace.  A legacy principal hash exists only
    # to migrate rows written by older builds and is never persisted anew.
    principal_id_hash: str | None = None
    subscription_id_hash: str | None = None
    legacy_subscription_id_hash: str | None = None


class AccountResolver:
    """Best-effort, read-only mapping from local Codex homes to safe identities.

    Raw emails, workspace ids, JWT claims, filenames and token digests stay in
    memory.  SQLite receives only a domain-separated HMAC subscription key.
    """

    def __init__(
        self,
        home: Path | None = None,
        enabled: bool = True,
        refresh_seconds: float = 60.0,
        identity_key_file: Path | str | None = None,
        cockpit_tools_data_dir: Path | str | None = None,
        cockpit_tools_localstorage_db: Path | str | None = None,
        cockpit_tools_enabled: bool = True,
        cockpit_tools_authoritative_accounts: bool = False,
    ):
        self.home = home or Path.home()
        self.enabled = enabled
        self.refresh_seconds = refresh_seconds
        self.identity_key_file = (
            Path(identity_key_file).expanduser()
            if identity_key_file is not None
            else self.home / ".config" / "cliproxy-usage" / "identity.key"
        )
        configured_cockpit_dir = (
            cockpit_tools_data_dir
            if cockpit_tools_data_dir is not None
            else os.environ.get("COCKPIT_TOOLS_DATA_DIR")
        )
        self._cockpit_data_dir_explicit = bool(configured_cockpit_dir)
        self.cockpit_tools_data_dir = (
            Path(configured_cockpit_dir).expanduser()
            if configured_cockpit_dir
            else self.home / ".antigravity_cockpit"
        )
        configured_localstorage = (
            cockpit_tools_localstorage_db
            if cockpit_tools_localstorage_db is not None
            else os.environ.get("COCKPIT_TOOLS_LOCALSTORAGE_DB")
        )
        self._cockpit_localstorage_explicit = bool(configured_localstorage)
        self.cockpit_tools_enabled = bool(cockpit_tools_enabled)
        self.cockpit_tools_authoritative_accounts = bool(
            cockpit_tools_authoritative_accounts
        )
        self.cockpit_tools_localstorage_db = (
            Path(configured_localstorage).expanduser()
            if configured_localstorage
            else None
        )
        self._identity_secret: bytes | None = None
        # ``0.0`` is a valid monotonic timestamp on fresh processes.  Use a
        # negative sentinel so the first resolve always performs a scan.
        self._last_refresh = -float("inf")
        self._lock = threading.Lock()
        self._aliases: dict[str, AccountIdentity] = {}
        self._tokens: dict[str, AccountIdentity] = {}
        self._auth_indexes: dict[str, AccountIdentity] = {}
        self._accounts: dict[str, AccountIdentity] = {}
        self._identity_keys: dict[str, AccountIdentity] = {}
        self._legacy_identity_keys: dict[str, str] = {}
        self._ambiguous_legacy_identity_keys: set[str] = set()
        self._active_subscription_keys: set[str] = set()
        self._cockpit_accounts: dict[str, AccountIdentity] = {}
        self._cockpit_records: list[dict[str, Any]] = []
        self._cockpit_inventory_keys: set[str] = set()
        self._cockpit_inventory_authoritative = False
        self._cockpit_detected = False
        # Sub2API account names/emails are supplied by its authenticated
        # loopback API and remain process-local.  Only the HMAC-backed keys in
        # these identities are ever written to the meter database.
        self._sub2api_accounts: dict[str, AccountIdentity] = {}
        self._sub2api_identity_keys: dict[str, AccountIdentity] = {}
        # Independently signed-in Codex homes are an account source too.
        # Keep their labels across provider rescans, exclusively in memory.
        self._codex_app_homes: dict[str, AccountIdentity] = {}
        self._codex_app_identity_keys: dict[str, AccountIdentity] = {}

    def configure_cockpit_sources(
        self,
        *,
        data_dir: Path | str | None = None,
        localstorage_db: Path | str | None = None,
        enabled: bool | None = None,
        authoritative_accounts: bool | None = None,
    ) -> None:
        """Configure Cockpit sources before an importer starts polling."""

        with self._lock:
            if data_dir is not None:
                self.cockpit_tools_data_dir = Path(data_dir).expanduser()
                self._cockpit_data_dir_explicit = True
            if localstorage_db is not None:
                self.cockpit_tools_localstorage_db = Path(localstorage_db).expanduser()
                self._cockpit_localstorage_explicit = True
            if enabled is not None:
                self.cockpit_tools_enabled = bool(enabled)
            if authoritative_accounts is not None:
                self.cockpit_tools_authoritative_accounts = bool(
                    authoritative_accounts
                )
            self._last_refresh = -float("inf")

    def resolve(self, usage_alias: str | None, auth_fingerprint: str | None) -> AccountIdentity:
        self._refresh_if_needed()
        if auth_fingerprint and auth_fingerprint in self._tokens:
            matched = self._tokens[auth_fingerprint]
            # A locally matched token is stronger evidence than a caller-
            # supplied alias.  In particular, never combine account B's token
            # identity with account A's known alias when both are present.
            resolved_alias = matched.usage_alias
            return AccountIdentity(
                resolved_alias,
                matched.account_id_hash,
                matched.account_id_tail,
                matched.account_email,
                matched.principal_id_hash,
                matched.subscription_id_hash,
                matched.legacy_subscription_id_hash,
            )
        if usage_alias and usage_alias in self._aliases:
            return self._aliases[usage_alias]
        return AccountIdentity(usage_alias, None, None)

    def resolve_queue(
        self,
        auth_index: str | None,
        access_token_sha256: str | None,
        model_alias: str | None = None,
    ) -> AccountIdentity:
        """Resolve a CLIProxyAPI usage-queue item without retaining its API key.

        CLIProxyAPI publishes the full SHA-256 digest in ``access_token_sha256``
        and an auth-file name in ``auth_index``.  The normal sidecar resolver
        intentionally stores only a 16-character digest prefix, so this method
        compares prefixes and never treats the queue's ``api_key`` field as an
        identity source.
        """

        self._refresh_if_needed()
        digest = safe_text(access_token_sha256, 128)
        safe_index = safe_text(auth_index, 512)
        valid_digest = bool(digest and re.fullmatch(r"[0-9a-fA-F]{64}", digest))
        if valid_digest:
            for force_refresh in (False, True):
                if force_refresh:
                    # A queue record can arrive while CLIProxyAPI is
                    # atomically replacing an auth file.  Always retry one
                    # fresh scan for a valid-but-currently-unknown digest,
                    # even if the cache is less than a second old.
                    self._refresh_if_needed(force=True)
                identity = self._tokens.get(digest[:16].lower())
                if identity is not None:
                    return identity
            # A cryptographic selector was supplied but did not match.  Never
            # fall back to a reusable filename that may now belong to another
            # Team member after credential rotation.
            return AccountIdentity(None, None, None)
        if safe_index:
            identity = self._auth_indexes.get(Path(safe_index).name)
            if identity is not None:
                return identity
        # The model alias is useful as a last-resort display label, but it is
        # deliberately not used as an account identity when no auth mapping is
        # available.
        return AccountIdentity(None, None, None)

    def resolve_auth_file(
        self,
        auth_name: str | None,
        account_id: str | None = None,
        account_email: str | None = None,
        principal_id: str | None = None,
        provider_subscription_id: str | None = None,
    ) -> AccountIdentity:
        """Resolve one management auth-file item to its local principal.

        CLIProxyAPI's ``auth_index`` is an opaque call selector, while
        ``name``/``id`` is the local JSON filename.  Matching by filename lets
        us recover the stable JWT user principal without persisting the name,
        email or raw claim.  Structured management fields are used only when
        the local file has not become visible yet.  The filename itself is
        never parsed as an email or identity.
        """

        self._refresh_if_needed()
        safe_name = safe_text(auth_name, 512)
        if safe_name:
            identity = self._auth_indexes.get(Path(safe_name).name)
            if identity is not None:
                account = safe_text(account_id, 256)
                incoming_email = normalize_email_identity(account_email)
                local_email = normalize_email_identity(identity.account_email)
                incoming_principal = safe_text(principal_id, 512)
                incoming_provider_id = safe_text(provider_subscription_id, 512)
                evidence_conflicts = bool(
                    account
                    and short_hash(account) != identity.account_id_hash
                )
                if incoming_email and local_email:
                    # Email is the canonical member evidence.  A matching
                    # structured email remains stable across JWT principal
                    # rotation; a conflicting email is always fail-closed.
                    if incoming_email != local_email:
                        evidence_conflicts = True
                elif (
                    incoming_principal
                    and identity.principal_id_hash
                    and short_hash(incoming_principal) != identity.principal_id_hash
                ):
                    evidence_conflicts = True
                # Provider ids are a fallback identity only.  Ignore their
                # rotation when a stronger local email/principal exists, but
                # compare them when both sides necessarily use that fallback.
                if (
                    incoming_provider_id
                    and not local_email
                    and not identity.principal_id_hash
                    and identity.subscription_id_hash
                    and self._private_hash(
                        "codex-provider-subscription-v1", incoming_provider_id
                    )
                    != identity.subscription_id_hash
                ):
                    evidence_conflicts = True
                if evidence_conflicts:
                    # An exact local filename plus contradictory structured
                    # claims is evidence of a stale/corrupt management item,
                    # not permission to construct a second identity from the
                    # incoming claims.
                    return AccountIdentity(None, None, None)
                return identity
        account = safe_text(account_id, 256)
        if not account:
            return AccountIdentity(None, None, None)
        if account_email or principal_id or provider_subscription_id:
            return self._identity(
                None,
                account,
                account_email,
                principal_id,
                provider_subscription_id,
            )
        return self._identity(None, account)

    def auth_file_known(self, auth_name: str | None) -> bool:
        """Return whether an exact local auth filename is currently indexed."""

        safe_name = safe_text(auth_name, 512)
        if not safe_name:
            return False
        self._refresh_if_needed()
        return Path(safe_name).name in self._auth_indexes

    def resolve_account_id(self, account_id: str | None) -> AccountIdentity:
        account = safe_text(account_id, 256)
        if not account:
            return AccountIdentity(None, None, None)
        self._refresh_if_needed()
        return self._identity(None, account)

    def resolve_account_hash(self, account_id_hash: str | None) -> AccountIdentity:
        """Return only generic workspace metadata for a legacy account hash.

        A currently unique workspace is not proof that its historical rows
        belonged to the one member still visible today.  Account-only legacy
        identities must never be promoted to a member subscription.
        """

        target = safe_text(account_id_hash, 64)
        if not target:
            return AccountIdentity(None, None, None)
        self._refresh_if_needed()
        matches = [
            identity
            for identity in self._identity_keys.values()
            if identity.account_id_hash == target
        ]
        if matches:
            sample = matches[0]
            return AccountIdentity(None, target, sample.account_id_tail)
        return AccountIdentity(None, None, None)

    def resolve_identity_key(self, value: str | None) -> AccountIdentity:
        """Resolve a safe persisted identity key for dashboard-only metadata."""

        key = safe_text(value, 300)
        if not key:
            return AccountIdentity(None, None, None)
        self._refresh_if_needed()
        canonical = self._legacy_identity_keys.get(key, key)
        return self._identity_keys.get(
            canonical,
            self._codex_app_identity_keys.get(
                canonical,
                self._sub2api_identity_keys.get(
                    canonical,
                    AccountIdentity(None, None, None),
                ),
            ),
        )

    def subscription_account_hashes(self) -> set[str]:
        """Return account hashes that have principal-scoped identities."""

        self._refresh_if_needed()
        return {
            identity.account_id_hash
            for identity in self._identity_keys.values()
            if identity.account_id_hash and identity.subscription_id_hash
        }

    def active_subscription_keys(self, *, force_refresh: bool = False) -> set[str]:
        """Return current local canonical keys without exposing their inputs."""

        self._refresh_if_needed(force=force_refresh)
        return (
            set(self._active_subscription_keys)
            | set(self._sub2api_identity_keys)
            | set(self._codex_app_identity_keys)
        )

    def register_codex_app_home(
        self, home_key: str, identity: AccountIdentity
    ) -> None:
        """Publish a stable local auth observation without persisting labels."""

        with self._lock:
            self._codex_app_homes[home_key] = identity
            self._codex_app_identity_keys = {
                key: item
                for item in self._codex_app_homes.values()
                if (key := canonical_subscription_key(resolved_identity_key(item)))
            }

    def retain_codex_app_homes(self, home_keys: set[str]) -> None:
        """Drop contributions from homes no longer configured for collection."""

        with self._lock:
            self._codex_app_homes = {
                key: identity
                for key, identity in self._codex_app_homes.items()
                if key in home_keys
            }
            self._codex_app_identity_keys = {
                key: item
                for item in self._codex_app_homes.values()
                if (key := canonical_subscription_key(resolved_identity_key(item)))
            }

    def register_sub2api_accounts(
        self,
        instance_scope: str,
        records: Sequence[Mapping[str, Any]],
    ) -> dict[str, AccountIdentity]:
        """Replace the in-memory Sub2API account inventory.

        A unique email match reuses an existing CLIProxyAPI/Cockpit identity.
        Otherwise the Sub2API instance and numeric selector receive a stable,
        domain-separated HMAC identity.  Raw selectors and labels never leave
        this in-memory mapping.
        """

        scope = safe_text(instance_scope, 512)
        if not scope:
            raise ValueError("invalid Sub2API instance scope")
        self._refresh_if_needed()
        with self._lock:
            email_candidates: dict[str, dict[str, AccountIdentity]] = {}
            for key, identity in self._identity_keys.items():
                normalized = normalize_email_identity(identity.account_email)
                canonical = canonical_subscription_key(key)
                if normalized and canonical:
                    email_candidates.setdefault(normalized, {})[canonical] = identity

            accounts: dict[str, AccountIdentity] = {}
            identity_keys: dict[str, AccountIdentity] = {}
            for record in records:
                raw_id = record.get("id")
                if isinstance(raw_id, bool):
                    continue
                try:
                    account_id = str(int(raw_id))
                except (TypeError, ValueError, OverflowError):
                    continue
                if int(account_id) <= 0 or account_id in accounts:
                    continue
                extra = record.get("extra")
                extra = extra if isinstance(extra, Mapping) else {}
                email = safe_email(extra.get("email")) or safe_email(record.get("name"))
                normalized = normalize_email_identity(email)
                matches = email_candidates.get(normalized or "", {})
                identity = next(iter(matches.values())) if len(matches) == 1 else None
                if identity is None:
                    subscription_hash = self._private_hash(
                        "sub2api-account-v1",
                        scope,
                        account_id,
                    )
                    identity = AccountIdentity(
                        None,
                        self._private_hash(
                            "sub2api-account-reference-v1",
                            scope,
                            account_id,
                        )[:16],
                        None,
                        email,
                        None,
                        subscription_hash,
                        None,
                    )
                key = resolved_identity_key(identity)
                if not canonical_subscription_key(key):
                    continue
                accounts[account_id] = identity
                identity_keys[key] = identity
            self._sub2api_accounts = accounts
            self._sub2api_identity_keys = identity_keys
            return dict(accounts)

    def resolve_sub2api_account(self, account_id: Any) -> AccountIdentity:
        if isinstance(account_id, bool):
            return AccountIdentity(None, None, None)
        try:
            key = str(int(account_id))
        except (TypeError, ValueError, OverflowError):
            return AccountIdentity(None, None, None)
        return self._sub2api_accounts.get(key, AccountIdentity(None, None, None))

    def sub2api_event_import_key(
        self,
        instance_scope: str,
        event_id: Any,
        request_id: Any,
        created_at: Any,
    ) -> str | None:
        """Return a stable import key without retaining Sub2API request IDs."""

        if isinstance(event_id, bool):
            return None
        try:
            numeric_id = int(event_id)
        except (TypeError, ValueError, OverflowError):
            return None
        scope = safe_text(instance_scope, 512)
        request = safe_text(request_id, 4096) or ""
        timestamp = safe_text(created_at, 128) or ""
        if not scope or numeric_id <= 0:
            return None
        return "sub2api:" + self._private_hash(
            "sub2api-usage-event-v1",
            scope,
            str(numeric_id),
            request,
            timestamp,
        )

    def resolve_cockpit_account(
        self,
        storage_id: str | None,
        email: str | None = None,
    ) -> AccountIdentity:
        """Resolve a Cockpit selector without persisting its raw identifier.

        Cockpit's ``request_logs.account_id`` is an internal selector, not the
        OpenAI workspace id.  A structured workspace/member record from the
        WebKit cache is preferred.  Unknown selectors receive only a
        domain-separated keyed fallback, which can later be migrated when the
        safe cache record becomes available.
        """

        storage = safe_text(storage_id, 512)
        if not self.enabled or not storage:
            return AccountIdentity(None, None, None)
        self._refresh_if_needed()
        known = self._cockpit_accounts.get(storage)
        normalized_email = normalize_email_identity(email)
        if known is not None:
            known_email = normalize_email_identity(known.account_email)
            if normalized_email and known_email and normalized_email != known_email:
                return AccountIdentity(None, None, None)
            return known

        if normalized_email:
            matching = {
                key: identity
                for key, identity in self._identity_keys.items()
                if normalize_email_identity(identity.account_email) == normalized_email
                and canonical_subscription_key(key)
            }
            if len(matching) == 1:
                return next(iter(matching.values()))
        fallback = self._private_hash(
            "cockpit-tools-account-v1",
            storage,
            normalized_email or "",
        )
        return AccountIdentity(
            None,
            None,
            None,
            safe_email(email),
            None,
            fallback,
            None,
        )

    def cockpit_event_import_key(self, event_key: str | None) -> str | None:
        """Return a stable, non-reversible import key for a Cockpit event."""

        if not isinstance(event_key, str) or not event_key or len(event_key) > 65_536:
            return None
        return "cockpit:" + self._private_hash(
            "cockpit-tools-event-v1", event_key
        )

    def cockpit_account_records(
        self,
        *,
        force_refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """Return an in-memory copy containing only allowlisted safe fields."""

        self._refresh_if_needed(force=force_refresh)
        return [dict(record) for record in self._cockpit_records]

    def cockpit_inventory(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return sanitized Cockpit inventory health and canonical keys."""

        self._refresh_if_needed(force=force_refresh)
        keys = set(self._cockpit_inventory_keys)
        return {
            "detected": self._cockpit_detected,
            "authoritative": self._cockpit_inventory_authoritative,
            "owns_active_inventory": bool(
                self.cockpit_tools_authoritative_accounts
                and self._cockpit_detected
                and self._cockpit_inventory_authoritative
            ),
            "account_count": len(keys),
            "active_keys": keys,
        }

    def identity_migrations(self) -> dict[str, str]:
        """Return safe legacy-to-HMAC lineage proven by current auth files."""

        self._refresh_if_needed()
        return dict(self._legacy_identity_keys)

    def ambiguous_legacy_account_keys(self) -> set[str]:
        """Return workspace-only keys shared by multiple current members."""

        self._refresh_if_needed()
        counts: dict[str, set[str]] = {}
        for key, identity in self._identity_keys.items():
            if identity.account_id_hash and identity.subscription_id_hash:
                counts.setdefault(identity.account_id_hash, set()).add(key)
        return {
            f"account:{account_hash}"
            for account_hash, identities in counts.items()
            if len(identities) > 1
        }

    def ambiguous_legacy_identity_keys(self) -> set[str]:
        """Return old principal keys that map to multiple email identities."""

        self._refresh_if_needed()
        return set(self._ambiguous_legacy_identity_keys)

    def _refresh_if_needed(self, *, force: bool = False) -> None:
        if not self.enabled or (
            not force and time.monotonic() - self._last_refresh < self.refresh_seconds
        ):
            return
        with self._lock:
            if not force and time.monotonic() - self._last_refresh < self.refresh_seconds:
                return
            (
                aliases,
                tokens,
                auth_indexes,
                accounts,
                identity_keys,
                legacy_identity_keys,
                ambiguous_legacy_identity_keys,
                active_subscription_keys,
                cockpit_accounts,
                cockpit_records,
                cockpit_inventory_keys,
                cockpit_inventory_authoritative,
                cockpit_detected,
            ) = self._scan()
            self._aliases = aliases
            self._tokens = tokens
            self._auth_indexes = auth_indexes
            self._accounts = accounts
            self._identity_keys = identity_keys
            self._legacy_identity_keys = legacy_identity_keys
            self._ambiguous_legacy_identity_keys = ambiguous_legacy_identity_keys
            self._active_subscription_keys = active_subscription_keys
            self._cockpit_accounts = cockpit_accounts
            self._cockpit_records = cockpit_records
            self._cockpit_inventory_keys = cockpit_inventory_keys
            self._cockpit_inventory_authoritative = cockpit_inventory_authoritative
            self._cockpit_detected = cockpit_detected
            self._last_refresh = time.monotonic()

    def _cockpit_localstorage_candidates(self) -> list[Path]:
        if self.cockpit_tools_localstorage_db is not None:
            return [self.cockpit_tools_localstorage_db]
        root = (
            self.home
            / "Library"
            / "WebKit"
            / "com.jlcodes.cockpit-tools"
            / "WebsiteData"
        )
        try:
            candidates = list(root.rglob("LocalStorage/localstorage.sqlite3"))
        except OSError:
            return []
        try:
            return sorted(
                candidates,
                key=lambda candidate: candidate.stat().st_mtime_ns,
                reverse=True,
            )
        except OSError:
            return candidates

    @staticmethod
    def _safe_cockpit_account_record(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        storage_id = safe_text(value.get("id") or value.get("storage_id"), 512)
        if not storage_id:
            return None
        quota = value.get("quota") if isinstance(value.get("quota"), Mapping) else {}
        raw_quota = (
            quota.get("raw_data")
            if isinstance(quota.get("raw_data"), Mapping)
            else {}
        )
        rate_limit = (
            raw_quota.get("rate_limit")
            if isinstance(raw_quota.get("rate_limit"), Mapping)
            else {}
        )
        provider_allowed = rate_limit.get("allowed")
        provider_limit_reached = rate_limit.get("limit_reached")
        quota_error = (
            value.get("quota_error")
            if isinstance(value.get("quota_error"), Mapping)
            else {}
        )
        raw_terminal_error_code = safe_text(quota_error.get("code"), 64)
        terminal_error_code = (
            raw_terminal_error_code.casefold()
            if raw_terminal_error_code
            and raw_terminal_error_code.casefold()
            in COCKPIT_TOOLS_TERMINAL_ACCOUNT_ERROR_CODES
            else None
        )
        return {
            "id": storage_id,
            "account_id": safe_text(value.get("account_id"), 512),
            "email": safe_email(value.get("email")),
            "user_id": safe_text(value.get("user_id"), 512),
            "plan_type": safe_plan_type(value.get("plan_type")),
            "subscription_active_until": normalize_optional_timestamp(
                value.get("subscription_active_until")
            ),
            "usage_updated_at": normalize_optional_timestamp(
                value.get("usage_updated_at")
            ),
            "hourly_percentage": _percent_value(quota.get("hourly_percentage")),
            "hourly_reset_time": normalize_optional_timestamp(
                quota.get("hourly_reset_time")
            ),
            "hourly_window_minutes": as_nonnegative_int(
                quota.get("hourly_window_minutes")
            ),
            "hourly_window_present": quota.get("hourly_window_present") is True,
            "weekly_percentage": _percent_value(quota.get("weekly_percentage")),
            "weekly_reset_time": normalize_optional_timestamp(
                quota.get("weekly_reset_time")
            ),
            "weekly_window_minutes": as_nonnegative_int(
                quota.get("weekly_window_minutes")
            ),
            "weekly_window_present": quota.get("weekly_window_present") is True,
            # These structured booleans distinguish a rounded/displayed 0%
            # from a provider-confirmed cooldown.  No error body, upsell text,
            # or free-form rate-limit detail leaves the in-memory cache.
            "provider_allowed": (
                provider_allowed if isinstance(provider_allowed, bool) else None
            ),
            "provider_limit_reached": (
                provider_limit_reached
                if isinstance(provider_limit_reached, bool)
                else None
            ),
            # A small exact allowlist is enough to suppress credentials that
            # Cockpit has authoritatively proved unusable.  Free-form error
            # text never leaves the credential cache or reaches SQLite.
            "terminal_error_code": terminal_error_code,
        }

    def _read_cockpit_localstorage_records(
        self,
    ) -> tuple[list[dict[str, Any]], bool, bool]:
        candidates = self._cockpit_localstorage_candidates()
        detected = bool(
            self._cockpit_localstorage_explicit
            or any(candidate.exists() for candidate in candidates)
        )
        for path in candidates:
            try:
                if not path.is_file():
                    continue
                with closing(open_sqlite_readonly(path)) as connection:
                    row = connection.execute(
                        """SELECT value FROM ItemTable
                             WHERE key=? AND length(value)<=? LIMIT 1""",
                        (
                            COCKPIT_TOOLS_ACCOUNT_CACHE_KEY,
                            MAX_COCKPIT_TOOLS_CACHE_BYTES,
                        ),
                    ).fetchone()
                if row is None:
                    continue
                raw = row[0]
                if isinstance(raw, bytes):
                    if len(raw) > MAX_COCKPIT_TOOLS_CACHE_BYTES:
                        continue
                    text = raw.decode("utf-16le")
                elif isinstance(raw, str):
                    if len(raw.encode("utf-8", "ignore")) > MAX_COCKPIT_TOOLS_CACHE_BYTES:
                        continue
                    text = raw
                else:
                    continue
                payload = json.loads(text.lstrip("\ufeff"))
                if isinstance(payload, Mapping):
                    payload = first_present(payload, ("accounts", "data", "items"))
                if not isinstance(payload, list):
                    continue
                records: list[dict[str, Any]] = []
                seen: dict[str, dict[str, Any]] = {}
                conflicts: set[str] = set()
                complete = True
                for item in payload[:100_000]:
                    record = self._safe_cockpit_account_record(item)
                    if record is None:
                        complete = False
                        continue
                    storage_id = record["id"]
                    existing = seen.get(storage_id)
                    if existing is not None and existing != record:
                        conflicts.add(storage_id)
                        complete = False
                        continue
                    seen[storage_id] = record
                for storage_id, record in seen.items():
                    if storage_id not in conflicts:
                        records.append(record)
                if len(payload) > 100_000:
                    complete = False
                return records, True, complete
            except (
                OSError,
                sqlite3.Error,
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValueError,
            ):
                continue
        return [], detected, not detected

    def _read_cockpit_index_records(
        self,
    ) -> tuple[list[dict[str, Any]], bool, bool]:
        path = self.cockpit_tools_data_dir / COCKPIT_TOOLS_ACCOUNTS_INDEX_NAME
        log_path = self.cockpit_tools_data_dir / COCKPIT_TOOLS_LOG_DB_NAME
        detected = bool(
            self._cockpit_data_dir_explicit
            or self.cockpit_tools_data_dir.exists()
            or path.exists()
            or log_path.exists()
        )
        try:
            if not path.is_file() or path.stat().st_size > MAX_COCKPIT_TOOLS_CACHE_BYTES:
                return [], False, detected
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return [], False, detected
        raw_accounts = payload.get("accounts") if isinstance(payload, Mapping) else None
        if not isinstance(raw_accounts, list):
            return [], False, True
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        complete = True
        for item in raw_accounts:
            record = self._safe_cockpit_account_record(item)
            if record is None or record["id"] in seen:
                complete = False
                continue
            seen.add(record["id"])
            records.append(record)
        return records, complete, True

    def _cockpit_source_records(
        self,
    ) -> tuple[list[dict[str, Any]], set[str], bool, bool]:
        if not self.cockpit_tools_enabled:
            return [], set(), True, False
        index_records, index_complete, index_detected = self._read_cockpit_index_records()
        (
            cache_records,
            cache_detected,
            cache_complete,
        ) = self._read_cockpit_localstorage_records()
        cache_by_id = {record["id"]: record for record in cache_records}
        combined: list[dict[str, Any]] = []
        indexed_ids: set[str] = set()
        for indexed in index_records:
            storage_id = indexed["id"]
            indexed_ids.add(storage_id)
            cached = cache_by_id.pop(storage_id, None)
            if cached is None:
                record = dict(indexed)
                # Cockpit can remove an unusable credential from its WebKit
                # cache while leaving a stale summary in codex_accounts.json.
                # Only the agreement of two complete sources may classify
                # that index-only record as deleted; a missing or malformed
                # cache remains fail-closed below.
                if cache_detected and cache_complete:
                    record["credential_cache_missing"] = True
                combined.append(record)
                continue
            indexed_email = normalize_email_identity(indexed.get("email"))
            cached_email = normalize_email_identity(cached.get("email"))
            if indexed_email and cached_email and indexed_email != cached_email:
                # The index and credential-bearing cache disagree.  Keep only
                # the index's safe summary and prevent authoritative deletion.
                index_complete = False
                combined.append(indexed)
                continue
            merged = dict(indexed)
            for key, value in cached.items():
                # Most optional cache fields use ``False`` as an absent-value
                # sentinel and must not erase a safe value from the index.
                # The provider gate booleans are different: ``False`` is an
                # explicit upstream decision and must survive the merge.
                if value is not None and (
                    value is not False
                    or key in {"provider_allowed", "provider_limit_reached"}
                ):
                    merged[key] = value
            combined.append(merged)
        combined.extend(cache_by_id.values())
        detected = index_detected or cache_detected
        # A machine without Cockpit has an empty, complete contribution to a
        # CLIProxy-only union.  Once any Cockpit artifact is detected, only a
        # complete codex_accounts.json may authorize deletion decisions.
        authoritative = (index_complete and cache_complete) if detected else True
        return combined, indexed_ids, authoritative, detected

    def _scan(
        self,
    ) -> tuple[
        dict[str, AccountIdentity],
        dict[str, AccountIdentity],
        dict[str, AccountIdentity],
        dict[str, AccountIdentity],
        dict[str, AccountIdentity],
        dict[str, str],
        set[str],
        set[str],
        dict[str, AccountIdentity],
        list[dict[str, Any]],
        set[str],
        bool,
        bool,
    ]:
        aliases: dict[str, AccountIdentity] = {}
        tokens: dict[str, AccountIdentity] = {}
        auth_indexes: dict[str, AccountIdentity] = {}
        accounts: dict[str, AccountIdentity] = {}
        identity_keys: dict[str, AccountIdentity] = {}
        legacy_identity_keys: dict[str, str] = {}
        conflicted_legacy_keys: set[str] = set()

        def remember_legacy_identity(old_key: str, new_key: str) -> None:
            if old_key in conflicted_legacy_keys:
                return
            existing = legacy_identity_keys.get(old_key)
            if existing is not None and existing != new_key:
                # One old principal digest cannot prove which newer email
                # identity owned its history.  Remove the mapping permanently
                # for this scan instead of depending on filesystem order.
                legacy_identity_keys.pop(old_key, None)
                conflicted_legacy_keys.add(old_key)
                return
            legacy_identity_keys[old_key] = new_key

        def remember_principal_fallback(
            account_id: str,
            account_email: str | None,
            principal_id: str | None,
            identity: AccountIdentity,
        ) -> None:
            principal = safe_text(principal_id, 512)
            if (
                not normalize_email_identity(account_email)
                or not principal
                or not identity.subscription_id_hash
            ):
                return
            fallback = self._private_hash(
                "codex-workspace-principal-v1", account_id, principal
            )
            canonical = f"subscription:{identity.subscription_id_hash}"
            if fallback and f"subscription:{fallback}" != canonical:
                # This keyed fallback was canonical before structured email
                # became available.  Register it for every richer identity so
                # two emails claiming the same principal become ambiguous
                # instead of depending on filesystem order.
                remember_legacy_identity(f"subscription:{fallback}", canonical)
        zshrc = self.home / ".zshrc"
        alias_homes: dict[str, Path] = {}
        try:
            text = zshrc.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        pattern = re.compile(
            r"^alias\s+(codex-\d+)=.*?(?:CODEX_HOME=)?[\\\"]?\$HOME/([^\"' ;]+)", re.MULTILINE
        )
        for match in pattern.finditer(text):
            alias_homes[match.group(1)] = self.home / match.group(2)

        alias_records: list[dict[str, Any]] = []
        for alias, codex_home in alias_homes.items():
            data = self._read_json(codex_home / "auth.json")
            (
                account_id,
                access_tokens,
                account_email,
                principal_id,
                provider_subscription_id,
            ) = self._account_and_tokens(data)
            if not account_id:
                continue
            alias_records.append(
                {
                    "alias": alias,
                    "account_id": account_id,
                    "access_tokens": access_tokens,
                    "account_email": account_email,
                    "principal_id": principal_id,
                    "provider_subscription_id": provider_subscription_id,
                }
            )

        subscription_to_alias: dict[str, str] = {}
        token_to_alias: dict[str, str] = {}
        conflicted_token_fingerprints: set[str] = set()
        account_identities: dict[str, dict[str, AccountIdentity]] = {}
        alias_records_by_name = {
            str(record["alias"]): record for record in alias_records
        }

        def alias_evidence_compatible(
            identity: AccountIdentity,
            account_id: str,
            account_email: str | None,
            principal_id: str | None,
        ) -> bool:
            if short_hash(account_id) != identity.account_id_hash:
                return False
            incoming_email = normalize_email_identity(account_email)
            known_email = normalize_email_identity(identity.account_email)
            principal = safe_text(principal_id, 512)
            if incoming_email and known_email:
                if incoming_email != known_email:
                    return False
            elif (
                principal
                and identity.principal_id_hash
                and short_hash(principal) != identity.principal_id_hash
            ):
                return False
            return True

        def remember_token_identity(
            fingerprint: str,
            identity: AccountIdentity,
            alias: str | None,
        ) -> None:
            if fingerprint in conflicted_token_fingerprints:
                return
            existing = tokens.get(fingerprint)
            if existing is not None and (
                resolved_identity_key(existing) != resolved_identity_key(identity)
            ):
                # A token claimed by conflicting structured principals is not
                # safe identity evidence.  Auth filename matching can still
                # resolve queue rows without letting filesystem order choose.
                tokens.pop(fingerprint, None)
                token_to_alias.pop(fingerprint, None)
                conflicted_token_fingerprints.add(fingerprint)
                return
            tokens[fingerprint] = identity
            if not alias:
                return
            existing_alias = token_to_alias.get(fingerprint)
            if existing_alias and existing_alias != alias:
                existing_identity = aliases.get(existing_alias)
                if existing_identity is None or (
                    resolved_identity_key(existing_identity)
                    != resolved_identity_key(identity)
                ):
                    tokens.pop(fingerprint, None)
                    token_to_alias.pop(fingerprint, None)
                    conflicted_token_fingerprints.add(fingerprint)
                    return
                # Two local aliases for the same canonical subscription are
                # harmless; retain the first stable display mapping.
                return
            token_to_alias[fingerprint] = alias

        for record in alias_records:
            alias = str(record["alias"])
            account_id = str(record["account_id"])
            access_tokens = record["access_tokens"]
            identity = self._identity(
                alias,
                account_id,
                record.get("account_email"),
                record.get("principal_id"),
                record.get("provider_subscription_id"),
            )
            aliases[alias] = identity
            if identity.subscription_id_hash:
                subscription_to_alias[identity.subscription_id_hash] = alias
            identity_keys[resolved_identity_key(identity)] = identity
            if identity.legacy_subscription_id_hash and identity.subscription_id_hash:
                remember_legacy_identity(
                    f"subscription:{identity.legacy_subscription_id_hash}",
                    f"subscription:{identity.subscription_id_hash}",
                )
            remember_principal_fallback(
                account_id,
                record.get("account_email"),
                record.get("principal_id"),
                identity,
            )
            account_identities.setdefault(account_id, {})[
                resolved_identity_key(identity)
            ] = identity
            for token in access_tokens:
                fingerprint = short_hash(token)
                if fingerprint:
                    remember_token_identity(fingerprint, identity, alias)

        proxy_dir = self.home / ".cli-proxy-api"
        try:
            # CLIProxyAPI accepts Codex auth files with user-defined names.
            # Team/workspace exports in particular do not necessarily use the
            # historical ``codex-*.json`` convention, so inspect every JSON
            # file and then filter by its provider metadata.  Keep the legacy
            # filename fallback for older records that predate ``type``.
            proxy_files = list(proxy_dir.glob("*.json"))
        except OSError:
            proxy_files = []
        for path in proxy_files:
            data = self._read_json(path)
            provider = (
                safe_text(data.get("provider") or data.get("type"), 64)
                if isinstance(data, Mapping)
                else None
            )
            if (provider or "").lower() != "codex" and not path.name.startswith("codex-"):
                continue
            (
                account_id,
                access_tokens,
                account_email,
                principal_id,
                provider_subscription_id,
            ) = self._account_and_tokens(data)
            if not account_id:
                continue
            subscription_hash = self._subscription_hash(
                account_id,
                account_email,
                principal_id,
                provider_subscription_id,
            )
            alias = subscription_to_alias.get(subscription_hash or "")
            if alias:
                candidate = aliases.get(alias)
                if candidate is None or not alias_evidence_compatible(
                    candidate, account_id, account_email, principal_id
                ):
                    alias = None
            if not alias:
                for token in access_tokens:
                    candidate_alias = token_to_alias.get(short_hash(token) or "")
                    candidate = aliases.get(candidate_alias or "")
                    if candidate_alias and candidate and alias_evidence_compatible(
                        candidate, account_id, account_email, principal_id
                    ):
                        alias = candidate_alias
                        break
            known = aliases.get(alias or "")
            known_record = alias_records_by_name.get(alias or "", {})
            identity = self._identity(
                alias,
                account_id,
                account_email or (known.account_email if known else None),
                principal_id or known_record.get("principal_id"),
                provider_subscription_id
                or known_record.get("provider_subscription_id"),
            )
            auth_indexes[path.name] = identity
            key = resolved_identity_key(identity)
            if alias and identity.subscription_id_hash:
                existing_alias = aliases.get(alias)
                if existing_alias:
                    old_key = resolved_identity_key(existing_alias)
                    aliases[alias] = identity
                    subscription_to_alias[identity.subscription_id_hash] = alias
                    if old_key != key:
                        if old_key.startswith("subscription:") and key.startswith(
                            "subscription:"
                        ):
                            remember_legacy_identity(old_key, key)
                        identity_keys.pop(old_key, None)
                        account_identities.get(account_id, {}).pop(old_key, None)
                    for fingerprint, mapped_alias in token_to_alias.items():
                        if mapped_alias == alias:
                            tokens[fingerprint] = identity
            identity_keys[key] = identity
            if identity.legacy_subscription_id_hash and identity.subscription_id_hash:
                remember_legacy_identity(
                    f"subscription:{identity.legacy_subscription_id_hash}",
                    f"subscription:{identity.subscription_id_hash}",
                )
            remember_principal_fallback(
                account_id,
                account_email or (known.account_email if known else None),
                principal_id or known_record.get("principal_id"),
                identity,
            )
            account_identities.setdefault(account_id, {})[key] = identity
            for token in access_tokens:
                fingerprint = short_hash(token)
                if fingerprint:
                    remember_token_identity(fingerprint, identity, alias)

        cli_active_keys = {
            key for key in identity_keys if canonical_subscription_key(key)
        }
        email_candidates: dict[str, dict[str, AccountIdentity]] = {}
        for key, identity in identity_keys.items():
            normalized = normalize_email_identity(identity.account_email)
            canonical = canonical_subscription_key(key)
            if normalized and canonical:
                email_candidates.setdefault(normalized, {})[canonical] = identity

        (
            cockpit_records,
            indexed_cockpit_ids,
            cockpit_inventory_authoritative,
            cockpit_detected,
        ) = self._cockpit_source_records()
        cockpit_accounts: dict[str, AccountIdentity] = {}
        cockpit_inventory_keys: set[str] = set()
        cockpit_inactive_keys: set[str] = set()
        cockpit_principal_targets: dict[str, set[str]] = {}
        for record in cockpit_records:
            workspace_id = safe_text(record.get("account_id"), 512)
            normalized_email = normalize_email_identity(record.get("email"))
            user_id = safe_text(record.get("user_id"), 512)
            if not workspace_id or not normalized_email or not user_id:
                continue
            principal_key = "subscription:" + self._private_hash(
                "codex-workspace-principal-v1", workspace_id, user_id
            )
            target_hash = self._subscription_hash(
                workspace_id,
                normalized_email,
                user_id,
            )
            if target_hash:
                cockpit_principal_targets.setdefault(principal_key, set()).add(
                    f"subscription:{target_hash}"
                )
        for record in cockpit_records:
            storage_id = str(record["id"])
            workspace_id = safe_text(record.get("account_id"), 512)
            email = safe_email(record.get("email"))
            normalized_email = normalize_email_identity(email)
            user_id = safe_text(record.get("user_id"), 512)
            identity: AccountIdentity | None = None
            if workspace_id and (normalized_email or user_id):
                identity = self._identity(
                    None,
                    workspace_id,
                    email,
                    user_id,
                )
            elif normalized_email:
                matches = email_candidates.get(normalized_email, {})
                if len(matches) == 1:
                    identity = next(iter(matches.values()))

            fallback_with_email = self._private_hash(
                "cockpit-tools-account-v1",
                storage_id,
                normalized_email or "",
            )
            fallback_without_email = self._private_hash(
                "cockpit-tools-account-v1",
                storage_id,
                "",
            )
            if identity is None or not identity.subscription_id_hash:
                identity = AccountIdentity(
                    None,
                    None,
                    None,
                    email,
                    None,
                    fallback_with_email,
                    None,
                )
            canonical_key = f"subscription:{identity.subscription_id_hash}"
            if workspace_id and normalized_email and user_id:
                principal_fallback = self._private_hash(
                    "codex-workspace-principal-v1",
                    workspace_id,
                    user_id,
                )
                if f"subscription:{principal_fallback}" != canonical_key:
                    remember_legacy_identity(
                        f"subscription:{principal_fallback}",
                        canonical_key,
                    )
                principal_key = f"subscription:{principal_fallback}"
                if cockpit_principal_targets.get(principal_key) == {canonical_key}:
                    previous = identity_keys.get(principal_key)
                    if previous is not None:
                        identity = AccountIdentity(
                            previous.usage_alias,
                            identity.account_id_hash,
                            identity.account_id_tail,
                            identity.account_email,
                            identity.principal_id_hash,
                            identity.subscription_id_hash,
                            identity.legacy_subscription_id_hash,
                        )
                        for alias_name, alias_identity in list(aliases.items()):
                            if resolved_identity_key(alias_identity) == principal_key:
                                aliases[alias_name] = identity
                        for fingerprint, token_identity in list(tokens.items()):
                            if resolved_identity_key(token_identity) == principal_key:
                                tokens[fingerprint] = identity
                        for filename, file_identity in list(auth_indexes.items()):
                            if resolved_identity_key(file_identity) == principal_key:
                                auth_indexes[filename] = identity
                        identity_keys.pop(principal_key, None)
            existing = identity_keys.get(canonical_key)
            if existing is not None:
                identity = AccountIdentity(
                    existing.usage_alias,
                    identity.account_id_hash or existing.account_id_hash,
                    identity.account_id_tail or existing.account_id_tail,
                    identity.account_email or existing.account_email,
                    identity.principal_id_hash or existing.principal_id_hash,
                    identity.subscription_id_hash,
                    identity.legacy_subscription_id_hash
                    or existing.legacy_subscription_id_hash,
                )
            identity_keys[canonical_key] = identity
            cockpit_accounts[storage_id] = identity
            for fallback in {fallback_with_email, fallback_without_email}:
                old_key = f"subscription:{fallback}"
                if old_key != canonical_key:
                    remember_legacy_identity(old_key, canonical_key)
            if storage_id in indexed_cockpit_ids:
                if record.get("terminal_error_code") or record.get(
                    "credential_cache_missing"
                ):
                    cockpit_inactive_keys.add(canonical_key)
                else:
                    cockpit_inventory_keys.add(canonical_key)

        for account_id, keyed in account_identities.items():
            sample = next(iter(keyed.values()))
            accounts[account_id] = AccountIdentity(
                None,
                sample.account_id_hash,
                sample.account_id_tail,
            )

        def migrated_active_key(value: str) -> str:
            current = value
            visited: set[str] = set()
            while current in legacy_identity_keys and current not in visited:
                visited.add(current)
                current = legacy_identity_keys[current]
            return current

        active_subscription_keys: set[str] = set()
        cockpit_owns_inventory = bool(
            self.cockpit_tools_authoritative_accounts
            and cockpit_detected
            and cockpit_inventory_authoritative
        )
        inventory_keys = (
            cockpit_inventory_keys
            if cockpit_owns_inventory
            else cli_active_keys | cockpit_inventory_keys
        )
        # A structured terminal error or a record removed from Cockpit's
        # complete credential cache is newer and more authoritative than a
        # stale local auth file. Treat it as deleted until Cockpit restores a
        # healthy credential record. After an operator declares Cockpit the
        # inventory owner, every CLI-only key is likewise excluded, but only
        # while Cockpit's two-source inventory remains complete.
        for key in inventory_keys - cockpit_inactive_keys:
            migrated = migrated_active_key(key)
            if canonical_subscription_key(migrated):
                active_subscription_keys.add(migrated)
        return (
            aliases,
            tokens,
            auth_indexes,
            accounts,
            identity_keys,
            legacy_identity_keys,
            conflicted_legacy_keys,
            active_subscription_keys,
            cockpit_accounts,
            cockpit_records,
            cockpit_inventory_keys,
            cockpit_inventory_authoritative,
            cockpit_detected,
        )

    @staticmethod
    def _read_json(path: Path) -> Any:
        try:
            if path.stat().st_size > 5 * 1024 * 1024:
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None

    @staticmethod
    def _account_and_tokens(
        data: Any,
    ) -> tuple[str | None, list[str], str | None, str | None, str | None]:
        if not isinstance(data, Mapping):
            return None, [], None, None, None
        nested = data.get("tokens") if isinstance(data.get("tokens"), Mapping) else {}
        raw_tokens = [nested.get("access_token"), data.get("access_token")]

        def decoded_claims(value: Any) -> Mapping[str, Any]:
            return value if isinstance(value, Mapping) else _decode_jwt_claims_unverified(value)

        claim_sets = [
            decoded_claims(value)
            for value in (
                nested.get("id_token"),
                data.get("id_token"),
                nested.get("access_token"),
                data.get("access_token"),
            )
            if value is not None
        ]
        structured_claims: list[
            tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]
        ] = []
        for claims in claim_sets:
            auth_claims = claims.get("https://api.openai.com/auth")
            profile_claims = claims.get("https://api.openai.com/profile")
            structured_claims.append(
                (
                    claims,
                    auth_claims if isinstance(auth_claims, Mapping) else {},
                    profile_claims if isinstance(profile_claims, Mapping) else {},
                )
            )

        account_candidates = [nested.get("account_id"), data.get("account_id")]
        account_candidates.append(data.get("chatgpt_account_id"))
        for claims, auth_claims, _profile_claims in structured_claims:
            account_candidates.extend(
                (auth_claims.get("chatgpt_account_id"), claims.get("chatgpt_account_id"))
            )
        valid_accounts = [
            account
            for value in account_candidates
            if (account := safe_text(value, 256)) is not None
        ]
        if len(set(valid_accounts)) > 1:
            return None, [], None, None, None
        account_id = valid_accounts[0] if valid_accounts else None

        email_candidates: list[Any] = [data.get("email"), nested.get("email")]
        for claims, auth_claims, profile_claims in structured_claims:
            email_candidates.extend(
                (claims.get("email"), auth_claims.get("email"), profile_claims.get("email"))
            )
        valid_emails = [
            email
            for value in email_candidates
            if (email := safe_email(value)) is not None
        ]
        normalized_emails = {
            normalized
            for email in valid_emails
            if (normalized := normalize_email_identity(email)) is not None
        }
        if len(normalized_emails) > 1:
            # Contradictory structured claims are not a precedence question.
            # Reject the complete auth record so neither a lower-priority
            # principal nor a token can guess which member owns it.
            return None, [], None, None, None
        account_email = valid_emails[0] if valid_emails else None

        explicit_principal_candidates: list[Any] = [data.get("chatgpt_user_id")]
        subject_candidates: list[Any] = []
        for claims, auth_claims, _profile_claims in structured_claims:
            explicit_principal_candidates.extend(
                (
                    auth_claims.get("chatgpt_user_id"),
                    auth_claims.get("user_id"),
                    claims.get("chatgpt_user_id"),
                )
            )
            subject_candidates.append(claims.get("sub"))
        explicit_principals = [
            principal
            for value in explicit_principal_candidates
            if (principal := safe_text(value, 512)) is not None
        ]
        subjects = [
            subject
            for value in subject_candidates
            if (subject := safe_text(value, 512)) is not None
        ]
        principal_values = explicit_principals or subjects
        if not account_email and len(set(principal_values)) > 1:
            return None, [], None, None, None
        principal_id = principal_values[0] if principal_values else None

        provider_candidates: list[Any] = [
            data.get("subscription_id"),
            data.get("chatgpt_subscription_id"),
            nested.get("subscription_id"),
        ]
        for claims, auth_claims, _profile_claims in structured_claims:
            provider_candidates.extend(
                (
                    auth_claims.get("subscription_id"),
                    auth_claims.get("chatgpt_subscription_id"),
                    claims.get("subscription_id"),
                    claims.get("chatgpt_subscription_id"),
                )
            )
        provider_values = [
            provider
            for value in provider_candidates
            if (provider := safe_text(value, 512)) is not None
        ]
        if not account_email and not principal_id and len(set(provider_values)) > 1:
            return None, [], None, None, None
        provider_subscription_id = provider_values[0] if provider_values else None
        return (
            account_id,
            [token for token in raw_tokens if isinstance(token, str) and token],
            account_email,
            principal_id,
            provider_subscription_id,
        )

    def _load_identity_secret(self) -> bytes:
        if self._identity_secret is not None:
            return self._identity_secret
        path = self.identity_key_file
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.is_symlink():
            raise ValueError("identity key path must not be a symlink")
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            descriptor = None
        if descriptor is not None:
            try:
                os.write(descriptor, secrets.token_hex(32).encode("ascii"))
            finally:
                os.close(descriptor)
        if path.is_symlink() or not path.is_file():
            raise ValueError("identity key must be a regular file")
        mode = path.stat().st_mode & 0o777
        if mode & 0o077:
            raise PermissionError("identity key must be owner-only (mode 600)")
        raw = path.read_bytes()
        if len(raw) > 128:
            raise ValueError("identity key is invalid")
        try:
            secret = bytes.fromhex(raw.decode("ascii").strip())
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("identity key is invalid") from exc
        if len(secret) != 32:
            raise ValueError("identity key is invalid")
        self._identity_secret = secret
        return secret

    def _private_hash(self, domain: str, *parts: str) -> str:
        payload = "\0".join((domain, *parts)).encode("utf-8", "strict")
        return hmac.new(self._load_identity_secret(), payload, hashlib.sha256).hexdigest()[:32]

    def _subscription_hash(
        self,
        account_id: str,
        account_email: str | None,
        principal_id: str | None = None,
        provider_subscription_id: str | None = None,
    ) -> str | None:
        email = normalize_email_identity(account_email)
        if email:
            return self._private_hash("codex-workspace-email-v1", account_id, email)
        principal = safe_text(principal_id, 512)
        if principal:
            return self._private_hash("codex-workspace-principal-v1", account_id, principal)
        provider_id = safe_text(provider_subscription_id, 512)
        if provider_id:
            return self._private_hash("codex-provider-subscription-v1", provider_id)
        return None

    def _identity(
        self,
        alias: str | None,
        account_id: str,
        account_email: str | None = None,
        principal_id: str | None = None,
        provider_subscription_id: str | None = None,
    ) -> AccountIdentity:
        principal = safe_text(principal_id, 512)
        return AccountIdentity(
            alias,
            short_hash(account_id),
            account_id[-8:] if account_id else None,
            safe_email(account_email),
            short_hash(principal),
            self._subscription_hash(
                account_id,
                account_email,
                principal,
                provider_subscription_id,
            ),
            short_hash(f"{account_id}\0{principal}") if principal else None,
        )


def identity_key(
    account_id_hash: str | None,
    usage_alias: str | None,
    auth_fingerprint: str | None,
    installation_id: str | None,
    session_id: str | None,
) -> str:
    if account_id_hash:
        return f"account:{account_id_hash}"
    if usage_alias:
        return f"alias:{usage_alias}"
    if auth_fingerprint:
        return f"auth:{auth_fingerprint}"
    if installation_id:
        return f"installation:{short_hash(installation_id)}"
    if session_id:
        return f"session:{short_hash(session_id)}"
    return "unknown"


def resolved_identity_key(
    identity: AccountIdentity,
    usage_alias: str | None = None,
    auth_fingerprint: str | None = None,
    installation_id: str | None = None,
    session_id: str | None = None,
) -> str:
    """Return the principal-scoped key when a stable JWT user is known.

    ``chatgpt_account_id`` identifies a workspace for Team subscriptions and
    can therefore collide across users.  A composite subscription hash takes
    precedence; old/sparse auth formats retain the established fallback key
    order for backward compatibility.
    """

    if identity.subscription_id_hash:
        canonical = canonical_subscription_key(
            f"subscription:{identity.subscription_id_hash}"
        )
        if canonical:
            return canonical
    return identity_key(
        identity.account_id_hash,
        usage_alias if usage_alias is not None else identity.usage_alias,
        auth_fingerprint,
        installation_id,
        session_id,
    )


@dataclass
class RequestInfo:
    endpoint: str
    method: str
    model: str | None
    stream: int
    session_id: str | None
    thread_id: str | None
    turn_id: str | None
    installation_id: str | None
    window_id: str | None
    usage_alias: str | None
    usage_project: str | None
    auth_fingerprint: str | None
    account_id_hash: str | None
    account_id_tail: str | None
    identity_key: str


def request_info(
    endpoint: str,
    method: str,
    headers: Mapping[str, str],
    body: bytes,
    resolver: AccountResolver,
) -> RequestInfo:
    parsed = parse_json_bytes(body)
    payload = parsed if isinstance(parsed, Mapping) else {}
    usage_alias = safe_alias(headers.get("X-Usage-Alias"))
    model = safe_text(payload.get("model"), 200)
    stream_value = payload.get("stream")
    stream = int(stream_value is True or str(stream_value).lower() in {"1", "true", "yes"})

    authorization = headers.get("Authorization", "")
    token: str | None = None
    if authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    elif headers.get("X-Api-Key"):
        token = headers.get("X-Api-Key")
    auth_fingerprint = short_hash(token)
    identity = resolver.resolve(usage_alias, auth_fingerprint)
    resolved_alias = usage_alias or identity.usage_alias
    key = (
        f"subscription:{identity.subscription_id_hash}"
        if identity.subscription_id_hash
        else "unknown"
    )
    return RequestInfo(
        endpoint=endpoint,
        method=method,
        model=model,
        stream=stream,
        session_id=None,
        thread_id=None,
        turn_id=None,
        installation_id=None,
        window_id=None,
        usage_alias=resolved_alias,
        usage_project=None,
        auth_fingerprint=None,
        account_id_hash=identity.account_id_hash,
        account_id_tail=identity.account_id_tail,
        identity_key=key,
    )


@dataclass
class UsageEvent:
    ts: str
    identity_key: str
    endpoint: str
    method: str
    model: str | None
    status_code: int
    ok: int
    duration_ms: int
    stream: int
    session_id: str | None
    thread_id: str | None
    turn_id: str | None
    installation_id: str | None
    window_id: str | None
    usage_alias: str | None
    usage_project: str | None
    auth_fingerprint: str | None
    account_id_hash: str | None
    account_id_tail: str | None
    input_tokens: int | None
    output_tokens: int | None
    cached_tokens: int | None
    cache_write_tokens: int | None
    reasoning_tokens: int | None
    total_tokens: int | None
    estimated_api_cost_usd: float | None
    non_cached_input_cost_usd: float | None
    cached_input_cost_usd: float | None
    output_cost_usd: float | None
    long_context_pricing_applied: int
    subscription_amortized_cost_usd: float | None
    api_equivalent_quota_usd: float | None
    usage_missing: int
    error_type: str | None
    error_message_redacted: str | None
    request_bytes: int
    response_bytes: int
    call_count: int = 1
    source: str = "sidecar"
    request_id: str | None = None
    # Some gateway responses happen before an upstream subscription is
    # selected. Keep them in usage history without treating them as an
    # upstream API response, account-pool attempt, or availability signal.
    account_attempt: int = 1


@dataclass(frozen=True)
class _PreparedImportedEvent:
    import_key: str
    source: str
    values: Mapping[str, Any]
    observation_action: str
    observation_values: Mapping[str, Any] | None


@dataclass(frozen=True)
class _ImportedEventState:
    import_record_exists: bool
    import_source: str | None
    usage_event_id: int | None
    usage: Mapping[str, Any] | None
    observation: Mapping[str, Any] | None


@dataclass(frozen=True)
class _ImportedEventDecision:
    prepared: _PreparedImportedEvent
    status: str
    values: Mapping[str, Any]
    changed_columns: tuple[str, ...]
    observation_changed: bool
    usage_event_id: int | None

    @property
    def needs_write(self) -> bool:
        return self.status in {"new", "changed"} or (
            self.status == "retired" and self.observation_changed
        )


def _decode_jwt_claims_unverified(value: Any) -> Mapping[str, Any]:
    """Read non-secret identity metadata from a local JWT without verifying it.

    The token itself is never returned, logged or persisted.  This is suitable
    only for matching two already-local Codex homes; it is not authentication.
    """

    if not isinstance(value, str) or value.count(".") < 2:
        return {}
    try:
        encoded = value.split(".", 2)[1]
        encoded += "=" * (-len(encoded) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(encoded.encode("ascii")))
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return decoded if isinstance(decoded, Mapping) else {}


def period_start(period: str, now: datetime | None = None) -> str:
    local_now = now or datetime.now().astimezone()
    if local_now.tzinfo is None:
        local_now = local_now.replace(tzinfo=timezone.utc)
    if str(period).strip().lower() in ALL_TIME_PERIODS:
        # SQLite stores UTC timestamps in lexicographically sortable ISO form.
        # A year-0001 sentinel gives an explicit, portable all-time query while
        # keeping the existing SQL shape and indexes intact.
        return PERIOD_START_SENTINEL
    if period == "today":
        start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        match = re.fullmatch(r"([1-9]\d*)d", period)
        if not match:
            raise ValueError("period must be 'today', 'all', or Nd, for example 7d")
        start = local_now - timedelta(days=int(match.group(1)))
    return start.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class UsageRepository:
    def __init__(self, path: Path | str):
        self.path = Path(path).expanduser().resolve()
        # A WAL checkpoint can be temporarily blocked by a live reader.  Keep
        # the retry state in process memory; startup privacy minimization also
        # retries after a restart, so no identifiable pages are abandoned.
        self._privacy_checkpoint_pending = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A repository can be opened directly (without the shell launcher),
        # and SQLite may inherit permissive modes from an older database.  Do
        # a best-effort owner-only pass before and after WAL setup so existing
        # databases and their sidecars receive the same privacy boundary.
        self._harden_storage_permissions()
        self.initialize()

    def _harden_storage_permissions(self) -> None:
        """Keep local SQLite files readable only by the current owner.

        ``umask`` in the launcher covers the creation race, while this pass
        repairs existing databases and sidecars for direct CLI/server starts.
        Windows ACLs do not map cleanly to POSIX mode bits, so the explicit
        chmod is intentionally a no-op there.  Detected sidecar symlinks are
        skipped; callers should provide a regular database path.
        """

        if os.name == "nt":
            return
        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
            Path(f"{self.path}-journal"),
        ):
            try:
                if candidate.is_symlink() or not candidate.is_file():
                    continue
                if candidate.stat().st_mode & 0o777 != 0o600:
                    candidate.chmod(0o600)
            except OSError as exc:
                # Do not include the local path in logs; it can contain an
                # account name.  The database remains usable, but the caller
                # gets a clear diagnostic if the OS rejects the hardening.
                LOG.warning("database permission hardening failed: %s", type(exc).__name__)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=15.0,
            factory=_ClosingSQLiteConnection,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA secure_delete=ON")
        self._harden_storage_permissions()
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            self._harden_storage_permissions()
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS usage_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT NOT NULL,
                  identity_key TEXT,
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
                  cache_write_tokens INTEGER,
                  reasoning_tokens INTEGER,
                  total_tokens INTEGER,
                  estimated_api_cost_usd REAL,
                  non_cached_input_cost_usd REAL,
                  cached_input_cost_usd REAL,
                  output_cost_usd REAL,
                  long_context_pricing_applied INTEGER DEFAULT 0,
                  subscription_amortized_cost_usd REAL,
                  api_equivalent_quota_usd REAL,
                  usage_missing INTEGER,
                  error_type TEXT,
                  error_message_redacted TEXT,
                  request_bytes INTEGER,
                  response_bytes INTEGER,
                  call_count INTEGER DEFAULT 1,
                  source TEXT DEFAULT 'sidecar',
                  request_id TEXT,
                  account_attempt INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS model_prices (
                  model_pattern TEXT PRIMARY KEY,
                  input_per_million REAL,
                  output_per_million REAL,
                  cached_input_per_million REAL,
                  cache_write_per_million REAL,
                  long_context_threshold_tokens INTEGER,
                  long_input_per_million REAL,
                  long_cached_input_per_million REAL,
                  long_cache_write_per_million REAL,
                  long_output_per_million REAL,
                  reasoning_per_million REAL,
                  currency TEXT,
                  source_note TEXT,
                  source_kind TEXT DEFAULT 'manual',
                  updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS price_sync_metadata (
                  id INTEGER PRIMARY KEY CHECK (id = 1),
                  source_url TEXT,
                  fetched_at TEXT,
                  content_sha256 TEXT,
                  parser_version TEXT,
                  status TEXT,
                  model_count INTEGER,
                  repriced_events INTEGER DEFAULT 0,
                  error_type TEXT,
                  error_message_redacted TEXT,
                  updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS quota_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT NOT NULL,
                  identity_key TEXT,
                  account_id_hash TEXT,
                  account_id_tail TEXT,
                  usage_alias TEXT,
                  event_type TEXT,
                  source TEXT,
                  raw_message_redacted TEXT
                );

                CREATE TABLE IF NOT EXISTS account_quota_cycles (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  identity_key TEXT,
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

                CREATE TABLE IF NOT EXISTS subscription_quota_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fetched_at TEXT NOT NULL,
                  identity_key TEXT NOT NULL,
                  account_id_hash TEXT,
                  account_id_tail TEXT,
                  usage_alias TEXT,
                  plan_type TEXT,
                  subscription_active_until TEXT,
                  window_kind TEXT NOT NULL,
                  used_percent REAL,
                  remaining_percent REAL,
                  window_seconds INTEGER,
                  reset_at TEXT,
                  estimated_full_quota_usd REAL,
                  estimated_remaining_quota_usd REAL,
                  estimate_method TEXT,
                  provider_allowed INTEGER,
                  provider_limit_reached INTEGER,
                  source TEXT NOT NULL,
                  UNIQUE(identity_key, window_kind, fetched_at)
                );

                CREATE TABLE IF NOT EXISTS local_import_records (
                  import_key TEXT PRIMARY KEY,
                  source TEXT NOT NULL,
                  usage_event_id INTEGER,
                  imported_at TEXT NOT NULL,
                  FOREIGN KEY(usage_event_id) REFERENCES usage_events(id)
                );

                CREATE TABLE IF NOT EXISTS local_import_files (
                  path TEXT PRIMARY KEY,
                  size INTEGER NOT NULL DEFAULT 0,
                  mtime_ns INTEGER NOT NULL DEFAULT 0,
                  offset INTEGER NOT NULL DEFAULT 0,
                  session_id TEXT,
                  model_provider TEXT,
                  model TEXT,
                  turn_id TEXT,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS local_import_bindings (
                  home_key TEXT PRIMARY KEY,
                  binding_key TEXT NOT NULL,
                  bound_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS remote_import_state (
                  source TEXT PRIMARY KEY,
                  last_complete_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_response_observations (
                  observation_key TEXT PRIMARY KEY,
                  minute_ts TEXT NOT NULL,
                  status_code INTEGER,
                  call_count INTEGER NOT NULL DEFAULT 1,
                  source TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_response_backfills (
                  source TEXT PRIMARY KEY,
                  version INTEGER NOT NULL,
                  completed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS anonymous_usage_daily (
                  bucket_start TEXT NOT NULL,
                  model TEXT NOT NULL,
                  status_code INTEGER NOT NULL,
                  ok INTEGER NOT NULL,
                  usage_missing INTEGER NOT NULL,
                  long_context_pricing_applied INTEGER NOT NULL,
                  split_priced INTEGER NOT NULL,
                  total_priced INTEGER NOT NULL,
                  calls INTEGER NOT NULL,
                  account_attempts INTEGER NOT NULL DEFAULT 0,
                  streaming_calls INTEGER NOT NULL DEFAULT 0,
                  non_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
                  codex_status_tokens INTEGER NOT NULL DEFAULT 0,
                  duration_ms INTEGER NOT NULL,
                  input_tokens INTEGER,
                  cached_tokens INTEGER,
                  cache_write_tokens INTEGER,
                  output_tokens INTEGER,
                  reasoning_tokens INTEGER,
                  total_tokens INTEGER,
                  estimated_api_cost_usd REAL,
                  non_cached_input_cost_usd REAL,
                  cached_input_cost_usd REAL,
                  output_cost_usd REAL,
                  PRIMARY KEY (
                    bucket_start, model, status_code, ok, usage_missing,
                    long_context_pricing_applied, split_priced, total_priced
                  )
                );

                CREATE TABLE IF NOT EXISTS active_subscription_registry (
                  identity_key TEXT PRIMARY KEY,
                  state TEXT NOT NULL CHECK (state IN ('active', 'suspect_missing')),
                  first_seen_at TEXT NOT NULL,
                  last_seen_at TEXT NOT NULL,
                  missing_since TEXT,
                  consecutive_misses INTEGER NOT NULL DEFAULT 0,
                  last_scan_generation INTEGER NOT NULL,
                  high_risk_missing INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS subscription_inventory_state (
                  id INTEGER PRIMARY KEY CHECK (id = 1),
                  initialized INTEGER NOT NULL DEFAULT 0,
                  generation INTEGER NOT NULL DEFAULT 0,
                  last_complete_at TEXT,
                  last_active_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS retired_subscription_tombstones (
                  identity_key TEXT PRIMARY KEY,
                  retired_at TEXT NOT NULL,
                  last_scan_generation INTEGER NOT NULL
                );
                """
            )
            self._ensure_column(conn, "usage_events", "identity_key", "TEXT")
            self._ensure_column(conn, "usage_events", "source", "TEXT DEFAULT 'sidecar'")
            self._ensure_column(conn, "usage_events", "request_id", "TEXT")
            # Very old databases did not persist the HTTP status.  Add the
            # nullable column before the Cockpit classification backfill
            # below; otherwise that migration would fail while inspecting an
            # otherwise valid legacy database.
            self._ensure_column(conn, "usage_events", "status_code", "INTEGER")
            self._ensure_column(
                conn,
                "usage_events",
                "account_attempt",
                "INTEGER NOT NULL DEFAULT 1",
            )
            self._ensure_column(conn, "usage_events", "non_cached_input_cost_usd", "REAL")
            self._ensure_column(conn, "usage_events", "cached_input_cost_usd", "REAL")
            self._ensure_column(conn, "usage_events", "output_cost_usd", "REAL")
            self._ensure_column(conn, "usage_events", "cache_write_tokens", "INTEGER")
            self._ensure_column(
                conn,
                "usage_events",
                "long_context_pricing_applied",
                "INTEGER DEFAULT 0",
            )
            self._ensure_column(conn, "model_prices", "source_kind", "TEXT DEFAULT 'manual'")
            self._ensure_column(conn, "model_prices", "long_context_threshold_tokens", "INTEGER")
            self._ensure_column(conn, "model_prices", "cache_write_per_million", "REAL")
            self._ensure_column(conn, "model_prices", "long_input_per_million", "REAL")
            self._ensure_column(conn, "model_prices", "long_cached_input_per_million", "REAL")
            self._ensure_column(conn, "model_prices", "long_cache_write_per_million", "REAL")
            self._ensure_column(conn, "model_prices", "long_output_per_million", "REAL")
            self._ensure_column(conn, "price_sync_metadata", "repriced_events", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "quota_events", "identity_key", "TEXT")
            self._ensure_column(conn, "account_quota_cycles", "identity_key", "TEXT")
            self._ensure_column(
                conn,
                "subscription_quota_snapshots",
                "provider_allowed",
                "INTEGER",
            )
            self._ensure_column(
                conn,
                "subscription_quota_snapshots",
                "provider_limit_reached",
                "INTEGER",
            )
            self._ensure_column(
                conn,
                "anonymous_usage_daily",
                "streaming_calls",
                "INTEGER NOT NULL DEFAULT 0",
            )
            anonymous_usage_columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(anonymous_usage_daily)")
            }
            had_anonymous_account_attempts = (
                "account_attempts" in anonymous_usage_columns
            )
            self._ensure_column(
                conn,
                "anonymous_usage_daily",
                "account_attempts",
                "INTEGER NOT NULL DEFAULT 0",
            )
            if not had_anonymous_account_attempts:
                # Legacy anonymous buckets predate account-selection
                # classification.  Calls were previously treated as account
                # attempts, so preserve that behavior unless the aggregate has
                # the uniquely repairable pre-account Cockpit 401 shape below.
                conn.execute(
                    "UPDATE anonymous_usage_daily SET account_attempts=calls"
                )
            # Privacy retirement removes the source column, but the historical
            # Cockpit rejects at issue here still have a provable shape: no
            # selected model, HTTP 401, and no token or cost consumption.  Keep
            # their API-call/failure history while removing them from account
            # attempts and model-consumption groupings.  Reapplying this repair
            # is safe and makes upgrades from an interrupted older build
            # deterministic.
            conn.execute(
                """
                UPDATE anonymous_usage_daily
                   SET account_attempts=0
                 WHERE model='(unknown)'
                   AND status_code=401
                   AND COALESCE(input_tokens, 0)=0
                   AND COALESCE(cached_tokens, 0)=0
                   AND COALESCE(cache_write_tokens, 0)=0
                   AND COALESCE(output_tokens, 0)=0
                   AND COALESCE(reasoning_tokens, 0)=0
                   AND COALESCE(total_tokens, 0)=0
                   AND COALESCE(non_cached_input_tokens, 0)=0
                   AND COALESCE(codex_status_tokens, 0)=0
                   AND COALESCE(estimated_api_cost_usd, 0)=0
                   AND COALESCE(non_cached_input_cost_usd, 0)=0
                   AND COALESCE(cached_input_cost_usd, 0)=0
                   AND COALESCE(output_cost_usd, 0)=0
                """
            )
            # Cockpit records client-authentication failures before it chooses
            # a subscription with an empty account selector.  Older meter
            # builds imported those rows as unknown account attempts.  Their
            # persisted zero-token shape is sufficient to repair the
            # classification without retaining Cockpit's raw selector or
            # error body.  The independent response ledger remains unchanged.
            conn.execute(
                """
                UPDATE usage_events
                   SET account_attempt=0
                 WHERE source='cockpit_tools'
                   AND status_code=401
                   AND (identity_key IS NULL OR identity_key='unknown')
                   AND model IS NULL
                   AND COALESCE(input_tokens, 0)=0
                   AND COALESCE(cached_tokens, 0)=0
                   AND COALESCE(output_tokens, 0)=0
                """
            )
            self._ensure_column(
                conn,
                "anonymous_usage_daily",
                "non_cached_input_tokens",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "anonymous_usage_daily",
                "codex_status_tokens",
                "INTEGER NOT NULL DEFAULT 0",
            )
            # Older anonymous buckets predate the exact per-event derived
            # counters.  Their original distribution cannot be recovered,
            # but retain the best aggregate lower bound instead of leaving a
            # misleading all-zero value.  Newly written exact zeroes cannot
            # satisfy this predicate with a positive aggregate expression.
            conn.execute(
                """UPDATE anonymous_usage_daily
                      SET non_cached_input_tokens=
                            MAX(COALESCE(input_tokens, 0)
                                - COALESCE(cached_tokens, 0), 0),
                          codex_status_tokens=
                            MAX(COALESCE(input_tokens, 0)
                                - COALESCE(cached_tokens, 0), 0)
                            + COALESCE(output_tokens, 0)
                    WHERE non_cached_input_tokens=0
                      AND codex_status_tokens=0
                      AND (MAX(COALESCE(input_tokens, 0)
                               - COALESCE(cached_tokens, 0), 0)
                           + COALESCE(output_tokens, 0))>0"""
            )
            self._ensure_column(
                conn,
                "active_subscription_registry",
                "high_risk_missing",
                "INTEGER NOT NULL DEFAULT 0",
            )
            # Cockpit's cache field names describe an older two-window UI,
            # but the embedded window duration is authoritative.  In current
            # builds the field named ``hourly`` can carry a seven-day or
            # monthly primary window.  Repair snapshots written by older meter
            # versions so a 604800-second window cannot remain mislabeled as
            # five-hour quota.  ``OR IGNORE`` plus the cleanup handles a cache
            # that already supplied the correctly classified sibling row.
            for window_kind, predicate in (
                ("five_hour", "window_seconds=18000"),
                ("weekly", "window_seconds=604800"),
                (
                    "monthly",
                    "window_seconds BETWEEN 2419200 AND 2678400",
                ),
            ):
                conn.execute(
                    f"""UPDATE OR IGNORE subscription_quota_snapshots
                           SET window_kind=?
                         WHERE source=? AND {predicate} AND window_kind!=?""",
                    (window_kind, COCKPIT_TOOLS_QUOTA_SOURCE, window_kind),
                )
                conn.execute(
                    f"""DELETE FROM subscription_quota_snapshots
                         WHERE source=? AND {predicate} AND window_kind!=?""",
                    (COCKPIT_TOOLS_QUOTA_SOURCE, window_kind),
                )
            conn.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_usage_events_ts ON usage_events(ts);
                CREATE INDEX IF NOT EXISTS idx_usage_events_identity_ts ON usage_events(identity_key, ts);
                CREATE INDEX IF NOT EXISTS idx_usage_events_alias_ts ON usage_events(usage_alias, ts);
                CREATE INDEX IF NOT EXISTS idx_usage_events_model_ts ON usage_events(model, ts);
                CREATE INDEX IF NOT EXISTS idx_quota_events_identity_ts ON quota_events(identity_key, ts);
                CREATE INDEX IF NOT EXISTS idx_quota_cycles_identity_end ON account_quota_cycles(identity_key, cycle_end_ts);
                CREATE INDEX IF NOT EXISTS idx_subscription_quota_identity_kind_ts
                  ON subscription_quota_snapshots(identity_key, window_kind, fetched_at DESC);
                CREATE INDEX IF NOT EXISTS idx_local_import_records_source
                  ON local_import_records(source, imported_at DESC);
                CREATE INDEX IF NOT EXISTS idx_local_import_files_updated
                  ON local_import_files(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_local_import_bindings_updated
                  ON local_import_bindings(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_api_response_observations_minute_source
                  ON api_response_observations(minute_ts, source);
                CREATE INDEX IF NOT EXISTS idx_anonymous_usage_daily_bucket
                  ON anonymous_usage_daily(bucket_start);
                CREATE INDEX IF NOT EXISTS idx_active_subscription_registry_state
                  ON active_subscription_registry(state, consecutive_misses);
                CREATE INDEX IF NOT EXISTS idx_retired_subscription_tombstones_at
                  ON retired_subscription_tombstones(retired_at);
                """
            )
            # A pre-account response is a local gateway decision, not an
            # upstream API response.  Remove observations created by older
            # builds before reseeding the identity-free ledger.  Imported rows
            # whose detail was already privacy-retired are cleaned by the
            # versioned Cockpit raw-log replay below.
            conn.execute(
                """
                DELETE FROM api_response_observations
                 WHERE observation_key IN (
                       SELECT 'usage:' || id
                         FROM usage_events
                        WHERE COALESCE(account_attempt, 1)=0
                       UNION
                       SELECT 'import:' || imports.import_key
                         FROM local_import_records imports
                         JOIN usage_events events
                           ON events.id=imports.usage_event_id
                        WHERE COALESCE(events.account_attempt, 1)=0
                 )
                """
            )
            # Seed the identity-free response ledger from every API event that
            # still has a live detail row.  Imported events use their stable,
            # opaque import key so future repricing scans update the same
            # observation.  Cockpit's one-time raw-log backfill restores
            # observations whose account detail was already privacy-retired.
            usage_event_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(usage_events)")
            }
            response_columns = {"id", "ts", "status_code", "call_count", "source"}
            if response_columns <= usage_event_columns:
                placeholders = ",".join("?" for _ in RESPONSE_TIMELINE_SOURCES)
                conn.execute(
                    f"""
                    INSERT INTO api_response_observations (
                      observation_key, minute_ts, status_code, call_count, source
                    )
                    SELECT CASE WHEN imported.import_key IS NOT NULL
                                  THEN 'import:' || imported.import_key
                                  ELSE 'usage:' || usage_events.id END,
                           strftime('%Y-%m-%dT%H:%M:00Z', usage_events.ts),
                           usage_events.status_code,
                           MAX(COALESCE(usage_events.call_count, 1), 1),
                           usage_events.source
                      FROM usage_events
                      LEFT JOIN local_import_records imported
                        ON imported.usage_event_id=usage_events.id
                     WHERE usage_events.source IN ({placeholders})
                       AND COALESCE(usage_events.account_attempt, 1)=1
                       AND strftime('%Y-%m-%dT%H:%M:00Z', usage_events.ts) IS NOT NULL
                    ON CONFLICT(observation_key) DO UPDATE SET
                      minute_ts=excluded.minute_ts,
                      status_code=excluded.status_code,
                      call_count=excluded.call_count,
                      source=excluded.source
                    WHERE api_response_observations.minute_ts IS NOT excluded.minute_ts
                       OR api_response_observations.status_code IS NOT excluded.status_code
                       OR api_response_observations.call_count IS NOT excluded.call_count
                       OR api_response_observations.source IS NOT excluded.source
                    """,
                    RESPONSE_TIMELINE_SOURCES,
                )
            conn.executescript(
                """
                DROP VIEW IF EXISTS usage_statistics;
                CREATE VIEW usage_statistics AS
                SELECT id, ts, identity_key, endpoint, method, model,
                       status_code, ok, duration_ms, stream, session_id,
                       thread_id, turn_id, installation_id, window_id,
                       usage_alias, usage_project, auth_fingerprint,
                       account_id_hash, account_id_tail, input_tokens,
                       output_tokens, cached_tokens, cache_write_tokens,
                       reasoning_tokens, total_tokens, estimated_api_cost_usd,
                       non_cached_input_cost_usd, cached_input_cost_usd,
                       output_cost_usd, long_context_pricing_applied,
                       subscription_amortized_cost_usd,
                       api_equivalent_quota_usd, usage_missing, error_type,
                       error_message_redacted, request_bytes, response_bytes,
                       call_count, source, request_id,
                       CASE WHEN COALESCE(account_attempt, 1)=1
                            THEN call_count ELSE 0 END AS account_attempt_count,
                       CASE WHEN stream=1 THEN call_count ELSE 0 END
                         AS streaming_call_count,
                       MAX(COALESCE(input_tokens, 0)
                           - COALESCE(cached_tokens, 0), 0)
                         AS non_cached_input_token_count,
                       MAX(COALESCE(input_tokens, 0)
                           - COALESCE(cached_tokens, 0), 0)
                         + COALESCE(output_tokens, 0)
                         AS codex_status_token_count
                  FROM usage_events
                UNION ALL
                SELECT -rowid, bucket_start, NULL, NULL, NULL,
                       NULLIF(model, '(unknown)'), status_code, ok,
                       duration_ms, 0, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, NULL, input_tokens,
                       output_tokens, cached_tokens, cache_write_tokens,
                       reasoning_tokens, total_tokens, estimated_api_cost_usd,
                       non_cached_input_cost_usd, cached_input_cost_usd,
                       output_cost_usd, long_context_pricing_applied,
                       NULL, NULL, usage_missing, NULL, NULL, 0, 0,
                       calls, 'anonymous', NULL, account_attempts,
                       streaming_calls,
                       non_cached_input_tokens, codex_status_tokens
                  FROM anonymous_usage_daily;
                """
            )
            self._correct_astra_context_costs(conn)
            self._backfill_frozen_cost_components(conn)
            self._upgrade_long_context_costs(conn)

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, declaration: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    @staticmethod
    def _matched_price(model: str | None, rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        if not model:
            return None
        matches = [row for row in rows if fnmatch.fnmatchcase(model, row["model_pattern"])]
        if not matches:
            return None
        matches.sort(key=lambda row: (row["model_pattern"] == model, len(row["model_pattern"])), reverse=True)
        return matches[0]

    @staticmethod
    def _components_for_price(
        usage: NormalizedUsage,
        price: Mapping[str, Any] | None,
    ) -> PriceComponents | None:
        if price is None or usage.missing:
            return None
        # A total-only usage object cannot be priced without inventing an
        # input/output split; keep every cost field NULL instead of returning 0.
        if usage.input_tokens is None or usage.output_tokens is None:
            return None
        price = dict(price)
        input_tokens = usage.input_tokens
        output_tokens = usage.output_tokens
        cached_tokens = usage.cached_tokens or 0
        cache_write_tokens = usage.cache_write_tokens or 0
        ordinary_input_tokens = max(input_tokens - cached_tokens - cache_write_tokens, 0)
        long_context = False
        threshold = as_nonnegative_int(price.get("long_context_threshold_tokens"))
        if (
            price.get("model_pattern") != "gpt-6-astra"
            and threshold is not None
            and input_tokens > threshold
        ):
            long_rates = (
                price.get("long_input_per_million"),
                price.get("long_cached_input_per_million"),
                price.get("long_cache_write_per_million"),
                price.get("long_output_per_million"),
            )
            # A partially documented long-context tier is not safe to infer.
            # Use it only when every token category needed by this event has a
            # corresponding official rate; otherwise leave the event unpriced.
            if (
                (ordinary_input_tokens > 0 and long_rates[0] is None)
                or (cached_tokens > 0 and long_rates[1] is None)
                or (cache_write_tokens > 0 and long_rates[2] is None)
                or (output_tokens > 0 and long_rates[3] is None)
            ):
                return None
            raw_rates = long_rates
            long_context = True
        else:
            raw_rates = (
                price["input_per_million"],
                price["cached_input_per_million"],
                price.get("cache_write_per_million"),
                price["output_per_million"],
            )
        rates: list[float | None] = []
        for raw in raw_rates:
            if raw is None:
                rates.append(None)
                continue
            try:
                rate = float(raw)
            except (TypeError, ValueError, OverflowError):
                return None
            if not math.isfinite(rate) or rate < 0:
                return None
            rates.append(rate)
        input_rate, cached_rate, cache_write_rate, output_rate = rates
        if ordinary_input_tokens and input_rate is None:
            return None
        if cached_tokens and cached_rate is None:
            return None
        if cache_write_tokens and cache_write_rate is None:
            return None
        if output_tokens and output_rate is None:
            return None
        return PriceComponents(
            non_cached_input_cost_usd=(
                (
                    ordinary_input_tokens * float(input_rate or 0.0)
                    + cache_write_tokens * float(cache_write_rate or 0.0)
                )
                / 1_000_000
            ),
            cached_input_cost_usd=(
                cached_tokens * float(cached_rate or 0.0) / 1_000_000
            ),
            output_cost_usd=output_tokens * float(output_rate or 0.0) / 1_000_000,
            long_context_pricing_applied=long_context,
        )

    def price_components_for(
        self,
        model: str | None,
        usage: NormalizedUsage,
    ) -> PriceComponents | None:
        if not model or usage.missing:
            return None
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM model_prices WHERE currency = 'USD' OR currency IS NULL"
            ).fetchall()
        price = self._matched_price(model, rows)
        return self._components_for_price(usage, price)

    def price_for(self, model: str | None, usage: NormalizedUsage) -> float | None:
        components = self.price_components_for(model, usage)
        return components.total_cost_usd if components is not None else None

    @staticmethod
    def _verified_astra_context_components(
        frozen: PriceComponents,
        short: PriceComponents | None,
    ) -> PriceComponents:
        """Correct only snapshots matching Astra's short or erroneous long rates."""

        if short is None:
            return frozen
        actual = (
            frozen.non_cached_input_cost_usd,
            frozen.cached_input_cost_usd,
            frozen.output_cost_usd,
        )
        for input_factor, output_factor in ((1.0, 1.0), (2.0, 1.5)):
            expected = (
                short.non_cached_input_cost_usd * input_factor,
                short.cached_input_cost_usd * input_factor,
                short.output_cost_usd * output_factor,
            )
            if all(
                math.isclose(value, expected_cost, rel_tol=1e-9, abs_tol=1e-12)
                for value, expected_cost in zip(actual, expected)
            ):
                return short
        return frozen

    def correct_astra_context_components(
        self,
        model: str | None,
        usage: NormalizedUsage,
        components: PriceComponents | None,
    ) -> PriceComponents | None:
        if (
            model != "gpt-6-astra"
            or components is None
            or not components.long_context_pricing_applied
            or (usage.input_tokens or 0) <= LONG_CONTEXT_THRESHOLD_TOKENS
        ):
            return components
        return self._verified_astra_context_components(
            components, self.price_components_for(model, usage)
        )

    def _correct_astra_context_costs(self, conn: sqlite3.Connection) -> int:
        """Repair the known Astra surcharge while preserving unrelated snapshots."""

        price = conn.execute(
            """SELECT * FROM model_prices WHERE model_pattern='gpt-6-astra'
                 AND source_kind='official' AND (currency='USD' OR currency IS NULL)"""
        ).fetchone()
        if price is None:
            return 0
        conn.execute(
            """UPDATE model_prices SET long_context_threshold_tokens=NULL,
                      long_input_per_million=NULL, long_cached_input_per_million=NULL,
                      long_cache_write_per_million=NULL, long_output_per_million=NULL,
                      source_note=COALESCE(source_note, '') ||
                          '; corrected gpt-6-astra to flat context rates', updated_at=?
                WHERE model_pattern='gpt-6-astra'
                  AND (long_context_threshold_tokens IS NOT NULL
                       OR long_input_per_million IS NOT NULL
                       OR long_cached_input_per_million IS NOT NULL
                       OR long_cache_write_per_million IS NOT NULL
                       OR long_output_per_million IS NOT NULL)""",
            (utc_now(),),
        )
        events = conn.execute(
            """SELECT id, input_tokens, cached_tokens, cache_write_tokens, output_tokens,
                      estimated_api_cost_usd, non_cached_input_cost_usd,
                      cached_input_cost_usd, output_cost_usd
                 FROM usage_events
                WHERE model='gpt-6-astra' AND long_context_pricing_applied=1
                  AND input_tokens>? AND output_tokens IS NOT NULL
                  AND estimated_api_cost_usd IS NOT NULL
                  AND non_cached_input_cost_usd IS NOT NULL
                  AND cached_input_cost_usd IS NOT NULL AND output_cost_usd IS NOT NULL""",
            (LONG_CONTEXT_THRESHOLD_TOKENS,),
        ).fetchall()
        updated = 0
        for event in events:
            usage = NormalizedUsage(
                input_tokens=event["input_tokens"], output_tokens=event["output_tokens"],
                cached_tokens=event["cached_tokens"], cache_write_tokens=event["cache_write_tokens"],
            )
            frozen = PriceComponents(
                non_cached_input_cost_usd=event["non_cached_input_cost_usd"],
                cached_input_cost_usd=event["cached_input_cost_usd"],
                output_cost_usd=event["output_cost_usd"],
                long_context_pricing_applied=True,
            )
            if not math.isclose(
                frozen.total_cost_usd, event["estimated_api_cost_usd"],
                rel_tol=1e-9, abs_tol=1e-12,
            ):
                continue
            corrected = self._verified_astra_context_components(
                frozen, self._components_for_price(usage, price)
            )
            if corrected is frozen:
                continue
            conn.execute(
                """UPDATE usage_events SET non_cached_input_cost_usd=?,
                          cached_input_cost_usd=?, output_cost_usd=?,
                          estimated_api_cost_usd=?, long_context_pricing_applied=0
                    WHERE id=?""",
                (corrected.non_cached_input_cost_usd, corrected.cached_input_cost_usd,
                 corrected.output_cost_usd, corrected.total_cost_usd, event["id"]),
            )
            updated += 1
        return updated

    def _backfill_frozen_cost_components(self, conn: sqlite3.Connection) -> int:
        """Safely split legacy totals without changing their frozen value.

        A legacy row is filled only when all three new fields are NULL and the
        current component calculation agrees with its already-persisted total
        to sub-nanodollar precision. Price drift therefore leaves the row
        untouched instead of silently rewriting history.
        """

        prices = conn.execute(
            "SELECT * FROM model_prices WHERE currency = 'USD' OR currency IS NULL"
        ).fetchall()
        if not prices:
            return 0
        events = conn.execute(
            """
            SELECT id, model, input_tokens, cached_tokens, cache_write_tokens, output_tokens,
                   estimated_api_cost_usd
              FROM usage_events
             WHERE estimated_api_cost_usd IS NOT NULL
               AND input_tokens IS NOT NULL
               AND output_tokens IS NOT NULL
               AND non_cached_input_cost_usd IS NULL
               AND cached_input_cost_usd IS NULL
               AND output_cost_usd IS NULL
            """
        ).fetchall()
        filled = 0
        for event in events:
            usage = NormalizedUsage(
                input_tokens=as_nonnegative_int(event["input_tokens"]),
                output_tokens=as_nonnegative_int(event["output_tokens"]),
                cached_tokens=as_nonnegative_int(event["cached_tokens"]),
                cache_write_tokens=as_nonnegative_int(event["cache_write_tokens"]),
            )
            components = self._components_for_price(
                usage,
                self._matched_price(event["model"], prices),
            )
            if components is None:
                continue
            frozen_total = float(event["estimated_api_cost_usd"])
            if not math.isfinite(frozen_total) or not math.isclose(
                components.total_cost_usd,
                frozen_total,
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                continue
            cursor = conn.execute(
                """
                UPDATE usage_events
                   SET non_cached_input_cost_usd=?,
                       cached_input_cost_usd=?,
                       output_cost_usd=?
                 WHERE id=?
                   AND estimated_api_cost_usd=?
                   AND non_cached_input_cost_usd IS NULL
                   AND cached_input_cost_usd IS NULL
                   AND output_cost_usd IS NULL
                """,
                (
                    components.non_cached_input_cost_usd,
                    components.cached_input_cost_usd,
                    components.output_cost_usd,
                    event["id"],
                    event["estimated_api_cost_usd"],
                ),
            )
            filled += max(int(cursor.rowcount), 0)
        return filled

    def backfill_frozen_cost_components(self) -> int:
        """Public, idempotent entry point used by migration diagnostics."""

        with self.connect() as conn:
            return self._backfill_frozen_cost_components(conn)

    def _upgrade_long_context_costs(self, conn: sqlite3.Connection) -> int:
        """Upgrade frozen short-tier totals when the old total proves their origin.

        Older versions stored only short-context rates.  A row is rewritten
        only when all frozen components exactly match the current short tier
        and the model price now supplies a complete long tier.  This preserves
        manual/historical totals whose provenance cannot be established.
        """

        prices = conn.execute(
            """SELECT * FROM model_prices
                WHERE (currency='USD' OR currency IS NULL)
                  AND model_pattern<>'gpt-6-astra'
                  AND long_context_threshold_tokens IS NOT NULL"""
        ).fetchall()
        if not prices:
            return 0
        events = conn.execute(
            """
            SELECT id, model, input_tokens, cached_tokens, cache_write_tokens, output_tokens,
                   estimated_api_cost_usd, non_cached_input_cost_usd,
                   cached_input_cost_usd, output_cost_usd
              FROM usage_events
             WHERE COALESCE(long_context_pricing_applied, 0)=0
               AND input_tokens IS NOT NULL
               AND output_tokens IS NOT NULL
               AND estimated_api_cost_usd IS NOT NULL
               AND non_cached_input_cost_usd IS NOT NULL
               AND cached_input_cost_usd IS NOT NULL
               AND output_cost_usd IS NOT NULL
            """
        ).fetchall()
        updated = 0
        for event in events:
            price = self._matched_price(event["model"], prices)
            if price is None:
                continue
            threshold = as_nonnegative_int(price["long_context_threshold_tokens"])
            input_tokens = as_nonnegative_int(event["input_tokens"])
            if threshold is None or input_tokens is None or input_tokens <= threshold:
                continue
            usage = NormalizedUsage(
                input_tokens=input_tokens,
                output_tokens=as_nonnegative_int(event["output_tokens"]),
                cached_tokens=as_nonnegative_int(event["cached_tokens"]),
                cache_write_tokens=as_nonnegative_int(event["cache_write_tokens"]),
            )
            short_price = dict(price)
            short_price["long_context_threshold_tokens"] = None
            short_components = self._components_for_price(usage, short_price)
            long_components = self._components_for_price(usage, price)
            if short_components is None or long_components is None:
                continue
            frozen = (
                float(event["non_cached_input_cost_usd"]),
                float(event["cached_input_cost_usd"]),
                float(event["output_cost_usd"]),
                float(event["estimated_api_cost_usd"]),
            )
            expected_short = (
                short_components.non_cached_input_cost_usd,
                short_components.cached_input_cost_usd,
                short_components.output_cost_usd,
                short_components.total_cost_usd,
            )
            if not all(
                math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
                for actual, expected in zip(frozen, expected_short)
            ):
                continue
            cursor = conn.execute(
                """
                UPDATE usage_events
                   SET non_cached_input_cost_usd=?, cached_input_cost_usd=?,
                       output_cost_usd=?, estimated_api_cost_usd=?,
                       long_context_pricing_applied=1
                 WHERE id=? AND COALESCE(long_context_pricing_applied, 0)=0
                """,
                (
                    long_components.non_cached_input_cost_usd,
                    long_components.cached_input_cost_usd,
                    long_components.output_cost_usd,
                    long_components.total_cost_usd,
                    event["id"],
                ),
            )
            updated += max(int(cursor.rowcount), 0)
        return updated

    def upgrade_long_context_costs(self) -> int:
        """Public, idempotent long-context pricing migration entry point."""

        with self.connect() as conn:
            return self._upgrade_long_context_costs(conn)

    def reconcile_auth_identities(self, resolver: AccountResolver) -> int:
        """Re-bind provisional rows only when current member evidence proves them.

        CLIProxyAPI may publish a usage-queue item in the same moment that it
        refreshes an auth file.  If the resolver scans before that file is
        complete, the event is stored as ``alias:auth:<fingerprint>``.  That
        identity would otherwise remain a permanent, misleading ``UNKNOWN``
        dashboard card even after the file is available. Team members share a
        workspace account id, so an account hash by itself never selects a
        member. Only a still-resolvable provider-token fingerprint or explicit
        Codex alias can prove the canonical subscription; ambiguous legacy rows
        are later folded into anonymous token totals.
        """

        if not resolver.enabled:
            return 0
        scoped_account_hashes = sorted(resolver.subscription_account_hashes())
        privacy_rows_removed = False
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for tombstone in conn.execute(
                "SELECT identity_key FROM retired_subscription_tombstones"
            ).fetchall():
                removed = self._anonymize_subscription_conn(conn, tombstone[0])
                privacy_rows_removed = privacy_rows_removed or any(removed.values())
            for ambiguous_key in sorted(resolver.ambiguous_legacy_identity_keys()):
                removed = self._anonymize_subscription_conn(conn, ambiguous_key)
                privacy_rows_removed = privacy_rows_removed or any(removed.values())
            collision_clause = ""
            collision_params: tuple[str, ...] = ()
            if scoped_account_hashes:
                placeholders = ",".join("?" for _ in scoped_account_hashes)
                collision_clause = f"""
                    OR (
                         identity_key NOT LIKE 'subscription:%'
                         AND account_id_hash IN ({placeholders})
                         AND (auth_fingerprint IS NOT NULL OR usage_alias IS NOT NULL)
                       )
                """
                collision_params = tuple(scoped_account_hashes)
            rows = conn.execute(
                f"""
                SELECT id, identity_key, usage_alias, auth_fingerprint, source,
                       account_id_hash, account_id_tail
                 FROM usage_events
                 WHERE (
                       auth_fingerprint IS NOT NULL
                       AND (
                         usage_alias LIKE 'auth:%'
                         OR identity_key LIKE 'alias:auth:%'
                         OR account_id_hash IS NULL
                       )
                      )
                      {collision_clause}
                """,
                collision_params,
            ).fetchall()
            changed = 0
            related: dict[str, tuple[str, str | None, str | None, str | None]] = {}
            for row in rows:
                fingerprint = safe_text(row["auth_fingerprint"], 64)
                current_alias = safe_text(row["usage_alias"], 128)
                identity = resolver.resolve(
                    None,
                    fingerprint.lower()[:16] if fingerprint else None,
                )
                if (
                    not identity.account_id_hash
                    and not identity.usage_alias
                    and current_alias
                    and not is_auth_fallback_alias(current_alias)
                    and row["source"] != "usage_queue"
                ):
                    identity = resolver.resolve(current_alias, None)
                if not identity.subscription_id_hash:
                    continue
                if (
                    row["account_id_hash"] in scoped_account_hashes
                    and not identity.subscription_id_hash
                ):
                    # A legacy queue token can rotate out of the local auth
                    # set.  Its old alias was account-derived and is not safe
                    # evidence for choosing one Team member.
                    continue
                resolved_alias = identity.usage_alias
                resolved_key = resolved_identity_key(
                    identity,
                    resolved_alias,
                    fingerprint.lower()[:16] if fingerprint else None,
                )
                if not self._subscription_detail_allowed_conn(conn, resolved_key):
                    conn.execute(
                        "UPDATE usage_events SET identity_key='unknown' WHERE id=?",
                        (row["id"],),
                    )
                    changed += 1
                    continue
                if (
                    row["identity_key"] == resolved_key
                    and row["usage_alias"] == resolved_alias
                    and row["account_id_hash"] == identity.account_id_hash
                    and row["account_id_tail"] == identity.account_id_tail
                ):
                    continue
                cursor = conn.execute(
                    """
                    UPDATE usage_events
                       SET identity_key=?, usage_alias=?, account_id_hash=?, account_id_tail=?
                     WHERE id=?
                    """,
                    (
                        resolved_key,
                        resolved_alias,
                        identity.account_id_hash,
                        identity.account_id_tail,
                        row["id"],
                    ),
                )
                changed += max(int(cursor.rowcount), 0)
                old_key = safe_text(row["identity_key"], 300)
                if old_key and (
                    old_key.startswith("alias:auth:") or old_key.startswith("auth:")
                ):
                    related[old_key] = (
                        resolved_key,
                        resolved_alias,
                        identity.account_id_hash,
                        identity.account_id_tail,
                    )

            # Keep quota markers/cycles aligned if a provisional identity had
            # already produced one.  ``UPDATE OR IGNORE`` avoids violating the
            # snapshot uniqueness constraint when a canonical row exists.
            for old_key, (new_key, alias, account_hash, account_tail) in related.items():
                for table in ("quota_events", "account_quota_cycles"):
                    conn.execute(
                        f"""
                        UPDATE {table}
                           SET identity_key=?, usage_alias=?, account_id_hash=?, account_id_tail=?
                         WHERE identity_key=?
                        """,
                        (new_key, alias, account_hash, account_tail, old_key),
                    )
                conn.execute(
                    """
                    UPDATE OR IGNORE subscription_quota_snapshots
                       SET identity_key=?, usage_alias=?, account_id_hash=?, account_id_tail=?
                     WHERE identity_key=?
                    """,
                    (new_key, alias, account_hash, account_tail, old_key),
                )

            # Quota markers can outlive the usage row that first created them.
            # Reconcile those rows independently as well, using the embedded
            # auth fallback label or their already persisted account hash.
            for table in ("quota_events", "account_quota_cycles", "subscription_quota_snapshots"):
                related_rows = conn.execute(
                    f"""
                    SELECT id, identity_key, usage_alias, account_id_hash, account_id_tail
                     FROM {table}
                     WHERE usage_alias LIKE 'auth:%'
                        OR identity_key LIKE 'alias:auth:%'
                    """
                ).fetchall()
                for row in related_rows:
                    alias_text = safe_text(row["usage_alias"], 128)
                    key_text = safe_text(row["identity_key"], 300) or ""
                    fingerprint = None
                    if is_auth_fallback_alias(alias_text):
                        fingerprint = alias_text[5:]
                    else:
                        match = re.fullmatch(r"alias:(auth:[0-9a-f]{16})", key_text, re.IGNORECASE)
                        if match:
                            fingerprint = match.group(1)[5:]
                    identity = resolver.resolve(None, fingerprint.lower() if fingerprint else None)
                    if not identity.subscription_id_hash:
                        continue
                    resolved_alias = identity.usage_alias
                    resolved_key = resolved_identity_key(
                        identity,
                        resolved_alias,
                        fingerprint.lower() if fingerprint else None,
                    )
                    if not self._subscription_detail_allowed_conn(conn, resolved_key):
                        conn.execute(f"DELETE FROM {table} WHERE id=?", (row["id"],))
                        changed += 1
                        continue
                    if (
                        row["identity_key"] == resolved_key
                        and row["usage_alias"] == resolved_alias
                        and row["account_id_hash"] == identity.account_id_hash
                        and row["account_id_tail"] == identity.account_id_tail
                    ):
                        continue
                    conn.execute(
                        f"""
                        UPDATE OR IGNORE {table}
                           SET identity_key=?, usage_alias=?, account_id_hash=?, account_id_tail=?
                         WHERE id=?
                        """,
                        (
                            resolved_key,
                            resolved_alias,
                            identity.account_id_hash,
                            identity.account_id_tail,
                            row["id"],
                        ),
                    )
                    changed += 1
            for old_key, new_key in resolver.identity_migrations().items():
                if old_key == new_key:
                    continue
                if not self._prepare_proven_identity_migration_conn(
                    conn, old_key, new_key
                ):
                    removed = self._anonymize_subscription_conn(conn, old_key)
                    privacy_rows_removed = privacy_rows_removed or any(removed.values())
                    changed += int(removed.get("usage_events") or 0)
                    continue
                changed += max(
                    int(
                        conn.execute(
                            "UPDATE usage_events SET identity_key=? WHERE identity_key=?",
                            (new_key, old_key),
                        ).rowcount
                    ),
                    0,
                )
                for table in ("quota_events", "account_quota_cycles"):
                    conn.execute(
                        f"UPDATE {table} SET identity_key=? WHERE identity_key=?",
                        (new_key, old_key),
                    )
                conn.execute(
                    """UPDATE OR IGNORE subscription_quota_snapshots
                          SET identity_key=? WHERE identity_key=?""",
                    (new_key, old_key),
                )
                conn.execute(
                    "DELETE FROM subscription_quota_snapshots WHERE identity_key=?",
                    (old_key,),
                )
            conn.execute(
                """UPDATE usage_events SET endpoint=NULL, method=NULL,
                     session_id=NULL, thread_id=NULL, turn_id=NULL,
                     installation_id=NULL, window_id=NULL, usage_alias=NULL,
                     usage_project=NULL, auth_fingerprint=NULL,
                     account_id_hash=NULL, account_id_tail=NULL,
                     error_type=NULL, error_message_redacted=NULL, request_bytes=0,
                     response_bytes=0, request_id=NULL
                   WHERE endpoint IS NOT NULL OR method IS NOT NULL
                      OR session_id IS NOT NULL OR thread_id IS NOT NULL
                      OR turn_id IS NOT NULL OR installation_id IS NOT NULL
                      OR window_id IS NOT NULL OR usage_alias IS NOT NULL
                      OR usage_project IS NOT NULL OR auth_fingerprint IS NOT NULL
                      OR account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR error_type IS NOT NULL OR error_message_redacted IS NOT NULL
                      OR COALESCE(request_bytes, 0)!=0
                      OR COALESCE(response_bytes, 0)!=0
                      OR request_id IS NOT NULL"""
            )
            conn.execute(
                """UPDATE quota_events SET account_id_hash=NULL,
                     account_id_tail=NULL, usage_alias=NULL,
                     raw_message_redacted=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL OR raw_message_redacted IS NOT NULL"""
            )
            conn.execute(
                """UPDATE account_quota_cycles SET account_id_hash=NULL,
                     account_id_tail=NULL, usage_alias=NULL, notes=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL OR notes IS NOT NULL"""
            )
            conn.execute(
                """UPDATE subscription_quota_snapshots SET
                     account_id_hash=NULL, account_id_tail=NULL,
                     usage_alias=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL"""
            )
        if privacy_rows_removed or self._privacy_checkpoint_pending:
            self._checkpoint_privacy_wal("auth identity reconciliation")
        return changed

    @staticmethod
    def _add_nullable(existing: str, incoming: str) -> str:
        return (
            f"CASE WHEN {existing} IS NULL AND {incoming} IS NULL THEN NULL "
            f"ELSE COALESCE({existing}, 0) + COALESCE({incoming}, 0) END"
        )

    def _prepare_proven_identity_migration_conn(
        self,
        conn: sqlite3.Connection,
        old_key: str,
        new_key: str,
    ) -> bool:
        """Authorize a resolver-proven lineage without bypassing inventory.

        Before an authoritative inventory exists, the resolver lineage is the
        only local evidence available.  Once inventory is initialized, only a
        currently active old registry key may transfer that authorization to
        its proven successor.  Suspect or tombstoned identities never revive
        through a local migration.
        """

        if old_key == new_key:
            return True
        if conn.execute(
            """SELECT 1 FROM retired_subscription_tombstones
                 WHERE identity_key IN (?, ?) LIMIT 1""",
            (old_key, new_key),
        ).fetchone():
            return False
        inventory = conn.execute(
            "SELECT initialized FROM subscription_inventory_state WHERE id=1"
        ).fetchone()
        if not inventory or not inventory["initialized"]:
            return True
        old_row = conn.execute(
            "SELECT * FROM active_subscription_registry WHERE identity_key=?",
            (old_key,),
        ).fetchone()
        if old_row is None or old_row["state"] != "active":
            return False
        new_row = conn.execute(
            "SELECT * FROM active_subscription_registry WHERE identity_key=?",
            (new_key,),
        ).fetchone()
        if new_row is not None and new_row["state"] != "active":
            return False
        if new_row is None:
            conn.execute(
                """UPDATE active_subscription_registry
                      SET identity_key=?
                    WHERE identity_key=? AND state='active'""",
                (new_key, old_key),
            )
        else:
            conn.execute(
                """UPDATE active_subscription_registry
                      SET first_seen_at=MIN(first_seen_at, ?),
                          last_seen_at=MAX(last_seen_at, ?),
                          last_scan_generation=MAX(last_scan_generation, ?)
                    WHERE identity_key=?""",
                (
                    old_row["first_seen_at"],
                    old_row["last_seen_at"],
                    old_row["last_scan_generation"],
                    new_key,
                ),
            )
            conn.execute(
                "DELETE FROM active_subscription_registry WHERE identity_key=?",
                (old_key,),
            )
        return self._subscription_detail_allowed_conn(conn, new_key)

    def _anonymize_subscription_conn(
        self,
        conn: sqlite3.Connection,
        identity_key_value: Any,
    ) -> dict[str, int]:
        """Aggregate one retired identity and remove all linkable detail."""

        unsafe_models = [
            row[0]
            for row in conn.execute(
                """SELECT DISTINCT model FROM usage_events
                     WHERE identity_key=? AND model IS NOT NULL""",
                (identity_key_value,),
            ).fetchall()
            if safe_model_identifier(row[0]) is None
        ]
        for unsafe_model in unsafe_models:
            conn.execute(
                "UPDATE usage_events SET model=NULL WHERE identity_key=? AND model=?",
                (identity_key_value, unsafe_model),
            )
        event_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM usage_events WHERE identity_key=?",
                (identity_key_value,),
            ).fetchone()[0]
        )
        nullable_columns = (
            "input_tokens",
            "cached_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
            "total_tokens",
            "estimated_api_cost_usd",
            "non_cached_input_cost_usd",
            "cached_input_cost_usd",
            "output_cost_usd",
        )
        update_columns = ",\n".join(
            f"{column}={self._add_nullable(f'anonymous_usage_daily.{column}', f'excluded.{column}')}"
            for column in nullable_columns
        )
        conn.execute(
            f"""
            INSERT INTO anonymous_usage_daily (
              bucket_start, model, status_code, ok, usage_missing,
              long_context_pricing_applied, split_priced, total_priced,
              calls, account_attempts, streaming_calls, non_cached_input_tokens,
              codex_status_tokens, duration_ms, input_tokens, cached_tokens,
              cache_write_tokens, output_tokens, reasoning_tokens,
              total_tokens, estimated_api_cost_usd,
              non_cached_input_cost_usd, cached_input_cost_usd,
              output_cost_usd
            )
            SELECT
              strftime('%Y-%m-%dT%H:%M:%fZ',
                       datetime(date(ts, 'localtime') || ' 12:00:00', 'utc')),
              COALESCE(model, '(unknown)'), COALESCE(status_code, 0),
              COALESCE(ok, 0), COALESCE(usage_missing, 0),
              COALESCE(long_context_pricing_applied, 0),
              CASE WHEN non_cached_input_cost_usd IS NOT NULL
                         AND cached_input_cost_usd IS NOT NULL
                         AND output_cost_usd IS NOT NULL THEN 1 ELSE 0 END,
              CASE WHEN estimated_api_cost_usd IS NOT NULL THEN 1 ELSE 0 END,
              COALESCE(SUM(call_count), 0),
              COALESCE(SUM(CASE WHEN COALESCE(account_attempt, 1)=1
                                THEN call_count ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN stream=1 THEN call_count ELSE 0 END), 0),
              COALESCE(SUM(MAX(COALESCE(input_tokens, 0)
                                   - COALESCE(cached_tokens, 0), 0)), 0),
              COALESCE(SUM(MAX(COALESCE(input_tokens, 0)
                                   - COALESCE(cached_tokens, 0), 0)
                               + COALESCE(output_tokens, 0)), 0),
              COALESCE(SUM(COALESCE(duration_ms, 0) * COALESCE(call_count, 1)), 0),
              CASE WHEN COUNT(input_tokens)>0 THEN SUM(input_tokens) END,
              CASE WHEN COUNT(cached_tokens)>0 THEN SUM(cached_tokens) END,
              CASE WHEN COUNT(cache_write_tokens)>0 THEN SUM(cache_write_tokens) END,
              CASE WHEN COUNT(output_tokens)>0 THEN SUM(output_tokens) END,
              CASE WHEN COUNT(reasoning_tokens)>0 THEN SUM(reasoning_tokens) END,
              CASE WHEN COUNT(total_tokens)>0 THEN SUM(total_tokens) END,
              CASE WHEN COUNT(estimated_api_cost_usd)>0 THEN SUM(estimated_api_cost_usd) END,
              CASE WHEN COUNT(non_cached_input_cost_usd)>0 THEN SUM(non_cached_input_cost_usd) END,
              CASE WHEN COUNT(cached_input_cost_usd)>0 THEN SUM(cached_input_cost_usd) END,
              CASE WHEN COUNT(output_cost_usd)>0 THEN SUM(output_cost_usd) END
              FROM usage_events
             WHERE identity_key=?
             GROUP BY date(ts, 'localtime'), COALESCE(model, '(unknown)'),
                      COALESCE(status_code, 0), COALESCE(ok, 0),
                      COALESCE(usage_missing, 0),
                      COALESCE(long_context_pricing_applied, 0),
                      CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                AND cached_input_cost_usd IS NOT NULL
                                AND output_cost_usd IS NOT NULL THEN 1 ELSE 0 END,
                      CASE WHEN estimated_api_cost_usd IS NOT NULL THEN 1 ELSE 0 END
            ON CONFLICT (
              bucket_start, model, status_code, ok, usage_missing,
              long_context_pricing_applied, split_priced, total_priced
            ) DO UPDATE SET
              calls=anonymous_usage_daily.calls + excluded.calls,
              account_attempts=anonymous_usage_daily.account_attempts
                               + excluded.account_attempts,
              streaming_calls=anonymous_usage_daily.streaming_calls
                              + excluded.streaming_calls,
              non_cached_input_tokens=
                anonymous_usage_daily.non_cached_input_tokens
                + excluded.non_cached_input_tokens,
              codex_status_tokens=anonymous_usage_daily.codex_status_tokens
                                  + excluded.codex_status_tokens,
              duration_ms=anonymous_usage_daily.duration_ms + excluded.duration_ms,
              {update_columns}
            """,
            (identity_key_value,),
        )
        conn.execute(
            """UPDATE local_import_records
                  SET usage_event_id=NULL
                WHERE usage_event_id IN (
                      SELECT id FROM usage_events WHERE identity_key=?
                )""",
            (identity_key_value,),
        )
        deleted: dict[str, int] = {"usage_events": event_count}
        for table in (
            "usage_events",
            "quota_events",
            "account_quota_cycles",
            "subscription_quota_snapshots",
        ):
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE identity_key=?",
                (identity_key_value,),
            )
            deleted[table] = max(int(cursor.rowcount), 0)
        return deleted

    def _merge_unsafe_anonymous_models_conn(self, conn: sqlite3.Connection) -> int:
        """Fold unsafe historical model labels into the anonymous sentinel.

        Updating the model column in place can violate the composite primary
        key when an ``(unknown)`` bucket already exists.  Upsert every unsafe
        row instead, preserving nullable-token and cost semantics exactly.
        """

        unsafe_models = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT model FROM anonymous_usage_daily"
            ).fetchall()
            if row[0] != "(unknown)" and safe_model_identifier(row[0]) is None
        ]
        nullable_columns = (
            "input_tokens",
            "cached_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
            "total_tokens",
            "estimated_api_cost_usd",
            "non_cached_input_cost_usd",
            "cached_input_cost_usd",
            "output_cost_usd",
        )
        update_columns = ",\n".join(
            f"{column}={self._add_nullable(f'anonymous_usage_daily.{column}', f'excluded.{column}')}"
            for column in nullable_columns
        )
        for unsafe_model in unsafe_models:
            conn.execute(
                f"""
                INSERT INTO anonymous_usage_daily (
                  bucket_start, model, status_code, ok, usage_missing,
                  long_context_pricing_applied, split_priced, total_priced,
                  calls, account_attempts, streaming_calls, non_cached_input_tokens,
                  codex_status_tokens, duration_ms, input_tokens, cached_tokens,
                  cache_write_tokens, output_tokens, reasoning_tokens,
                  total_tokens, estimated_api_cost_usd,
                  non_cached_input_cost_usd, cached_input_cost_usd,
                  output_cost_usd
                )
                SELECT bucket_start, '(unknown)', status_code, ok, usage_missing,
                       long_context_pricing_applied, split_priced, total_priced,
                       calls, account_attempts, streaming_calls, non_cached_input_tokens,
                       codex_status_tokens, duration_ms, input_tokens,
                       cached_tokens, cache_write_tokens, output_tokens,
                       reasoning_tokens, total_tokens, estimated_api_cost_usd,
                       non_cached_input_cost_usd, cached_input_cost_usd,
                       output_cost_usd
                  FROM anonymous_usage_daily
                 WHERE model=?
                ON CONFLICT (
                  bucket_start, model, status_code, ok, usage_missing,
                  long_context_pricing_applied, split_priced, total_priced
                ) DO UPDATE SET
                  calls=anonymous_usage_daily.calls + excluded.calls,
                  account_attempts=anonymous_usage_daily.account_attempts
                                   + excluded.account_attempts,
                  streaming_calls=anonymous_usage_daily.streaming_calls
                                  + excluded.streaming_calls,
                  non_cached_input_tokens=
                    anonymous_usage_daily.non_cached_input_tokens
                    + excluded.non_cached_input_tokens,
                  codex_status_tokens=anonymous_usage_daily.codex_status_tokens
                                      + excluded.codex_status_tokens,
                  duration_ms=anonymous_usage_daily.duration_ms
                              + excluded.duration_ms,
                  {update_columns}
                """,
                (unsafe_model,),
            )
            conn.execute(
                "DELETE FROM anonymous_usage_daily WHERE model=?", (unsafe_model,)
            )
        return len(unsafe_models)

    def _checkpoint_privacy_wal(self, reason: str) -> bool:
        """Truncate scrubbed WAL pages, remembering a blocked retry."""

        try:
            with self.connect() as conn:
                result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            busy = bool(result and int(result[0]))
            self._privacy_checkpoint_pending = busy
            if busy:
                LOG.warning("%s WAL checkpoint is busy; retry pending", reason)
            return not busy
        except sqlite3.Error:
            self._privacy_checkpoint_pending = True
            LOG.warning("%s WAL checkpoint failed; retry pending", reason)
            return False

    def register_codex_app_subscription(self, identity_key_value: str) -> bool:
        """Admit a newly observed local member before writing its first usage.

        This positive observation cannot retire other sources or reactivate a
        tombstone. Existing deletion state still requires a complete inventory
        reconciliation, whose union includes the registered local homes.
        """

        key = canonical_subscription_key(identity_key_value)
        if not key:
            return False
        with self.connect() as conn:
            if self._subscription_detail_allowed_conn(conn, key):
                return True
            now = utc_now()
            conn.execute(
                """INSERT OR IGNORE INTO active_subscription_registry (
                     identity_key, state, first_seen_at, last_seen_at,
                     missing_since, consecutive_misses, last_scan_generation,
                     high_risk_missing
                   ) SELECT ?, 'active', ?, ?, NULL, 0,
                            COALESCE((SELECT generation
                                       FROM subscription_inventory_state
                                      WHERE id=1), 0), 0
                      WHERE NOT EXISTS (
                        SELECT 1 FROM retired_subscription_tombstones
                         WHERE identity_key=?
                      )""",
                (key, now, now, key),
            )
            return self._subscription_detail_allowed_conn(conn, key)

    def reconcile_subscription_inventory(
        self,
        active_identity_keys: Iterable[str],
        observed_at: str | None = None,
        authoritative: bool = True,
    ) -> dict[str, Any]:
        """Track a complete management inventory and scrub confirmed removals.

        One incomplete or transiently empty response can never destroy data.
        A missing subscription is hidden immediately but is anonymized only
        after multiple complete inventories and a time grace period.
        """

        if self._privacy_checkpoint_pending:
            self._checkpoint_privacy_wal("subscription inventory retry")
        observed = normalize_timestamp(observed_at) if observed_at else utc_now()
        active = {
            key
            for value in active_identity_keys
            if (key := canonical_subscription_key(value))
        }
        result: dict[str, Any] = {
            "authoritative": bool(authoritative),
            "initialized": False,
            "active": len(active),
            "suspect": 0,
            "retired": 0,
            "retired_keys": [],
        }
        if not authoritative:
            return result
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = conn.execute(
                "SELECT * FROM subscription_inventory_state WHERE id=1"
            ).fetchone()
            initialized = bool(state and state["initialized"])
            generation = int(state["generation"] if state else 0) + 1
            for key in sorted(active):
                # A later complete inventory is the only event allowed to
                # reactivate a previously retired subscription.  Remove its
                # tombstone in the same write transaction as the active row.
                conn.execute(
                    "DELETE FROM retired_subscription_tombstones WHERE identity_key=?",
                    (key,),
                )
                conn.execute(
                    """
                    INSERT INTO active_subscription_registry (
                      identity_key, state, first_seen_at, last_seen_at,
                      missing_since, consecutive_misses, last_scan_generation,
                      high_risk_missing
                    ) VALUES (?, 'active', ?, ?, NULL, 0, ?, 0)
                    ON CONFLICT(identity_key) DO UPDATE SET
                      state='active', last_seen_at=excluded.last_seen_at,
                      missing_since=NULL, consecutive_misses=0,
                      last_scan_generation=excluded.last_scan_generation,
                      high_risk_missing=0
                    """,
                    (key, observed, observed, generation),
                )
            if not initialized:
                historical_keys = {
                    str(row[0])
                    for row in conn.execute(
                        """
                        SELECT identity_key FROM usage_events
                         WHERE identity_key LIKE 'subscription:%'
                           AND COALESCE(source, 'sidecar') IN (
                                 'sidecar', 'usage_queue', 'cockpit_tools',
                                 'sub2api'
                               )
                        UNION
                        SELECT identity_key FROM subscription_quota_snapshots
                         WHERE identity_key LIKE 'subscription:%'
                           AND source IN (
                                 'cliproxy_wham_usage', 'cockpit_tools_quota'
                               )
                        """
                    ).fetchall()
                    if canonical_subscription_key(row[0])
                }
                historical_missing = historical_keys - active
                initial_baseline_count = len(historical_keys | active)
                initial_large_drop = (
                    initial_baseline_count >= 4
                    and len(historical_missing) * 2 >= initial_baseline_count
                )
                initial_high_risk = bool(historical_missing) and (
                    not active or initial_large_drop
                )
                for key in sorted(historical_missing):
                    conn.execute(
                        """INSERT OR IGNORE INTO active_subscription_registry (
                             identity_key, state, first_seen_at, last_seen_at,
                             missing_since, consecutive_misses,
                             last_scan_generation, high_risk_missing
                           ) VALUES (?, 'suspect_missing', ?, ?, ?, 1, ?, ?)""",
                        (
                            key,
                            observed,
                            observed,
                            observed,
                            generation,
                            int(initial_high_risk),
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO subscription_inventory_state (
                      id, initialized, generation, last_complete_at,
                      last_active_count
                    ) VALUES (1, 1, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET initialized=1,
                      generation=excluded.generation,
                      last_complete_at=excluded.last_complete_at,
                      last_active_count=excluded.last_active_count
                    """,
                    (
                        generation,
                        observed,
                        initial_baseline_count
                        if initial_high_risk
                        else len(active),
                    ),
                )
                result["initialized"] = True
                result["suspect"] = len(historical_missing)
                return result

            missing_rows = [
                row
                for row in conn.execute(
                    "SELECT * FROM active_subscription_registry"
                ).fetchall()
                if row["identity_key"] not in active
            ]
            empty_inventory = not active and bool(missing_rows)
            previous_active_count = int(state["last_active_count"] or 0) if state else 0
            large_inventory_drop = (
                previous_active_count >= 4
                and (
                    len(active) * 2 <= previous_active_count
                    or len(missing_rows) * 2 >= previous_active_count
                )
            )
            high_risk_inventory = empty_inventory or large_inventory_drop
            observed_dt = datetime.fromisoformat(observed.replace("Z", "+00:00"))
            retired: list[str] = []
            for row in missing_rows:
                missing_since = row["missing_since"] or observed
                misses = int(row["consecutive_misses"] or 0) + 1
                # Once a key becomes suspect during an empty or sharply
                # reduced inventory, retain the longer confirmation policy
                # for that key.  A partial inventory recovery must not
                # silently downgrade the remaining suspect rows; only an
                # authoritative reappearance clears this flag above.
                persistent_high_risk = bool(row["high_risk_missing"]) or high_risk_inventory
                required_misses = (
                    EMPTY_INVENTORY_RETIRE_MISSES
                    if persistent_high_risk
                    else SUBSCRIPTION_RETIRE_MISSES
                )
                grace_seconds = (
                    EMPTY_INVENTORY_RETIRE_GRACE_SECONDS
                    if persistent_high_risk
                    else SUBSCRIPTION_RETIRE_GRACE_SECONDS
                )
                conn.execute(
                    """UPDATE active_subscription_registry
                          SET state='suspect_missing', missing_since=?,
                              consecutive_misses=?, last_scan_generation=?,
                              high_risk_missing=?
                        WHERE identity_key=?""",
                    (
                        missing_since,
                        misses,
                        generation,
                        int(persistent_high_risk),
                        row["identity_key"],
                    ),
                )
                missing_dt = datetime.fromisoformat(
                    str(missing_since).replace("Z", "+00:00")
                )
                if misses >= required_misses and (
                    observed_dt - missing_dt
                ).total_seconds() >= grace_seconds:
                    self._anonymize_subscription_conn(conn, row["identity_key"])
                    conn.execute(
                        """INSERT INTO retired_subscription_tombstones (
                             identity_key, retired_at, last_scan_generation
                           ) VALUES (?, ?, ?)
                           ON CONFLICT(identity_key) DO UPDATE SET
                             retired_at=excluded.retired_at,
                             last_scan_generation=excluded.last_scan_generation""",
                        (row["identity_key"], observed, generation),
                    )
                    conn.execute(
                        "DELETE FROM active_subscription_registry WHERE identity_key=?",
                        (row["identity_key"],),
                    )
                    retired.append(str(row["identity_key"]))
            conn.execute(
                """
                INSERT INTO subscription_inventory_state (
                  id, initialized, generation, last_complete_at,
                  last_active_count
                ) VALUES (1, 1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  generation=excluded.generation,
                  last_complete_at=excluded.last_complete_at,
                  last_active_count=excluded.last_active_count
                """,
                (
                    generation,
                    observed,
                    previous_active_count
                    if large_inventory_drop and missing_rows
                    else len(active),
                ),
            )
            result["suspect"] = int(
                conn.execute(
                    """SELECT COUNT(*) FROM active_subscription_registry
                         WHERE state='suspect_missing'"""
                ).fetchone()[0]
            )
            result["retired"] = len(retired)
            result["retired_keys"] = retired
            # The state row is initialized after the first complete scan and
            # remains initialized on every later authoritative scan.  Keep
            # the in-memory health payload truthful as well; otherwise a
            # healthy second poll would misleadingly report ``initialized``
            # as false even though deletion safeguards are active.
            result["initialized"] = True
        if result["retired"] or self._privacy_checkpoint_pending:
            self._checkpoint_privacy_wal("retired subscription")
        return result

    def apply_privacy_minimization(self, resolver: AccountResolver) -> int:
        """Migrate proven identities, then remove unnecessary identity detail."""

        migrations = resolver.identity_migrations() if resolver.enabled else {}
        ambiguous_accounts = (
            resolver.ambiguous_legacy_account_keys() if resolver.enabled else set()
        )
        ambiguous_legacy_identities = (
            resolver.ambiguous_legacy_identity_keys() if resolver.enabled else set()
        )
        changed = 0
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Scrub free-form labels before any row can be copied into an
            # anonymous bucket.  Existing unsafe anonymous labels are merged,
            # not updated in place, so collisions preserve every counter.
            unsafe_models = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT model FROM usage_events WHERE model IS NOT NULL"
                ).fetchall()
                if safe_model_identifier(row[0]) is None
            ]
            for unsafe_model in unsafe_models:
                conn.execute(
                    "UPDATE usage_events SET model=NULL WHERE model=?",
                    (unsafe_model,),
                )
            self._merge_unsafe_anonymous_models_conn(conn)
            for tombstone in conn.execute(
                "SELECT identity_key FROM retired_subscription_tombstones"
            ).fetchall():
                removed = self._anonymize_subscription_conn(conn, tombstone[0])
                changed += int(removed.get("usage_events") or 0)
            for old_key, new_key in migrations.items():
                if old_key == new_key:
                    continue
                if not canonical_subscription_key(new_key):
                    removed = self._anonymize_subscription_conn(conn, old_key)
                    changed += int(removed.get("usage_events") or 0)
                    continue
                if not self._prepare_proven_identity_migration_conn(
                    conn, old_key, new_key
                ):
                    removed = self._anonymize_subscription_conn(conn, old_key)
                    changed += int(removed.get("usage_events") or 0)
                    continue
                changed += max(
                    int(
                        conn.execute(
                            "UPDATE usage_events SET identity_key=? WHERE identity_key=?",
                            (new_key, old_key),
                        ).rowcount
                    ),
                    0,
                )
                for table in ("quota_events", "account_quota_cycles"):
                    conn.execute(
                        f"UPDATE {table} SET identity_key=? WHERE identity_key=?",
                        (new_key, old_key),
                    )
                conn.execute(
                    """UPDATE OR IGNORE subscription_quota_snapshots
                          SET identity_key=? WHERE identity_key=?""",
                    (new_key, old_key),
                )
                conn.execute(
                    "DELETE FROM subscription_quota_snapshots WHERE identity_key=?",
                    (old_key,),
                )
            # Any remaining non-subscription identity is legacy data whose
            # lineage cannot be proved without guessing.  Preserve its token
            # totals in the anonymous daily aggregate and remove account-,
            # alias-, installation-, session- and token-derived linkages.
            persisted_identity_keys = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT identity_key FROM usage_events
                     WHERE identity_key IS NOT NULL
                    UNION
                    SELECT identity_key FROM quota_events
                     WHERE identity_key IS NOT NULL
                    UNION
                    SELECT identity_key FROM account_quota_cycles
                     WHERE identity_key IS NOT NULL
                    UNION
                    SELECT identity_key FROM subscription_quota_snapshots
                     WHERE identity_key IS NOT NULL
                    """
                ).fetchall()
                if row[0]
            }
            legacy_keys = {
                key
                for key in persisted_identity_keys
                if canonical_subscription_key(key) is None
            }
            for obsolete_key in sorted(
                ambiguous_accounts | ambiguous_legacy_identities | legacy_keys,
                key=repr,
            ):
                self._anonymize_subscription_conn(conn, obsolete_key)
            # Registry and tombstone rows are intentionally opaque but must use
            # the exact current keyed format.  Old 16-hex keys have already had
            # their one chance to migrate through resolver-proven lineage.
            for table in (
                "active_subscription_registry",
                "retired_subscription_tombstones",
            ):
                invalid_keys = [
                    row[0]
                    for row in conn.execute(
                        f"SELECT identity_key FROM {table}"
                    ).fetchall()
                    if canonical_subscription_key(row[0]) is None
                ]
                conn.executemany(
                    f"DELETE FROM {table} WHERE identity_key=?",
                    ((key,) for key in invalid_keys),
                )
            historical_plans = conn.execute(
                "SELECT DISTINCT plan_type FROM subscription_quota_snapshots "
                "WHERE plan_type IS NOT NULL"
            ).fetchall()
            for row in historical_plans:
                normalized_plan = safe_plan_type(row[0])
                if normalized_plan == row[0]:
                    continue
                conn.execute(
                    "UPDATE subscription_quota_snapshots SET plan_type=? "
                    "WHERE plan_type=?",
                    (normalized_plan, row[0]),
                )
            conn.execute(
                """
                UPDATE usage_events SET
                  endpoint=NULL, method=NULL, session_id=NULL, thread_id=NULL,
                  turn_id=NULL, installation_id=NULL, window_id=NULL,
                  usage_alias=NULL, usage_project=NULL, auth_fingerprint=NULL,
                  account_id_hash=NULL, account_id_tail=NULL,
                  error_type=NULL, error_message_redacted=NULL, request_bytes=0,
                  response_bytes=0, request_id=NULL
                WHERE endpoint IS NOT NULL OR method IS NOT NULL
                   OR session_id IS NOT NULL OR thread_id IS NOT NULL
                   OR turn_id IS NOT NULL OR installation_id IS NOT NULL
                   OR window_id IS NOT NULL OR usage_alias IS NOT NULL
                   OR usage_project IS NOT NULL OR auth_fingerprint IS NOT NULL
                   OR account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                   OR error_type IS NOT NULL OR error_message_redacted IS NOT NULL
                   OR COALESCE(request_bytes, 0)!=0
                   OR COALESCE(response_bytes, 0)!=0
                   OR request_id IS NOT NULL
                """
            )
            conn.execute(
                """UPDATE quota_events SET account_id_hash=NULL,
                     account_id_tail=NULL, usage_alias=NULL,
                     raw_message_redacted=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL OR raw_message_redacted IS NOT NULL"""
            )
            conn.execute(
                """UPDATE account_quota_cycles SET account_id_hash=NULL,
                     account_id_tail=NULL, usage_alias=NULL, notes=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL OR notes IS NOT NULL"""
            )
            conn.execute(
                """UPDATE subscription_quota_snapshots SET
                     account_id_hash=NULL, account_id_tail=NULL,
                     usage_alias=NULL
                   WHERE account_id_hash IS NOT NULL OR account_id_tail IS NOT NULL
                      OR usage_alias IS NOT NULL"""
            )
            import_files = conn.execute("SELECT * FROM local_import_files").fetchall()
            if import_files:
                conn.execute("DELETE FROM local_import_files")
                for row in import_files:
                    stored_path = str(row["path"] or "")
                    path_key = (
                        stored_path
                        if re.fullmatch(r"file:[0-9a-f]{16}", stored_path)
                        else "file:" + (short_hash(stored_path) or "unknown")
                    )
                    conn.execute(
                        """INSERT OR REPLACE INTO local_import_files
                           (path, size, mtime_ns, offset, session_id,
                            model_provider, model, turn_id, updated_at)
                           VALUES (?, ?, ?, ?, NULL, ?, ?, NULL, ?)""",
                        (
                            path_key,
                            row["size"],
                            row["mtime_ns"],
                            row["offset"],
                            (
                                "openai"
                                if (safe_text(row["model_provider"], 64) or "").lower()
                                == "openai"
                                else None
                            ),
                            safe_model_identifier(row["model"]),
                            row["updated_at"],
                        ),
                    )
        self._checkpoint_privacy_wal("privacy minimization")
        return changed

    def set_price(
        self,
        pattern: str,
        input_rate: float,
        output_rate: float,
        cached_rate: float,
        source_note: str | None,
    ) -> None:
        if min(input_rate, output_rate, cached_rate) < 0:
            raise ValueError("prices must be non-negative")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO model_prices (
                  model_pattern, input_per_million, output_per_million,
                  cached_input_per_million, cache_write_per_million,
                  long_context_threshold_tokens,
                  long_input_per_million, long_cached_input_per_million,
                  long_cache_write_per_million, long_output_per_million,
                  reasoning_per_million, currency,
                  source_note, source_kind, updated_at
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'USD', ?, 'manual', ?)
                ON CONFLICT(model_pattern) DO UPDATE SET
                  input_per_million=excluded.input_per_million,
                  output_per_million=excluded.output_per_million,
                  cached_input_per_million=excluded.cached_input_per_million,
                  cache_write_per_million=NULL,
                  long_context_threshold_tokens=NULL,
                  long_input_per_million=NULL,
                  long_cached_input_per_million=NULL,
                  long_cache_write_per_million=NULL,
                  long_output_per_million=NULL,
                  currency='USD', source_note=excluded.source_note,
                  source_kind='manual',
                  updated_at=excluded.updated_at
                """,
                (pattern, input_rate, output_rate, cached_rate, redact_text(source_note, 500), utc_now()),
            )

    def list_prices(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM model_prices ORDER BY model_pattern")]

    def price_sync_status(self) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM price_sync_metadata WHERE id=1").fetchone()
        return dict(row) if row else {
            "id": 1,
            "source_url": None,
            "fetched_at": None,
            "content_sha256": None,
            "parser_version": OFFICIAL_PRICE_PARSER_VERSION,
            "status": "never_run",
            "model_count": 0,
            "repriced_events": 0,
            "error_type": None,
            "error_message_redacted": None,
            "updated_at": None,
        }

    def record_price_sync(
        self,
        *,
        source_url: str,
        fetched_at: str,
        content_sha256: str | None,
        parser_version: str,
        status: str,
        model_count: int,
        repriced_events: int = 0,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO price_sync_metadata (
                  id, source_url, fetched_at, content_sha256, parser_version,
                  status, model_count, repriced_events, error_type,
                  error_message_redacted, updated_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  source_url=excluded.source_url,
                  fetched_at=excluded.fetched_at,
                  content_sha256=excluded.content_sha256,
                  parser_version=excluded.parser_version,
                  status=excluded.status,
                  model_count=excluded.model_count,
                  repriced_events=excluded.repriced_events,
                  error_type=excluded.error_type,
                  error_message_redacted=excluded.error_message_redacted,
                  updated_at=excluded.updated_at
                """,
                (
                    source_url,
                    fetched_at,
                    content_sha256,
                    parser_version,
                    status,
                    max(int(model_count), 0),
                    max(int(repriced_events), 0),
                    safe_text(error_type, 120),
                    redact_text(error_message, 600),
                    utc_now(),
                ),
            )

    def replace_official_prices(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        source_url: str,
        fetched_at: str,
        content_sha256: str,
        parser_version: str,
    ) -> int:
        """Atomically replace only rows previously owned by the official sync."""

        if not rows:
            raise ValueError("official pricing parser returned no models")
        patterns = [safe_text(row.get("model_pattern"), 200) for row in rows]
        if any(not pattern for pattern in patterns):
            raise ValueError("official pricing parser returned an invalid model name")
        note = (
            f"official OpenAI pricing; URL={source_url}; fetched_at={fetched_at}; "
            f"sha256={content_sha256}; parser={parser_version}; standard short/long-context rates"
        )
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            repriced_events = self._correct_astra_context_costs(conn)
            conn.execute("DELETE FROM model_prices WHERE source_kind='official'")
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO model_prices (
                      model_pattern, input_per_million, output_per_million,
                      cached_input_per_million, cache_write_per_million,
                      long_context_threshold_tokens,
                      long_input_per_million, long_cached_input_per_million,
                      long_cache_write_per_million, long_output_per_million,
                      reasoning_per_million, currency,
                      source_note, source_kind, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'USD', ?, 'official', ?)
                    ON CONFLICT(model_pattern) DO UPDATE SET
                      input_per_million=excluded.input_per_million,
                      output_per_million=excluded.output_per_million,
                      cached_input_per_million=excluded.cached_input_per_million,
                      cache_write_per_million=excluded.cache_write_per_million,
                      long_context_threshold_tokens=excluded.long_context_threshold_tokens,
                      long_input_per_million=excluded.long_input_per_million,
                      long_cached_input_per_million=excluded.long_cached_input_per_million,
                      long_cache_write_per_million=excluded.long_cache_write_per_million,
                      long_output_per_million=excluded.long_output_per_million,
                      reasoning_per_million=NULL,
                      currency='USD', source_note=excluded.source_note,
                      source_kind='official', updated_at=excluded.updated_at
                    """,
                    (
                        row["model_pattern"],
                        row.get("input_per_million"),
                        row.get("output_per_million"),
                        row.get("cached_input_per_million"),
                        row.get("cache_write_per_million"),
                        row.get("long_context_threshold_tokens"),
                        row.get("long_input_per_million"),
                        row.get("long_cached_input_per_million"),
                        row.get("long_cache_write_per_million"),
                        row.get("long_output_per_million"),
                        note,
                        fetched_at,
                    ),
                )
                unpriced = conn.execute(
                    """
                    SELECT id, input_tokens, cached_tokens, cache_write_tokens, output_tokens
                      FROM usage_events
                     WHERE estimated_api_cost_usd IS NULL
                       AND non_cached_input_cost_usd IS NULL
                       AND cached_input_cost_usd IS NULL
                       AND output_cost_usd IS NULL
                       AND model=?
                       AND input_tokens IS NOT NULL
                       AND output_tokens IS NOT NULL
                    """,
                    (row["model_pattern"],),
                ).fetchall()
                for event in unpriced:
                    usage = NormalizedUsage(
                        input_tokens=as_nonnegative_int(event["input_tokens"]),
                        output_tokens=as_nonnegative_int(event["output_tokens"]),
                        cached_tokens=as_nonnegative_int(event["cached_tokens"]),
                        cache_write_tokens=as_nonnegative_int(event["cache_write_tokens"]),
                    )
                    components = self._components_for_price(usage, row)
                    if components is None:
                        continue
                    cursor = conn.execute(
                        """
                        UPDATE usage_events
                           SET non_cached_input_cost_usd=?, cached_input_cost_usd=?,
                               output_cost_usd=?, estimated_api_cost_usd=?,
                               long_context_pricing_applied=?
                         WHERE id=? AND estimated_api_cost_usd IS NULL
                        """,
                        (
                            components.non_cached_input_cost_usd,
                            components.cached_input_cost_usd,
                            components.output_cost_usd,
                            components.total_cost_usd,
                            int(components.long_context_pricing_applied),
                            event["id"],
                        ),
                    )
                    repriced_events += max(int(cursor.rowcount), 0)
            repriced_events += self._correct_astra_context_costs(conn)
            repriced_events += self._upgrade_long_context_costs(conn)
            conn.execute(
                """
                INSERT INTO price_sync_metadata (
                  id, source_url, fetched_at, content_sha256, parser_version,
                  status, model_count, repriced_events, error_type,
                  error_message_redacted, updated_at
                ) VALUES (1, ?, ?, ?, ?, 'ok', ?, ?, NULL, NULL, ?)
                ON CONFLICT(id) DO UPDATE SET
                  source_url=excluded.source_url,
                  fetched_at=excluded.fetched_at,
                  content_sha256=excluded.content_sha256,
                  parser_version=excluded.parser_version,
                  status='ok', model_count=excluded.model_count,
                  repriced_events=excluded.repriced_events,
                  error_type=NULL, error_message_redacted=NULL, updated_at=excluded.updated_at
                """,
                (
                    source_url,
                    fetched_at,
                    content_sha256,
                    parser_version,
                    len(rows),
                    repriced_events,
                    utc_now(),
                ),
            )
        return repriced_events

    def maybe_auto_reset(self, info: RequestInfo, ts: str) -> bool:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not self._subscription_detail_allowed_conn(conn, info.identity_key):
                return False
            last_quota = conn.execute(
                f"SELECT ts FROM quota_events WHERE identity_key=? AND event_type IN ({','.join('?' for _ in QUOTA_EVENT_TYPES)}) ORDER BY ts DESC, id DESC LIMIT 1",
                (info.identity_key, *sorted(QUOTA_EVENT_TYPES)),
            ).fetchone()
            if not last_quota:
                return False
            # A successful queue item is not proof that the subscription was
            # reset: CLIProxyAPI can succeed on another route while the
            # account's Codex weekly/monthly window is still exhausted.  The
            # read-only WHAM snapshot is stronger evidence.  Do not split a
            # cycle while the latest subscription window still reports 100%.
            snapshots = conn.execute(
                """
                SELECT q.window_kind, q.used_percent, q.reset_at
                  FROM subscription_quota_snapshots q
                 WHERE q.identity_key=?
                   AND q.window_kind IN ('weekly', 'monthly')
                   AND NOT EXISTS (
                         SELECT 1
                           FROM subscription_quota_snapshots newer
                          WHERE newer.identity_key=q.identity_key
                            AND newer.window_kind=q.window_kind
                            AND (
                                  newer.fetched_at > q.fetched_at
                                  OR (newer.fetched_at=q.fetched_at
                                      AND newer.id > q.id)
                                )
                       )
                """,
                (info.identity_key,),
            ).fetchall()
            for snapshot in snapshots:
                if snapshot["used_percent"] is None:
                    continue
                try:
                    reset_at = normalize_optional_timestamp(snapshot["reset_at"])
                    if float(snapshot["used_percent"]) >= QUOTA_ESTIMATE_CAP_CONFIDENCE_PERCENT and (
                        not reset_at or ts < reset_at
                    ):
                        return False
                except (TypeError, ValueError):
                    continue
            last_reset = conn.execute(
                f"SELECT ts FROM quota_events WHERE identity_key=? AND event_type IN ({','.join('?' for _ in RESET_EVENT_TYPES)}) ORDER BY ts DESC, id DESC LIMIT 1",
                (info.identity_key, *sorted(RESET_EVENT_TYPES)),
            ).fetchone()
            if last_reset and last_reset["ts"] >= last_quota["ts"]:
                return False
            conn.execute(
                """INSERT INTO quota_events
                (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                 event_type, source, raw_message_redacted)
                VALUES (?, ?, ?, ?, ?, 'reset_detected', 'success_after_quota', NULL)""",
                (ts, info.identity_key, None, None, None),
            )
            return True

    def insert_event(self, event: UsageEvent) -> int:
        event_id, _identity_allowed = self._insert_event_with_status(event)
        return event_id

    @staticmethod
    def _validate_event_costs(event: UsageEvent) -> None:
        component_values = (
            event.non_cached_input_cost_usd,
            event.cached_input_cost_usd,
            event.output_cost_usd,
        )
        populated_components = sum(value is not None for value in component_values)
        if populated_components not in {0, 3}:
            raise ValueError("event cost components must be all NULL or all populated")
        if populated_components == 3:
            if event.estimated_api_cost_usd is None:
                raise ValueError("event total cost is required with cost components")
            total = sum(float(value) for value in component_values if value is not None)
            if not math.isclose(
                total,
                float(event.estimated_api_cost_usd),
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError("event cost components do not equal total cost")

    def _insert_event_with_status(self, event: UsageEvent) -> tuple[int, bool]:
        self._validate_event_costs(event)
        values = self._minimized_event_values(event)
        columns = list(values)
        placeholders = ",".join("?" for _ in columns)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            canonical_key = canonical_subscription_key(event.identity_key)
            identity_allowed = bool(
                canonical_key
                and self._subscription_detail_allowed_conn(conn, canonical_key)
            )
            if not identity_allowed:
                values["identity_key"] = "unknown"
            cursor = conn.execute(
                f"INSERT INTO usage_events ({','.join(columns)}) VALUES ({placeholders})",
                tuple(values[column] for column in columns),
            )
            event_id = int(cursor.lastrowid)
            self._upsert_api_response_observation_conn(
                conn,
                f"usage:{event_id}",
                event.ts,
                event.status_code,
                event.call_count,
                values["source"],
                values["account_attempt"],
            )
            return event_id, identity_allowed

    @staticmethod
    def _minimized_event_values(event: UsageEvent) -> dict[str, Any]:
        """Return the durable token/statistics subset for every event source."""

        values = asdict(event)
        values["identity_key"] = (
            canonical_subscription_key(event.identity_key) or "unknown"
        )
        # The resolver already reduced an active account to an opaque HMAC
        # key. Request metadata and token/account fingerprints have no durable
        # statistical use and can make a deleted account re-identifiable.
        for field in (
            "endpoint",
            "method",
            "session_id",
            "thread_id",
            "turn_id",
            "installation_id",
            "window_id",
            "usage_alias",
            "usage_project",
            "auth_fingerprint",
            "account_id_hash",
            "account_id_tail",
            "error_message_redacted",
            "error_type",
            "request_id",
        ):
            values[field] = None
        values["model"] = safe_model_identifier(event.model)
        values["source"] = safe_alias(event.source) or "unknown"
        values["account_attempt"] = int(
            (as_nonnegative_int(event.account_attempt) or 0) > 0
        )
        values["request_bytes"] = 0
        values["response_bytes"] = 0
        return values

    @staticmethod
    def _api_response_observation_target(
        observation_key: str,
        timestamp: Any,
        status_code: Any,
        call_count: Any,
        source: Any,
        account_attempt: Any = 1,
    ) -> tuple[str, dict[str, Any] | None]:
        safe_source = safe_alias(source)
        safe_key = safe_text(observation_key, 512)
        normalized = normalize_optional_timestamp(timestamp)
        if (
            safe_source not in RESPONSE_TIMELINE_SOURCES
            or not safe_key
            or not normalized
        ):
            return "skip", None
        if as_nonnegative_int(account_attempt) == 0:
            return "delete", {"observation_key": safe_key}
        parsed_status = as_nonnegative_int(status_code)
        if parsed_status is not None and not 100 <= parsed_status <= 599:
            parsed_status = None
        calls = max(as_nonnegative_int(call_count) or 1, 1)
        return "upsert", {
            "observation_key": safe_key,
            "minute_ts": f"{normalized[:16]}:00Z",
            "status_code": parsed_status,
            "call_count": calls,
            "source": safe_source,
        }

    @staticmethod
    def _write_api_response_observation_conn(
        conn: sqlite3.Connection,
        action: str,
        values: Mapping[str, Any] | None,
    ) -> int:
        if action == "skip" or values is None:
            return 0
        if action == "delete":
            cursor = conn.execute(
                "DELETE FROM api_response_observations WHERE observation_key=?",
                (values["observation_key"],),
            )
            return max(int(cursor.rowcount), 0)
        if action != "upsert":
            raise ValueError("invalid observation action")
        cursor = conn.execute(
            """
            INSERT INTO api_response_observations (
              observation_key, minute_ts, status_code, call_count, source
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(observation_key) DO UPDATE SET
              minute_ts=excluded.minute_ts,
              status_code=excluded.status_code,
              call_count=excluded.call_count,
              source=excluded.source
            WHERE api_response_observations.minute_ts IS NOT excluded.minute_ts
               OR api_response_observations.status_code IS NOT excluded.status_code
               OR api_response_observations.call_count IS NOT excluded.call_count
               OR api_response_observations.source IS NOT excluded.source
            """,
            (
                values["observation_key"],
                values["minute_ts"],
                values["status_code"],
                values["call_count"],
                values["source"],
            ),
        )
        return max(int(cursor.rowcount), 0)

    @classmethod
    def _upsert_api_response_observation_conn(
        cls,
        conn: sqlite3.Connection,
        observation_key: str,
        timestamp: Any,
        status_code: Any,
        call_count: Any,
        source: Any,
        account_attempt: Any = 1,
    ) -> int:
        """Persist eligible HTTP response metadata at minute precision.

        Account-selection failures are retained in ``usage_events`` for
        request/failure history, but they are not upstream API responses and
        therefore must not affect the HTTP health timeline.
        """

        action, values = cls._api_response_observation_target(
            observation_key,
            timestamp,
            status_code,
            call_count,
            source,
            account_attempt,
        )
        return cls._write_api_response_observation_conn(conn, action, values)

    @staticmethod
    def _subscription_detail_allowed_conn(
        conn: sqlite3.Connection,
        identity_key_value: str | None,
    ) -> bool:
        """Reject identifiable writes while an inventory deletion is pending."""

        key = canonical_subscription_key(identity_key_value)
        if not key:
            return False
        if conn.execute(
            "SELECT 1 FROM retired_subscription_tombstones WHERE identity_key=?",
            (key,),
        ).fetchone():
            return False
        row = conn.execute(
            "SELECT state FROM active_subscription_registry WHERE identity_key=?",
            (key,),
        ).fetchone()
        if row is not None:
            return row["state"] == "active"
        inventory = conn.execute(
            "SELECT initialized FROM subscription_inventory_state WHERE id=1"
        ).fetchone()
        # Before the first complete management inventory, keep collection
        # available for installations that have not enabled quota polling.
        # Afterwards, the authoritative active registry is the allow-list:
        # an unknown late key cannot create a permanent phantom subscription.
        return not bool(inventory and inventory["initialized"])

    def record_event(self, event: UsageEvent, info: RequestInfo, source: str | None = None) -> int:
        """Persist one event and derive quota transitions in one best-effort path."""

        if source:
            event.source = safe_alias(source) or "sidecar"
        event_id, identity_allowed = self._insert_event_with_status(event)
        event_key = canonical_subscription_key(event.identity_key)
        info_key = canonical_subscription_key(info.identity_key)
        if not identity_allowed or not event_key or event_key != info_key:
            # A removed auth file disappears from the dashboard immediately.
            # Late queue/proxy records retain their token totals but cannot
            # recreate the retired subscription linkage during the grace
            # period before the historical rows are aggregated.
            return event_id
        if event.ok:
            self.maybe_auto_reset(info, event.ts)
        # HTTP handlers can finish out of order.  A quota error may close a
        # cycle before an earlier-timestamped successful response has finished
        # its SQLite insert.  Reconcile any already-complete cycle after every
        # insert so the cycle cost is not permanently understated by that race.
        self._refresh_complete_cycles_for_event(event)
        quota_type = detect_quota_event(event.status_code, event.error_type, event.error_message_redacted)
        if quota_type:
            self.record_quota_hit(
                info,
                event.ts,
                quota_type,
                event.source,
                event.error_message_redacted,
                usage_event_id=event_id,
            )
            # A quota-hit handler can finish before an older successful
            # response is inserted. Refresh once more after the hit creates a
            # cycle so that out-of-order HTTP completions cannot undercount it.
            self._refresh_complete_cycles_for_event(event)
        return event_id

    def _refresh_complete_cycles_for_event(self, event: UsageEvent) -> None:
        with self.connect() as conn:
            cycles = conn.execute(
                """SELECT id, identity_key, cycle_start_ts, cycle_end_ts
                     FROM account_quota_cycles
                    WHERE identity_key=? AND is_complete_cycle=1""",
                (event.identity_key,),
            ).fetchall()
            for cycle in cycles:
                # A later reset starts a new cycle; otherwise a response that
                # completed out of order is still part of this already-closed
                # provider window and may extend its observed end timestamp.
                later_reset = conn.execute(
                    f"""SELECT 1 FROM quota_events
                         WHERE identity_key=? AND event_type IN ({','.join('?' for _ in RESET_EVENT_TYPES)})
                           AND ts>? LIMIT 1""",
                    (event.identity_key, *sorted(RESET_EVENT_TYPES), cycle["cycle_end_ts"]),
                ).fetchone()
                if later_reset:
                    continue
                end_ts = max(str(cycle["cycle_end_ts"]), str(event.ts))
                reset = conn.execute(
                    f"""SELECT ts FROM quota_events
                         WHERE identity_key=? AND event_type IN ({','.join('?' for _ in RESET_EVENT_TYPES)})
                           AND ts<=?
                         ORDER BY ts DESC, id DESC LIMIT 1""",
                    (event.identity_key, *sorted(RESET_EVENT_TYPES), end_ts),
                ).fetchone()
                boundary = reset["ts"] if reset else None
                if boundary is None:
                    first = conn.execute(
                        "SELECT MIN(ts) AS ts FROM usage_events WHERE identity_key=? AND ts<=?",
                        (event.identity_key, end_ts),
                    ).fetchone()
                else:
                    first = conn.execute(
                        """SELECT MIN(ts) AS ts FROM usage_events
                             WHERE identity_key=? AND ts>=? AND ts<=?""",
                        (event.identity_key, boundary, end_ts),
                    ).fetchone()
                start = first["ts"] if first and first["ts"] else cycle["cycle_start_ts"]
                totals = conn.execute(
                    """SELECT
                         COALESCE(SUM(call_count),0) total_calls,
                         COALESCE(SUM(CASE WHEN ok=1 THEN call_count ELSE 0 END),0) successful_calls,
                         COALESCE(SUM(CASE WHEN ok=0 THEN call_count ELSE 0 END),0) failed_calls,
                         COALESCE(SUM(CASE WHEN stream=1 THEN call_count ELSE 0 END),0) streaming_calls,
                         COALESCE(SUM(input_tokens),0) total_input_tokens,
                         COALESCE(SUM(cached_tokens),0) total_cached_tokens,
                         COALESCE(SUM(output_tokens),0) total_output_tokens,
                         COALESCE(SUM(reasoning_tokens),0) total_reasoning_tokens,
                         COALESCE(SUM(total_tokens),0) total_tokens,
                         SUM(estimated_api_cost_usd) estimated_api_cost_usd
                       FROM usage_events
                      WHERE identity_key=? AND ts>=? AND ts<=?""",
                    (event.identity_key, start, end_ts),
                ).fetchone()
                cost = totals["estimated_api_cost_usd"]
                conn.execute(
                    """UPDATE account_quota_cycles SET
                         cycle_start_ts=?, cycle_end_ts=?, total_calls=?, successful_calls=?, failed_calls=?,
                         streaming_calls=?, total_input_tokens=?, total_cached_tokens=?,
                         total_output_tokens=?, total_reasoning_tokens=?, total_tokens=?,
                         estimated_api_cost_usd=?, observed_floor_usd=?,
                         api_equivalent_quota_usd=? WHERE id=?""",
                    (
                        start,
                        end_ts,
                        totals["total_calls"],
                        totals["successful_calls"],
                        totals["failed_calls"],
                        totals["streaming_calls"],
                        totals["total_input_tokens"],
                        totals["total_cached_tokens"],
                        totals["total_output_tokens"],
                        totals["total_reasoning_tokens"],
                        totals["total_tokens"],
                        cost,
                        cost,
                        cost,
                        cycle["id"],
                    ),
                )

    def record_imported_event(
        self,
        event: UsageEvent,
        import_key: str,
        source: str,
    ) -> bool:
        """Insert one idempotent local/manual import in a single transaction."""

        safe_key = safe_text(import_key, 300)
        safe_source = safe_alias(source)
        if not safe_key or not safe_source:
            raise ValueError("invalid import identity")
        event.source = safe_source
        self._validate_event_costs(event)
        values = self._minimized_event_values(event)
        columns = list(values)
        placeholders = ",".join("?" for _ in columns)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            imported = conn.execute(
                "SELECT source FROM local_import_records WHERE import_key=?",
                (safe_key,),
            ).fetchone()
            if imported is not None:
                if imported["source"] == safe_source:
                    self._upsert_api_response_observation_conn(
                        conn,
                        f"import:{safe_key}",
                        event.ts,
                        event.status_code,
                        event.call_count,
                        safe_source,
                        values["account_attempt"],
                    )
                return False
            self._upsert_api_response_observation_conn(
                conn,
                f"import:{safe_key}",
                event.ts,
                event.status_code,
                event.call_count,
                safe_source,
                values["account_attempt"],
            )
            canonical_key = canonical_subscription_key(event.identity_key)
            if canonical_key and not self._subscription_detail_allowed_conn(
                conn, canonical_key
            ):
                values["identity_key"] = "unknown"
            cursor = conn.execute(
                f"INSERT INTO usage_events ({','.join(columns)}) VALUES ({placeholders})",
                tuple(values[column] for column in columns),
            )
            conn.execute(
                """INSERT INTO local_import_records
                   (import_key, source, usage_event_id, imported_at)
                   VALUES (?, ?, ?, ?)""",
                (safe_key, safe_source, int(cursor.lastrowid), utc_now()),
            )
        return True

    @staticmethod
    def _empty_import_sync_stats() -> dict[str, int]:
        return {
            "scanned": 0,
            "new": 0,
            "changed": 0,
            "unchanged": 0,
            "retired": 0,
            "source_conflict": 0,
            "write_transactions": 0,
            "usage_updates": 0,
            "observation_updates": 0,
        }

    @staticmethod
    def _imported_event_states_conn(
        conn: sqlite3.Connection,
        batch: Sequence[_PreparedImportedEvent],
    ) -> dict[str, _ImportedEventState]:
        if not batch:
            return {}
        event_columns = tuple(batch[0].values)
        requested_values = ",".join("(?)" for _ in batch)
        event_selection = ",\n".join(
            f'events."{column}" AS "event_{column}"' for column in event_columns
        )
        rows = conn.execute(
            f"""
            WITH requested(import_key) AS (VALUES {requested_values})
            SELECT requested.import_key AS requested_key,
                   imports.import_key IS NOT NULL AS import_record_exists,
                   imports.source AS import_source,
                   imports.usage_event_id AS usage_event_id,
                   events.id AS usage_row_id,
                   {event_selection},
                   observations.observation_key AS observation_key,
                   observations.minute_ts AS observation_minute_ts,
                   observations.status_code AS observation_status_code,
                   observations.call_count AS observation_call_count,
                   observations.source AS observation_source
              FROM requested
              LEFT JOIN local_import_records imports
                ON imports.import_key=requested.import_key
              LEFT JOIN usage_events events
                ON events.id=imports.usage_event_id
              LEFT JOIN api_response_observations observations
                ON observations.observation_key='import:' || requested.import_key
            """,
            tuple(item.import_key for item in batch),
        ).fetchall()
        states: dict[str, _ImportedEventState] = {}
        for row in rows:
            usage = (
                {column: row[f"event_{column}"] for column in event_columns}
                if row["usage_row_id"] is not None
                else None
            )
            observation = (
                {
                    "observation_key": row["observation_key"],
                    "minute_ts": row["observation_minute_ts"],
                    "status_code": row["observation_status_code"],
                    "call_count": row["observation_call_count"],
                    "source": row["observation_source"],
                }
                if row["observation_key"] is not None
                else None
            )
            states[str(row["requested_key"])] = _ImportedEventState(
                import_record_exists=bool(row["import_record_exists"]),
                import_source=row["import_source"],
                usage_event_id=(
                    int(row["usage_event_id"])
                    if row["usage_event_id"] is not None
                    else None
                ),
                usage=usage,
                observation=observation,
            )
        return states

    @staticmethod
    def _subscription_detail_allowed_keys_conn(
        conn: sqlite3.Connection,
        identity_keys: Iterable[Any],
    ) -> set[str]:
        keys = sorted(
            {
                key
                for value in identity_keys
                if (key := canonical_subscription_key(value))
            }
        )
        if not keys:
            return set()
        placeholders = ",".join("?" for _ in keys)
        tombstones = {
            str(row["identity_key"])
            for row in conn.execute(
                f"""SELECT identity_key FROM retired_subscription_tombstones
                      WHERE identity_key IN ({placeholders})""",
                keys,
            ).fetchall()
        }
        registry = {
            str(row["identity_key"]): str(row["state"])
            for row in conn.execute(
                f"""SELECT identity_key, state FROM active_subscription_registry
                      WHERE identity_key IN ({placeholders})""",
                keys,
            ).fetchall()
        }
        inventory = conn.execute(
            "SELECT initialized FROM subscription_inventory_state WHERE id=1"
        ).fetchone()
        initialized = bool(inventory and inventory["initialized"])
        return {
            key
            for key in keys
            if key not in tombstones
            and (
                registry.get(key) == "active"
                or (key not in registry and not initialized)
            )
        }

    @staticmethod
    def _observation_target_changed(
        prepared: _PreparedImportedEvent,
        existing: Mapping[str, Any] | None,
    ) -> bool:
        if prepared.observation_action == "skip":
            return False
        if prepared.observation_action == "delete":
            return existing is not None
        target = prepared.observation_values
        if target is None or existing is None:
            return True
        return any(
            existing.get(column) != target.get(column)
            for column in ("minute_ts", "status_code", "call_count", "source")
        )

    @staticmethod
    def _classify_imported_event(
        prepared: _PreparedImportedEvent,
        state: _ImportedEventState,
        allowed_identity_keys: set[str],
        restore_retired_observation: bool = False,
    ) -> _ImportedEventDecision:
        values = dict(prepared.values)
        if state.import_record_exists:
            if state.import_source != prepared.source:
                return _ImportedEventDecision(
                    prepared, "source_conflict", values, (), False, None
                )
            existing = state.usage
            if state.usage_event_id is None or existing is None:
                # Privacy retirement is a durable dedupe tombstone.  Do not
                # recreate account detail.  Cockpit's versioned legacy replay
                # can explicitly restore its identity-free response ledger;
                # normal overlap imports, including Sub2API, cannot.
                observation_changed = bool(
                    restore_retired_observation
                    and UsageRepository._observation_target_changed(
                        prepared,
                        state.observation,
                    )
                )
                return _ImportedEventDecision(
                    prepared,
                    "retired",
                    values,
                    (),
                    observation_changed,
                    None,
                )
            identity_key_value = canonical_subscription_key(values["identity_key"])
            if identity_key_value not in allowed_identity_keys:
                # A suspect or retired key must not downgrade a still-live
                # historical row during an external repricing scan.
                values["identity_key"] = existing["identity_key"]
            changed_columns = tuple(
                column
                for column, value in values.items()
                if existing[column] != value
            )
            observation_changed = UsageRepository._observation_target_changed(
                prepared,
                state.observation,
            )
            status = "changed" if changed_columns or observation_changed else "unchanged"
            return _ImportedEventDecision(
                prepared,
                status,
                values,
                changed_columns,
                observation_changed,
                state.usage_event_id,
            )

        identity_key_value = canonical_subscription_key(values["identity_key"])
        if identity_key_value not in allowed_identity_keys:
            values["identity_key"] = "unknown"
        return _ImportedEventDecision(
            prepared,
            "new",
            values,
            tuple(values),
            UsageRepository._observation_target_changed(
                prepared,
                state.observation,
            ),
            None,
        )

    def _classify_imported_event_batch_conn(
        self,
        conn: sqlite3.Connection,
        batch: Sequence[_PreparedImportedEvent],
        restore_retired_observations: bool = False,
    ) -> list[_ImportedEventDecision]:
        states = self._imported_event_states_conn(conn, batch)
        allowed = self._subscription_detail_allowed_keys_conn(
            conn,
            (item.values.get("identity_key") for item in batch),
        )
        return [
            self._classify_imported_event(
                item,
                states[item.import_key],
                allowed,
                restore_retired_observations,
            )
            for item in batch
        ]

    def _write_imported_event_batch_conn(
        self,
        conn: sqlite3.Connection,
        classified: Sequence[_ImportedEventDecision],
    ) -> dict[str, int]:
        stats = self._empty_import_sync_stats()
        stats["scanned"] = len(classified)
        for decision in classified:
            prepared = decision.prepared
            stats[decision.status] += 1
            if decision.status == "new":
                values = decision.values
                columns = list(values)
                placeholders = ",".join("?" for _ in columns)
                cursor = conn.execute(
                    f"INSERT INTO usage_events ({','.join(columns)}) VALUES ({placeholders})",
                    tuple(values[column] for column in columns),
                )
                conn.execute(
                    """INSERT INTO local_import_records
                       (import_key, source, usage_event_id, imported_at)
                       VALUES (?, ?, ?, ?)""",
                    (
                        prepared.import_key,
                        prepared.source,
                        int(cursor.lastrowid),
                        utc_now(),
                    ),
                )
                stats["observation_updates"] += self._write_api_response_observation_conn(
                    conn,
                    prepared.observation_action,
                    prepared.observation_values,
                )
                continue
            if decision.status == "retired" and decision.observation_changed:
                stats["observation_updates"] += self._write_api_response_observation_conn(
                    conn,
                    prepared.observation_action,
                    prepared.observation_values,
                )
                continue
            if decision.status != "changed":
                continue
            if decision.changed_columns:
                if decision.usage_event_id is None:
                    raise ValueError("missing imported usage event")
                assignments = ", ".join(
                    f"{column}=?" for column in decision.changed_columns
                )
                cursor = conn.execute(
                    f"UPDATE usage_events SET {assignments} WHERE id=?",
                    (
                        *[
                            decision.values[column]
                            for column in decision.changed_columns
                        ],
                        decision.usage_event_id,
                    ),
                )
                stats["usage_updates"] += max(int(cursor.rowcount), 0)
            if decision.observation_changed:
                stats["observation_updates"] += self._write_api_response_observation_conn(
                    conn,
                    prepared.observation_action,
                    prepared.observation_values,
                )
        return stats

    def sync_imported_events(
        self,
        events: Sequence[tuple[UsageEvent, str]],
        source: str,
        batch_size: int = IMPORTED_EVENT_SYNC_BATCH_SIZE,
        restore_retired_observations: bool = False,
    ) -> dict[str, int]:
        """Synchronize external events with read-first, bounded write batches."""

        safe_source = safe_alias(source)
        if not safe_source:
            raise ValueError("invalid import identity")
        prepared_events: list[_PreparedImportedEvent] = []
        seen_keys: set[str] = set()
        for event, import_key in events:
            safe_key = safe_text(import_key, 300)
            if not safe_key or safe_key in seen_keys:
                raise ValueError("invalid import identity")
            seen_keys.add(safe_key)
            event.source = safe_source
            self._validate_event_costs(event)
            values = self._minimized_event_values(event)
            observation_action, observation_values = self._api_response_observation_target(
                f"import:{safe_key}",
                values["ts"],
                values["status_code"],
                values["call_count"],
                safe_source,
                values["account_attempt"],
            )
            prepared_events.append(
                _PreparedImportedEvent(
                    import_key=safe_key,
                    source=safe_source,
                    values=values,
                    observation_action=observation_action,
                    observation_values=observation_values,
                )
            )

        stats = self._empty_import_sync_stats()
        bounded_batch_size = max(1, min(int(batch_size), IMPORTED_EVENT_SYNC_BATCH_SIZE))
        for offset in range(0, len(prepared_events), bounded_batch_size):
            batch = prepared_events[offset : offset + bounded_batch_size]
            with self.connect() as conn:
                classified = self._classify_imported_event_batch_conn(
                    conn,
                    batch,
                    restore_retired_observations,
                )
            if not any(decision.needs_write for decision in classified):
                batch_stats = self._empty_import_sync_stats()
                batch_stats["scanned"] = len(classified)
                for decision in classified:
                    batch_stats[decision.status] += 1
            else:
                with self.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    classified = self._classify_imported_event_batch_conn(
                        conn,
                        batch,
                        restore_retired_observations,
                    )
                    batch_stats = self._write_imported_event_batch_conn(conn, classified)
                batch_stats["write_transactions"] = 1
            for key, value in batch_stats.items():
                stats[key] += value
        return stats

    def sync_imported_event(
        self,
        event: UsageEvent,
        import_key: str,
        source: str,
        restore_retired_observation: bool = False,
    ) -> bool:
        """Synchronize one external event through the bounded batch path."""

        result = self.sync_imported_events(
            [(event, import_key)],
            source,
            restore_retired_observations=restore_retired_observation,
        )
        return bool(result["new"])

    def import_status(self, source: str = "codex_app_local") -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT COUNT(*) AS imported_events, MAX(imported_at) AS last_import_at
                     FROM local_import_records WHERE source=?""",
                (source,),
            ).fetchone()
        return dict(row)

    def remote_import_state(self, source: str) -> dict[str, Any] | None:
        safe_source = safe_alias(source)
        if not safe_source:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM remote_import_state WHERE source=?",
                (safe_source,),
            ).fetchone()
        return dict(row) if row is not None else None

    def save_remote_import_state(self, source: str, last_complete_at: Any) -> None:
        safe_source = safe_alias(source)
        completed = normalize_optional_timestamp(last_complete_at)
        if not safe_source or not completed:
            raise ValueError("invalid remote import state")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO remote_import_state (source, last_complete_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                  last_complete_at=excluded.last_complete_at,
                  updated_at=excluded.updated_at
                """,
                (safe_source, completed, utc_now()),
            )

    def api_response_backfill_required(
        self,
        source: str,
        version: int = RESPONSE_OBSERVATION_BACKFILL_VERSION,
    ) -> bool:
        safe_source = safe_alias(source)
        if safe_source not in RESPONSE_TIMELINE_SOURCES:
            return False
        with self.connect() as conn:
            row = conn.execute(
                "SELECT version FROM api_response_backfills WHERE source=?",
                (safe_source,),
            ).fetchone()
        return row is None or int(row["version"] or 0) < int(version)

    def mark_api_response_backfill(
        self,
        source: str,
        version: int = RESPONSE_OBSERVATION_BACKFILL_VERSION,
    ) -> None:
        safe_source = safe_alias(source)
        if safe_source not in RESPONSE_TIMELINE_SOURCES:
            raise ValueError("invalid API response source")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO api_response_backfills (source, version, completed_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                  version=excluded.version,
                  completed_at=excluded.completed_at
                """,
                (safe_source, int(version), utc_now()),
            )

    @staticmethod
    def _local_import_path_key(path: Path | str) -> str:
        value = str(path)
        if re.fullmatch(r"file:[0-9a-f]{16}", value):
            return value
        return "file:" + (short_hash(value) or "unknown")

    def local_import_file_state(self, path: Path | str) -> dict[str, Any] | None:
        path_key = self._local_import_path_key(path)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM local_import_files WHERE path=?", (path_key,)
            ).fetchone()
        return dict(row) if row is not None else None

    def local_import_file_states(
        self,
        paths: Sequence[Path | str],
    ) -> list[dict[str, Any] | None]:
        """Load many opaque file cursors with bounded SQLite parameter batches."""

        path_keys = [self._local_import_path_key(path) for path in paths]
        rows_by_key: dict[str, dict[str, Any]] = {}
        with self.connect() as conn:
            for start in range(0, len(path_keys), 500):
                batch = path_keys[start : start + 500]
                if not batch:
                    continue
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT * FROM local_import_files WHERE path IN ({placeholders})",
                    batch,
                ).fetchall()
                rows_by_key.update((str(row["path"]), dict(row)) for row in rows)
        return [rows_by_key.get(path_key) for path_key in path_keys]

    def save_local_import_file_state(self, state: Mapping[str, Any]) -> None:
        raw_path = safe_text(state.get("path"), 4096)
        values = {
            "path": self._local_import_path_key(raw_path or ""),
            "size": as_nonnegative_int(state.get("size")) or 0,
            "mtime_ns": as_nonnegative_int(state.get("mtime_ns")) or 0,
            "offset": as_nonnegative_int(state.get("offset")) or 0,
            "session_id": None,
            "model_provider": (
                "openai"
                if (safe_text(state.get("model_provider"), 64) or "").lower()
                == "openai"
                else None
            ),
            "model": safe_model_identifier(state.get("model")),
            "turn_id": None,
            "updated_at": utc_now(),
        }
        if not raw_path:
            raise ValueError("invalid local import path")
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO local_import_files
                   (path, size, mtime_ns, offset, session_id, model_provider,
                    model, turn_id, updated_at)
                   VALUES (:path, :size, :mtime_ns, :offset, :session_id,
                           :model_provider, :model, :turn_id, :updated_at)
                   ON CONFLICT(path) DO UPDATE SET
                     size=excluded.size, mtime_ns=excluded.mtime_ns,
                     offset=excluded.offset, session_id=excluded.session_id,
                     model_provider=excluded.model_provider, model=excluded.model,
                     turn_id=excluded.turn_id, updated_at=excluded.updated_at""",
                values,
            )

    def local_import_binding(self, home_key: str) -> dict[str, Any] | None:
        """Return one opaque Codex-home/account binding timeline entry."""

        if not re.fullmatch(r"home:[0-9a-f]{32}", home_key):
            raise ValueError("invalid local import home key")
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM local_import_bindings WHERE home_key=?",
                (home_key,),
            ).fetchone()
        return dict(row) if row is not None else None

    def save_local_import_binding(self, home_key: str, binding_key: str) -> None:
        """Persist only keyed digests proving the currently observed binding."""

        if not re.fullmatch(r"home:[0-9a-f]{32}", home_key):
            raise ValueError("invalid local import home key")
        if not re.fullmatch(r"binding:[0-9a-f]{32}", binding_key):
            raise ValueError("invalid local import binding key")
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO local_import_bindings
                   (home_key, binding_key, bound_at, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(home_key) DO UPDATE SET
                     binding_key=excluded.binding_key,
                     bound_at=CASE
                       WHEN local_import_bindings.binding_key=excluded.binding_key
                       THEN local_import_bindings.bound_at
                       ELSE excluded.bound_at
                     END,
                     updated_at=excluded.updated_at""",
                (home_key, binding_key, now, now),
            )

    def record_quota_hit(
        self,
        info: RequestInfo,
        ts: str,
        event_type: str,
        source: str,
        message: str | None,
        usage_event_id: int | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not self._subscription_detail_allowed_conn(conn, info.identity_key):
                return
            last_reset = self._latest_event_ts(conn, info.identity_key, RESET_EVENT_TYPES)
            last_quota = self._latest_event_ts(conn, info.identity_key, QUOTA_EVENT_TYPES)
            first_for_cycle = not last_quota or (last_reset is not None and last_reset > last_quota)
            conn.execute(
                """INSERT INTO quota_events
                (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                 event_type, source, raw_message_redacted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ts,
                    info.identity_key,
                    None,
                    None,
                    None,
                    event_type,
                    source,
                    None,
                ),
            )
            if first_for_cycle:
                quota_value = self._close_cycle(
                    conn,
                    info.identity_key,
                    ts,
                    complete=True,
                    end_source=source,
                    info=info,
                )
                if usage_event_id is not None:
                    conn.execute(
                        "UPDATE usage_events SET api_equivalent_quota_usd=? WHERE id=?",
                        (quota_value, usage_event_id),
                    )

    def mark_reset(self, alias: str, resolver: AccountResolver) -> str:
        ts = utc_now()
        info = self._info_for_alias(alias, resolver)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not self._subscription_detail_allowed_conn(conn, info.identity_key):
                return info.identity_key
            last_reset = self._latest_event_ts(conn, info.identity_key, RESET_EVENT_TYPES)
            last_quota = self._latest_event_ts(conn, info.identity_key, QUOTA_EVENT_TYPES)
            cycle_is_already_closed = bool(last_quota and (not last_reset or last_quota > last_reset))
            if not cycle_is_already_closed:
                self._close_cycle(conn, info.identity_key, ts, complete=False, end_source="manual", info=info)
            conn.execute(
                """INSERT INTO quota_events
                (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                 event_type, source, raw_message_redacted)
                VALUES (?, ?, ?, ?, ?, 'manual_reset', 'cli', NULL)""",
                (ts, info.identity_key, None, None, None),
            )
        return info.identity_key

    def mark_quota_hit(self, alias: str, resolver: AccountResolver) -> str:
        info = self._info_for_alias(alias, resolver)
        self.record_quota_hit(info, utc_now(), "manual_quota_hit", "cli", "manual quota-hit marker")
        return info.identity_key

    def _info_for_alias(self, alias: str, resolver: AccountResolver) -> RequestInfo:
        alias = safe_alias(alias)
        if not alias:
            raise ValueError("alias is empty")
        identity = resolver.resolve(alias, None)
        key = resolved_identity_key(identity, alias)
        key = canonical_subscription_key(key)
        if not key:
            raise ValueError("alias is not mapped to a canonical subscription")
        return RequestInfo(
            "manual",
            "CLI",
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            alias,
            None,
            None,
            identity.account_id_hash,
            identity.account_id_tail,
            key,
        )

    @staticmethod
    def _latest_event_ts(conn: sqlite3.Connection, key: str, event_types: set[str]) -> str | None:
        row = conn.execute(
            f"SELECT ts FROM quota_events WHERE identity_key=? AND event_type IN ({','.join('?' for _ in event_types)}) ORDER BY ts DESC, id DESC LIMIT 1",
            (key, *sorted(event_types)),
        ).fetchone()
        return row["ts"] if row else None

    def _close_cycle(
        self,
        conn: sqlite3.Connection,
        key: str,
        end_ts: str,
        complete: bool,
        end_source: str,
        info: RequestInfo,
    ) -> float | None:
        start_ts = self._latest_event_ts(conn, key, RESET_EVENT_TYPES)
        if start_ts is None:
            row = conn.execute(
                "SELECT MIN(ts) AS first_ts FROM usage_events WHERE identity_key=? AND ok=1 AND ts<=?", (key, end_ts)
            ).fetchone()
            if not row or row["first_ts"] is None:
                row = conn.execute(
                    "SELECT MIN(ts) AS first_ts FROM usage_events WHERE identity_key=? AND ts<=?", (key, end_ts)
                ).fetchone()
            start_ts = row["first_ts"] if row else None
        if start_ts is None:
            start_ts = end_ts
        duplicate = conn.execute(
            """SELECT id FROM account_quota_cycles
               WHERE identity_key=? AND cycle_start_ts=? AND is_complete_cycle=? LIMIT 1""",
            (key, start_ts, int(complete)),
        ).fetchone()
        if duplicate:
            row = conn.execute(
                "SELECT api_equivalent_quota_usd FROM account_quota_cycles WHERE id=?", (duplicate["id"],)
            ).fetchone()
            return row["api_equivalent_quota_usd"] if row else None
        totals = conn.execute(
            """
            SELECT
              COALESCE(SUM(call_count), 0) total_calls,
              COALESCE(SUM(CASE WHEN ok=1 THEN call_count ELSE 0 END), 0) successful_calls,
              COALESCE(SUM(CASE WHEN ok=0 THEN call_count ELSE 0 END), 0) failed_calls,
              COALESCE(SUM(CASE WHEN stream=1 THEN call_count ELSE 0 END), 0) streaming_calls,
              COALESCE(SUM(input_tokens), 0) total_input_tokens,
              COALESCE(SUM(cached_tokens), 0) total_cached_tokens,
              COALESCE(SUM(output_tokens), 0) total_output_tokens,
              COALESCE(SUM(reasoning_tokens), 0) total_reasoning_tokens,
              COALESCE(SUM(total_tokens), 0) total_tokens,
              SUM(estimated_api_cost_usd) estimated_api_cost_usd
            FROM usage_events WHERE identity_key=? AND ts>=? AND ts<=?
            """,
            (key, start_ts, end_ts),
        ).fetchone()
        cost = totals["estimated_api_cost_usd"]
        reset_source_row = conn.execute(
            f"SELECT source FROM quota_events WHERE identity_key=? AND event_type IN ({','.join('?' for _ in RESET_EVENT_TYPES)}) AND ts=? ORDER BY id DESC LIMIT 1",
            (key, *sorted(RESET_EVENT_TYPES), start_ts),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO account_quota_cycles (
              identity_key, account_id_hash, account_id_tail, usage_alias,
              cycle_start_ts, cycle_end_ts, reset_detected_by,
              quota_hit_detected_by, total_calls, successful_calls, failed_calls,
              streaming_calls, total_input_tokens, total_cached_tokens,
              total_output_tokens, total_reasoning_tokens, total_tokens,
              estimated_api_cost_usd, observed_floor_usd,
              api_equivalent_quota_usd, is_complete_cycle, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                None,
                None,
                None,
                start_ts,
                end_ts,
                reset_source_row["source"] if reset_source_row else "first_observed_request",
                end_source if complete else None,
                totals["total_calls"],
                totals["successful_calls"],
                totals["failed_calls"],
                totals["streaming_calls"],
                totals["total_input_tokens"],
                totals["total_cached_tokens"],
                totals["total_output_tokens"],
                totals["total_reasoning_tokens"],
                totals["total_tokens"],
                cost,
                cost,
                cost if complete else None,
                int(complete),
                None,
            ),
        )
        return cost if complete else None

    def summary(self, period: str) -> dict[str, Any]:
        start = period_start(period)
        with self.connect() as conn:
            row = conn.execute(
                """
                WITH filtered AS (
                  SELECT * FROM usage_statistics WHERE ts>=?
                ), identified_requests AS (
                  SELECT request_id,
                         MAX(CASE WHEN ok=1 THEN 1 ELSE 0 END) logical_ok,
                         MAX(CASE WHEN stream=1 THEN 1 ELSE 0 END) logical_stream
                    FROM filtered
                   WHERE account_attempt_count>0 AND request_id IS NOT NULL
                   GROUP BY request_id
                )
                SELECT
                  COALESCE(SUM(call_count), 0) calls,
                  COALESCE(SUM(account_attempt_count), 0) account_attempts,
                  COALESCE(SUM(CASE WHEN ok=1 THEN call_count ELSE 0 END), 0) successful_calls,
                  COALESCE(SUM(CASE WHEN ok=0 THEN call_count ELSE 0 END), 0) failed_calls,
                  COALESCE(SUM(CASE WHEN ok=1 THEN account_attempt_count ELSE 0 END), 0)
                    successful_account_attempts,
                  COALESCE(SUM(CASE WHEN ok=0 THEN account_attempt_count ELSE 0 END), 0)
                    failed_account_attempts,
                  COALESCE(SUM(streaming_call_count), 0) streaming_calls,
                  (SELECT COUNT(*) FROM identified_requests)
                    + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                         AND request_id IS NULL
                                        THEN account_attempt_count ELSE 0 END), 0)
                    logical_requests,
                  COALESCE((SELECT SUM(logical_ok) FROM identified_requests), 0)
                    + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                         AND request_id IS NULL AND ok=1
                                        THEN account_attempt_count ELSE 0 END), 0)
                    successful_logical_requests,
                  COALESCE((SELECT SUM(CASE WHEN logical_ok=0 THEN 1 ELSE 0 END)
                              FROM identified_requests), 0)
                    + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                         AND request_id IS NULL AND ok=0
                                        THEN account_attempt_count ELSE 0 END), 0)
                    failed_logical_requests,
                  COALESCE((SELECT SUM(logical_stream) FROM identified_requests), 0)
                    + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                         AND request_id IS NULL
                                        THEN streaming_call_count ELSE 0 END), 0)
                    streaming_logical_requests,
                  COALESCE(SUM(input_tokens), 0) input_tokens,
                  COALESCE(SUM(output_tokens), 0) output_tokens,
                  COALESCE(SUM(cached_tokens), 0) cached_tokens,
                  COALESCE(SUM(reasoning_tokens), 0) reasoning_tokens,
                  COALESCE(SUM(total_tokens), 0) total_tokens,
                  COALESCE(SUM(CASE WHEN long_context_pricing_applied=1 THEN call_count ELSE 0 END), 0)
                    long_context_priced_calls,
                  COALESCE(SUM(non_cached_input_token_count), 0)
                    non_cached_input_tokens,
                  COALESCE(SUM(codex_status_token_count), 0)
                    codex_status_tokens,
                  COALESCE(SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)), 0)
                    api_processed_tokens,
                  SUM(estimated_api_cost_usd) estimated_api_cost_usd,
                  SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                 AND cached_input_cost_usd IS NOT NULL
                                 AND output_cost_usd IS NOT NULL
                           THEN non_cached_input_cost_usd END) non_cached_input_cost_usd,
                  SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                 AND cached_input_cost_usd IS NOT NULL
                                 AND output_cost_usd IS NOT NULL
                           THEN cached_input_cost_usd END) cached_input_cost_usd,
                  SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                 AND cached_input_cost_usd IS NOT NULL
                                 AND output_cost_usd IS NOT NULL
                           THEN output_cost_usd END) output_cost_usd,
                  COALESCE(SUM(CASE
                    WHEN non_cached_input_cost_usd IS NOT NULL
                     AND cached_input_cost_usd IS NOT NULL
                     AND output_cost_usd IS NOT NULL
                    THEN call_count ELSE 0 END), 0) split_priced_events,
                  COALESCE(SUM(CASE WHEN estimated_api_cost_usd IS NOT NULL THEN call_count ELSE 0 END), 0) priced_calls,
                  COALESCE(SUM(CASE WHEN usage_missing=1 THEN call_count ELSE 0 END), 0) usage_missing_calls
                FROM filtered
                """,
                (start,),
            ).fetchone()
        result = dict(row)
        split_values = (
            result.get("non_cached_input_cost_usd"),
            result.get("cached_input_cost_usd"),
            result.get("output_cost_usd"),
        )
        result["split_cost_total_usd"] = (
            sum(float(value) for value in split_values if value is not None)
            if int(result.get("split_priced_events") or 0) > 0
            else None
        )
        input_tokens = int(result["input_tokens"] or 0)
        cached_tokens = int(result["cached_tokens"] or 0)
        result["cache_hit_rate_percent"] = (
            cached_tokens / input_tokens * 100.0 if input_tokens else None
        )
        result["retry_attempts"] = max(
            int(result["account_attempts"] or 0) - int(result["logical_requests"] or 0), 0
        )
        with self.connect() as conn:
            quota_row = conn.execute(
                """SELECT SUM(api_equivalent_quota_usd) AS api_equivalent_quota_usd
                   FROM account_quota_cycles
                   WHERE is_complete_cycle=1 AND cycle_end_ts>=?""",
                (start,),
            ).fetchone()
            quota_value = quota_row["api_equivalent_quota_usd"]
            if quota_value is None:
                # ``usage_events`` is intentionally committed before quota
                # transition bookkeeping so a proxy response is never held
                # up by SQLite.  A reader can therefore briefly observe the
                # event row before ``account_quota_cycles`` is committed.
                # Use the just-observed quota error as a conservative,
                # race-safe fallback until the cycle row appears.
                pending = conn.execute(
                    """SELECT identity_key, MAX(ts) AS ts
                         FROM quota_events
                        WHERE ts>=? AND event_type IN ({})
                        GROUP BY identity_key""".format(
                            ",".join("?" for _ in QUOTA_EVENT_TYPES)
                        ),
                    (start, *sorted(QUOTA_EVENT_TYPES)),
                ).fetchall()
                if pending:
                    fallback = 0.0
                    found = False
                    for item in pending:
                        row_cost = conn.execute(
                            """SELECT SUM(estimated_api_cost_usd) AS cost
                                 FROM usage_events
                                WHERE identity_key=? AND ts>=? AND ts<=?""",
                            (item["identity_key"], start, item["ts"]),
                        ).fetchone()
                        if row_cost["cost"] is not None:
                            fallback += float(row_cost["cost"])
                            found = True
                    quota_value = fallback if found else None
            if quota_value is None:
                # The quota_events insert itself can trail the usage-event
                # commit on a concurrent request.  Infer only the narrow,
                # provider-visible quota shapes from the event row as a
                # read-time fallback; the durable cycle remains authoritative.
                pending_events = conn.execute(
                    """SELECT identity_key, MAX(ts) AS ts
                         FROM usage_events
                        WHERE ts>=? AND (
                              status_code=429 OR
                              lower(COALESCE(error_type,'')) LIKE '%quota%' OR
                              lower(COALESCE(error_message_redacted,'')) LIKE '%usage limit%' OR
                              lower(COALESCE(error_message_redacted,'')) LIKE '%rate limit%'
                        )
                        GROUP BY identity_key""",
                    (start,),
                ).fetchall()
                if pending_events:
                    fallback = 0.0
                    found = False
                    for item in pending_events:
                        row_cost = conn.execute(
                            """SELECT SUM(estimated_api_cost_usd) AS cost
                                 FROM usage_events
                                WHERE identity_key=? AND ts>=? AND ts<=?""",
                            (item["identity_key"], start, item["ts"]),
                        ).fetchone()
                        if row_cost["cost"] is not None:
                            fallback += float(row_cost["cost"])
                            found = True
                    quota_value = fallback if found else None
        result["api_equivalent_quota_usd"] = quota_value
        result["period"] = period
        result["since"] = None if str(period).strip().lower() in ALL_TIME_PERIODS else start
        return result

    def cost_breakdown(self, period: str) -> dict[str, Any]:
        """Return the immutable per-event cost split from one SQLite snapshot."""

        start = period_start(period)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                      AND cached_input_cost_usd IS NOT NULL
                                      AND output_cost_usd IS NOT NULL
                                THEN non_cached_input_cost_usd END) non_cached_input_cost_usd,
                       SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                      AND cached_input_cost_usd IS NOT NULL
                                      AND output_cost_usd IS NOT NULL
                                THEN cached_input_cost_usd END) cached_input_cost_usd,
                       SUM(CASE WHEN non_cached_input_cost_usd IS NOT NULL
                                      AND cached_input_cost_usd IS NOT NULL
                                      AND output_cost_usd IS NOT NULL
                                THEN output_cost_usd END) output_cost_usd,
                       COALESCE(SUM(CASE
                         WHEN non_cached_input_cost_usd IS NOT NULL
                          AND cached_input_cost_usd IS NOT NULL
                          AND output_cost_usd IS NOT NULL
                         THEN call_count ELSE 0 END), 0) split_priced_events
                  FROM usage_statistics
                 WHERE ts>=?
                """,
                (start,),
            ).fetchone()
        result = dict(row)
        components = (
            result.get("non_cached_input_cost_usd"),
            result.get("cached_input_cost_usd"),
            result.get("output_cost_usd"),
        )
        result["split_cost_total_usd"] = (
            sum(float(value) for value in components if value is not None)
            if int(result.get("split_priced_events") or 0) > 0
            else None
        )
        return result

    def coverage(self) -> dict[str, Any]:
        """Return the locally recorded time span without exposing any secrets."""

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS event_rows,
                       COALESCE(SUM(call_count), 0) AS calls,
                       COALESCE(SUM(account_attempt_count), 0) AS account_attempts,
                       COUNT(DISTINCT CASE WHEN account_attempt_count>0 THEN request_id END)
                         + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                              AND request_id IS NULL
                                             THEN account_attempt_count ELSE 0 END), 0)
                         AS logical_requests,
                       COALESCE(SUM(CASE WHEN session_id IS NOT NULL THEN call_count ELSE 0 END), 0)
                         AS session_identified_attempts,
                       COALESCE(SUM(CASE WHEN request_id IS NOT NULL THEN call_count ELSE 0 END), 0)
                         AS request_id_identified_attempts,
                       MIN(ts) AS first_event_ts,
                       MAX(ts) AS last_event_ts
                FROM usage_statistics
                """
            ).fetchone()
        return dict(row)

    def response_timeline(
        self,
        minutes: int = DEFAULT_RESPONSE_TIMELINE_MINUTES,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Return the dense one-minute HTTP response timeline.

        ``status_200`` counts status 200, including the synthetic success
        status on local Codex usage imports. Local imports cannot observe
        HTTP failures. Every other status (including the synthetic 502 when
        the upstream does not answer) is counted in ``status_non_200``. The
        fixed scope includes proxy, usage-queue, Cockpit Tools, Sub2API, and
        local Codex request events. Local gateway decisions made before
        account selection and manual token totals are excluded.

        The returned buckets are UTC and include zero-valued minutes so a
        client can draw a continuous line without inventing missing samples.
        Observations are identity-free and survive account-detail retirement;
        ``now`` is injectable for deterministic callers and tests.
        """

        try:
            window_minutes = int(minutes)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("minutes must be an integer") from exc
        if not 1 <= window_minutes <= MAX_RESPONSE_TIMELINE_MINUTES:
            raise ValueError(
                f"minutes must be between 1 and {MAX_RESPONSE_TIMELINE_MINUTES}"
            )

        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc).replace(second=0, microsecond=0)
        start = current - timedelta(minutes=window_minutes - 1)
        end = current + timedelta(minutes=1)
        start_text = start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_text = end.isoformat(timespec="seconds").replace("+00:00", "Z")
        placeholders = ",".join("?" for _ in RESPONSE_TIMELINE_SOURCES)

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT minute_ts AS bucket,
                       COALESCE(SUM(CASE WHEN status_code=200
                                         THEN call_count ELSE 0 END), 0)
                         AS status_200,
                       COALESCE(SUM(CASE WHEN status_code IS NULL
                                              OR status_code!=200
                                         THEN call_count ELSE 0 END), 0)
                         AS status_non_200
                  FROM api_response_observations
                 WHERE minute_ts>=? AND minute_ts<?
                   AND source IN ({placeholders})
                 GROUP BY bucket
                """,
                (start_text, end_text, *RESPONSE_TIMELINE_SOURCES),
            ).fetchall()

        by_bucket = {
            str(row["bucket"]): {
                "ts": str(row["bucket"]),
                "status_200": int(row["status_200"] or 0),
                "status_non_200": int(row["status_non_200"] or 0),
            }
            for row in rows
            if row["bucket"]
        }
        points: list[dict[str, Any]] = []
        for offset in range(window_minutes):
            bucket = start + timedelta(minutes=offset)
            bucket_text = bucket.isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )
            point = by_bucket.get(
                bucket_text,
                {"ts": bucket_text, "status_200": 0, "status_non_200": 0},
            )
            point["total"] = point["status_200"] + point["status_non_200"]
            points.append(point)

        total_200 = sum(point["status_200"] for point in points)
        total_non_200 = sum(point["status_non_200"] for point in points)
        return {
            "interval": "1m",
            "window_minutes": window_minutes,
            "timezone": "UTC",
            "source": "api",
            "sources": list(RESPONSE_TIMELINE_SOURCES),
            "from": start_text,
            "to": current.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "totals": {
                "status_200": total_200,
                "status_non_200": total_non_200,
                "total": total_200 + total_non_200,
            },
            "points": points,
        }

    def token_breakdown(self, period: str) -> dict[str, Any]:
        summary = self.summary(period)
        return {
            "period": period,
            "input_tokens": summary["input_tokens"],
            "cached_tokens": summary["cached_tokens"],
            "non_cached_input_tokens": summary["non_cached_input_tokens"],
            "output_tokens": summary["output_tokens"],
            "reasoning_tokens": summary["reasoning_tokens"],
            "total_tokens": summary["total_tokens"],
            "codex_status_tokens": summary["codex_status_tokens"],
            "api_processed_tokens": summary["api_processed_tokens"],
            "cache_hit_rate_percent": summary["cache_hit_rate_percent"],
            "estimated_api_cost_usd": summary["estimated_api_cost_usd"],
            "non_cached_input_cost_usd": summary["non_cached_input_cost_usd"],
            "cached_input_cost_usd": summary["cached_input_cost_usd"],
            "output_cost_usd": summary["output_cost_usd"],
            "split_cost_total_usd": summary["split_cost_total_usd"],
            "split_priced_events": summary["split_priced_events"],
            "calls": summary["calls"],
            "account_attempts": summary["account_attempts"],
            "logical_requests": summary["logical_requests"],
            "retry_attempts": summary["retry_attempts"],
            "successful_logical_requests": summary["successful_logical_requests"],
            "failed_logical_requests": summary["failed_logical_requests"],
            "streaming_logical_requests": summary["streaming_logical_requests"],
            "successful_attempts": summary["successful_account_attempts"],
            "failed_attempts": summary["failed_account_attempts"],
            "streaming_attempts": summary["streaming_calls"],
        }

    def daily_usage(self, days: int = 7) -> list[dict[str, Any]]:
        """Return a dense local-date series, including zero-usage days."""

        days = max(1, min(int(days), 366))
        end = datetime.now().astimezone().date()
        start = end - timedelta(days=days - 1)
        start_local = datetime.combine(start, datetime.min.time()).astimezone()
        start_utc = start_local.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT date(ts, 'localtime') AS date,
                       COALESCE(SUM(call_count), 0) AS calls,
                       COALESCE(SUM(account_attempt_count), 0) AS account_attempts,
                       COUNT(DISTINCT CASE WHEN account_attempt_count>0 THEN request_id END)
                         + COALESCE(SUM(CASE WHEN account_attempt_count>0
                                              AND request_id IS NULL
                                             THEN account_attempt_count ELSE 0 END), 0)
                         AS logical_requests,
                       COALESCE(SUM(total_tokens), 0) AS total_tokens,
                       COALESCE(SUM(input_tokens), 0) AS input_tokens,
                       COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
                       COALESCE(SUM(output_tokens), 0) AS output_tokens,
                       COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                       COALESCE(SUM(non_cached_input_token_count), 0)
                         AS non_cached_input_tokens,
                       COALESCE(SUM(codex_status_token_count), 0)
                         AS codex_status_tokens,
                       COALESCE(SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)), 0)
                         AS api_processed_tokens,
                       SUM(estimated_api_cost_usd) AS estimated_api_cost_usd,
                       SUM(non_cached_input_cost_usd) AS non_cached_input_cost_usd,
                       SUM(cached_input_cost_usd) AS cached_input_cost_usd,
                       SUM(output_cost_usd) AS output_cost_usd
                  FROM usage_statistics
                 WHERE ts>=?
                 GROUP BY date(ts, 'localtime')
                """,
                (start_utc,),
            ).fetchall()
        by_date = {row["date"]: dict(row) for row in rows}
        result: list[dict[str, Any]] = []
        for offset in range(days):
            day = (start + timedelta(days=offset)).isoformat()
            result.append(
                by_date.get(
                    day,
                    {
                        "date": day,
                        "calls": 0,
                        "account_attempts": 0,
                        "logical_requests": 0,
                        "total_tokens": 0,
                        "input_tokens": 0,
                        "cached_tokens": 0,
                        "output_tokens": 0,
                        "reasoning_tokens": 0,
                        "non_cached_input_tokens": 0,
                        "codex_status_tokens": 0,
                        "api_processed_tokens": 0,
                        "estimated_api_cost_usd": None,
                        "non_cached_input_cost_usd": None,
                        "cached_input_cost_usd": None,
                        "output_cost_usd": None,
                    },
                )
            )
        return result

    def insert_subscription_quota_snapshot(self, snapshot: Mapping[str, Any]) -> bool:
        allowed_windows = {"five_hour", "weekly", "monthly", "account_status"}
        window_kind = safe_text(snapshot.get("window_kind"), 32)
        if window_kind not in allowed_windows:
            raise ValueError("invalid subscription quota window")
        used = snapshot.get("used_percent")
        remaining = snapshot.get("remaining_percent")
        used_value = min(max(float(used), 0.0), 100.0) if used is not None else None
        remaining_value = min(max(float(remaining), 0.0), 100.0) if remaining is not None else None
        values = {
            "fetched_at": normalize_timestamp(snapshot.get("fetched_at")),
            "identity_key": canonical_subscription_key(snapshot.get("identity_key")),
            "account_id_hash": None,
            "account_id_tail": None,
            "usage_alias": None,
            "plan_type": safe_plan_type(snapshot.get("plan_type")),
            "subscription_active_until": normalize_optional_timestamp(
                snapshot.get("subscription_active_until")
            ),
            "window_kind": window_kind,
            "used_percent": used_value,
            "remaining_percent": remaining_value,
            "window_seconds": as_nonnegative_int(snapshot.get("window_seconds")),
            "reset_at": normalize_optional_timestamp(snapshot.get("reset_at")),
            "estimated_full_quota_usd": snapshot.get("estimated_full_quota_usd"),
            "estimated_remaining_quota_usd": snapshot.get("estimated_remaining_quota_usd"),
            "estimate_method": safe_alias(snapshot.get("estimate_method")),
            "provider_allowed": (
                int(snapshot.get("provider_allowed"))
                if isinstance(snapshot.get("provider_allowed"), bool)
                else None
            ),
            "provider_limit_reached": (
                int(snapshot.get("provider_limit_reached"))
                if isinstance(snapshot.get("provider_limit_reached"), bool)
                else None
            ),
            "source": safe_alias(snapshot.get("source")) or "cliproxy_wham_usage",
        }
        if not values["identity_key"]:
            raise ValueError("invalid subscription identity")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not self._subscription_detail_allowed_conn(
                conn, values["identity_key"]
            ):
                return False
            # Sparse management/JWT responses can omit either field
            # independently.  Look up each value independently as well: a
            # newer plan-only row must not hide an older, still valid renewal
            # timestamp (and vice versa).
            for field in ("plan_type", "subscription_active_until"):
                if values[field] is not None:
                    continue
                previous = conn.execute(
                    f"""SELECT {field}
                          FROM subscription_quota_snapshots
                         WHERE identity_key=? AND {field} IS NOT NULL
                         ORDER BY fetched_at DESC, id DESC LIMIT 1""",
                    (values["identity_key"],),
                ).fetchone()
                if previous is not None:
                    values[field] = previous[field]
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO subscription_quota_snapshots (
                  fetched_at, identity_key, account_id_hash, account_id_tail,
                  usage_alias, plan_type, subscription_active_until, window_kind,
                  used_percent, remaining_percent, window_seconds, reset_at,
                  estimated_full_quota_usd, estimated_remaining_quota_usd,
                  estimate_method, provider_allowed, provider_limit_reached,
                  source
                ) VALUES (
                  :fetched_at, :identity_key, :account_id_hash, :account_id_tail,
                  :usage_alias, :plan_type, :subscription_active_until, :window_kind,
                  :used_percent, :remaining_percent, :window_seconds, :reset_at,
                  :estimated_full_quota_usd, :estimated_remaining_quota_usd,
                  :estimate_method, :provider_allowed, :provider_limit_reached,
                  :source
                )
                """,
                values,
            )
            inserted = max(int(cursor.rowcount), 0) > 0
            if not inserted and (
                values["provider_allowed"] is not None
                or values["provider_limit_reached"] is not None
            ):
                # A meter upgraded in place may already have this exact
                # snapshot under the unique key, but without the newly
                # allowlisted provider gate booleans.  Fill those fields
                # without fabricating a second quota observation.
                conn.execute(
                    """UPDATE subscription_quota_snapshots
                          SET provider_allowed=
                                CASE WHEN :provider_allowed IS NULL
                                     THEN provider_allowed
                                     ELSE :provider_allowed END,
                              provider_limit_reached=
                                CASE WHEN :provider_limit_reached IS NULL
                                     THEN provider_limit_reached
                                     ELSE :provider_limit_reached END
                        WHERE identity_key=:identity_key
                          AND window_kind=:window_kind
                          AND fetched_at=:fetched_at""",
                    values,
                )
            return inserted

    def latest_subscription_quotas(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT q.*
                  FROM subscription_quota_snapshots q
                 WHERE NOT EXISTS (
                       SELECT 1
                         FROM active_subscription_registry retired
                        WHERE retired.identity_key=q.identity_key
                          AND retired.state='suspect_missing'
                       )
                   AND NOT EXISTS (
                       SELECT 1 FROM retired_subscription_tombstones tombstone
                        WHERE tombstone.identity_key=q.identity_key
                       )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM subscription_quota_snapshots newer
                        WHERE newer.identity_key=q.identity_key
                          AND newer.window_kind=q.window_kind
                          AND (
                                newer.fetched_at > q.fetched_at
                                OR (newer.fetched_at=q.fetched_at AND newer.id > q.id)
                              )
                       )
                   AND NOT (
                       q.identity_key LIKE 'account:%'
                       AND EXISTS (
                           SELECT 1
                             FROM subscription_quota_snapshots scoped
                            WHERE scoped.account_id_hash=q.account_id_hash
                              AND scoped.identity_key LIKE 'subscription:%'
                              AND scoped.fetched_at>=q.fetched_at
                       )
                   )
                 ORDER BY COALESCE(q.usage_alias, q.account_id_tail, q.identity_key),
                          CASE q.window_kind
                            WHEN 'five_hour' THEN 0
                            WHEN 'weekly' THEN 1
                            ELSE 2
                          END
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def subscription_dashboard_rows(self) -> list[dict[str, Any]]:
        quotas = self.latest_subscription_quotas()
        by_key: dict[str, dict[str, Any]] = {}
        for quota in quotas:
            key = quota["identity_key"]
            entry = by_key.setdefault(
                key,
                {
                    "identity_key": key,
                    "usage_alias": quota.get("usage_alias"),
                    "account_id_tail": quota.get("account_id_tail"),
                    "account_id_hash": quota.get("account_id_hash"),
                    "plan_type": None,
                    "subscription_active_until": None,
                    "fetched_at": quota.get("fetched_at"),
                    "windows": {},
                },
            )
            entry["windows"][quota["window_kind"]] = quota
            metadata_rank = (
                str(quota.get("fetched_at") or ""),
                int(quota.get("id") or 0),
            )
            for field in ("plan_type", "subscription_active_until"):
                value = quota.get(field)
                rank_key = f"_{field}_rank"
                if value is not None and metadata_rank > entry.get(
                    rank_key, ("", -1)
                ):
                    entry[field] = value
                    entry[rank_key] = metadata_rank
            if (quota.get("fetched_at") or "") > (entry.get("fetched_at") or ""):
                entry["fetched_at"] = quota.get("fetched_at")

        all_accounts = {row["identity_key"]: row for row in self.grouped("all", "account")}
        quota_cycles = {row["identity_key"]: row for row in self.quota_summary("all")}
        # Estimate from one SQLite snapshot per dashboard render.  The WHAM
        # percentage is the authoritative subscription window signal; event
        # cycles remain a fallback because transient 429s can fragment them.
        with self.connect() as conn:
            for entry in by_key.values():
                windows = entry.get("windows") or {}
                preferred = windows.get("weekly") or windows.get("monthly")
                if not preferred:
                    continue
                estimate = self._subscription_window_estimate(conn, entry, preferred)
                entry.update(estimate)
        for key, account in all_accounts.items():
            entry = by_key.setdefault(
                key,
                {
                    "identity_key": key,
                    "usage_alias": account.get("usage_alias"),
                    "account_id_tail": account.get("account_id_tail"),
                    "account_id_hash": account.get("account_id_hash"),
                    "plan_type": None,
                    "subscription_active_until": None,
                    "fetched_at": None,
                    "windows": {},
                },
            )
            entry["all_time_calls"] = account.get("calls")
            entry["all_time_account_attempts"] = account.get("account_attempts")
            entry["all_time_logical_requests"] = account.get("logical_requests")
            entry["all_time_successful_calls"] = account.get("successful_calls")
            entry["all_time_failed_calls"] = account.get("failed_calls")
            entry["all_time_extra_calls"] = account.get("retry_attempts")
            entry["all_time_tokens"] = account.get("total_tokens")
            entry["all_time_codex_status_tokens"] = account.get("codex_status_tokens")
            entry["all_time_non_cached_input_tokens"] = account.get("non_cached_input_tokens")
            entry["all_time_output_tokens"] = account.get("output_tokens")
            entry["all_time_cached_tokens"] = account.get("cached_tokens")
            entry["all_time_api_processed_tokens"] = account.get("api_processed_tokens")
            entry["all_time_cost_usd"] = account.get("estimated_api_cost_usd")
            entry["all_time_non_cached_input_cost_usd"] = account.get(
                "non_cached_input_cost_usd"
            )
            entry["all_time_cached_input_cost_usd"] = account.get(
                "cached_input_cost_usd"
            )
            entry["all_time_output_cost_usd"] = account.get("output_cost_usd")
            cycle = quota_cycles.get(key, {})
            if entry.get("current_cycle_floor_usd") is None:
                entry["current_cycle_floor_usd"] = cycle.get("current_cycle_observed_floor_usd")
            entry["historical_complete_cycle_usd"] = cycle.get(
                "last_complete_cycle_api_equivalent_quota_usd"
            )
            if entry.get("current_window_full_quota_usd") is None and not entry.get("windows"):
                entry["current_window_full_quota_usd"] = entry["historical_complete_cycle_usd"]
        with self.connect() as conn:
            latest_attempts = {
                row["identity_key"]: dict(row)
                for row in conn.execute(
                    """
                    SELECT event.identity_key, event.ts, event.status_code, event.ok
                      FROM usage_events event
                     WHERE event.identity_key IS NOT NULL
                       AND event.identity_key!='unknown'
                       AND COALESCE(event.account_attempt, 1)=1
                       AND NOT EXISTS (
                           SELECT 1
                             FROM usage_events newer
                            WHERE newer.identity_key=event.identity_key
                              AND COALESCE(newer.account_attempt, 1)=1
                              AND (
                                    newer.ts>event.ts
                                    OR (newer.ts=event.ts AND newer.id>event.id)
                                  )
                       )
                    """
                )
            }
            latest_usage_limits = {
                row["identity_key"]: row["ts"]
                for row in conn.execute(
                    """
                    SELECT identity_key, MAX(ts) ts
                      FROM quota_events
                     WHERE event_type IN ('usage_limit_hit', 'quota_hit', 'cooldown_hit')
                       AND identity_key IS NOT NULL
                     GROUP BY identity_key
                    """
                )
            }
        for key, entry in by_key.items():
            attempt = latest_attempts.get(key)
            attempt_at = str(attempt.get("ts") or "") if attempt else ""
            if attempt is not None:
                entry["last_execution_at"] = attempt.get("ts")
                entry["last_execution_status"] = attempt.get("status_code")

            provider_signals: list[tuple[tuple[str, int], Mapping[str, Any]]] = []
            reported_zero_at = ""
            for window in (entry.get("windows") or {}).values():
                if not isinstance(window, Mapping):
                    continue
                fetched_at = str(window.get("fetched_at") or "")
                remaining = _percent_value(window.get("remaining_percent"))
                used = _percent_value(window.get("used_percent"))
                if (
                    (remaining is not None and remaining <= 0.0001)
                    or (used is not None and used >= 99.9999)
                ) and fetched_at > reported_zero_at:
                    reported_zero_at = fetched_at
                if (
                    window.get("provider_allowed") is not None
                    or window.get("provider_limit_reached") is not None
                ):
                    provider_signals.append(
                        (
                            (fetched_at, int(window.get("id") or 0)),
                            window,
                        )
                    )
            if reported_zero_at:
                entry["reported_zero_at"] = reported_zero_at

            provider_at = ""
            provider_exhausted = False
            provider_available = False
            if provider_signals:
                (provider_at, _), provider = max(
                    provider_signals,
                    key=lambda item: item[0],
                )
                allowed = provider.get("provider_allowed")
                limit_reached = provider.get("provider_limit_reached")
                provider_exhausted = bool(
                    (limit_reached is not None and int(limit_reached) == 1)
                    or (allowed is not None and int(allowed) == 0)
                )
                provider_available = bool(
                    allowed is not None
                    and int(allowed) == 1
                    and not (
                        limit_reached is not None and int(limit_reached) == 1
                    )
                )
                entry["provider_gate_at"] = provider_at
                entry["provider_allowed"] = (
                    None if allowed is None else bool(int(allowed))
                )
                entry["provider_limit_reached"] = (
                    None if limit_reached is None else bool(int(limit_reached))
                )

            # Execution results and the structured provider gate are separate
            # clocks.  Use the newest signal: a later success can disprove a
            # stale cooldown, while a later ``allowed=false`` snapshot can
            # confirm exhaustion even when Cockpit's aggregate request log
            # records only the final successful retry on another account.
            if (
                provider_at
                and provider_at >= attempt_at
                and (provider_exhausted or provider_available)
            ):
                if provider_exhausted:
                    entry["execution_availability"] = "confirmed_exhausted"
                    entry["availability_source"] = "provider_gate"
                elif provider_available:
                    entry["execution_availability"] = "provider_available"
                    entry["availability_source"] = "provider_gate"
            elif attempt is not None:
                if (
                    int(attempt.get("status_code") or 0) == 429
                    and str(latest_usage_limits.get(key) or "") >= attempt_at
                ):
                    entry["execution_availability"] = "confirmed_exhausted"
                    entry["availability_source"] = "execution_429"
                elif int(attempt.get("ok") or 0) == 1:
                    if reported_zero_at and reported_zero_at >= attempt_at:
                        entry["execution_availability"] = "success_before_zero_snapshot"
                    else:
                        entry["execution_availability"] = "recent_success"
                    entry["availability_source"] = "execution"
                else:
                    entry["execution_availability"] = "recent_failure"
                    entry["availability_source"] = "execution"
        scoped_account_hashes = {
            entry.get("account_id_hash")
            for entry in by_key.values()
            if str(entry.get("identity_key") or "").startswith("subscription:")
            and entry.get("account_id_hash")
        }
        for entry in by_key.values():
            entry.pop("_plan_type_rank", None)
            entry.pop("_subscription_active_until_rank", None)
            entry["legacy_ambiguous"] = bool(
                str(entry.get("identity_key") or "").startswith("account:")
                and entry.get("account_id_hash") in scoped_account_hashes
            )
        result = list(by_key.values())
        result.sort(
            key=lambda row: (
                0 if row.get("usage_alias") else 1,
                numeric_alias_key(row.get("usage_alias")),
                str(row.get("account_id_tail") or row.get("identity_key")),
            )
        )
        return result

    def _subscription_window_estimate(
        self,
        conn: sqlite3.Connection,
        entry: Mapping[str, Any],
        window: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return a conservative USD-equivalent estimate for one live window.

        ``used_percent`` is a provider quota signal, not an API-token ratio.
        It is therefore only used as a projection after 5% usage, and a
        a 100%-used window uses the observed spend as the strongest estimate;
        at 95% used (5% remaining) the normal percentage projection is still
        applied.  A freshly reset/low-use account deliberately remains
        ``观测中`` instead of extrapolating a noisy tiny denominator.
        """

        account_hash = safe_text(entry.get("account_id_hash"), 64)
        identity_key_value = safe_text(entry.get("identity_key"), 300)
        reset_text = normalize_optional_timestamp(window.get("reset_at"))
        # The provider percentage is sampled at ``fetched_at`` but queue rows
        # may arrive between polls.  Include them through render time so the
        # observed floor never lags the local collector by a full poll period.
        end_text = max(normalize_timestamp(window.get("fetched_at")), utc_now())
        start_text: str | None = None
        if reset_text and window.get("window_seconds"):
            try:
                reset_dt = datetime.fromisoformat(reset_text.replace("Z", "+00:00"))
                start_text = (
                    reset_dt - timedelta(seconds=int(window["window_seconds"]))
                ).isoformat(timespec="microseconds").replace("+00:00", "Z")
            except (TypeError, ValueError, OverflowError):
                start_text = None
        if identity_key_value and identity_key_value.startswith("subscription:"):
            scope_sql = "identity_key=?"
            scope_value = identity_key_value
        elif account_hash:
            scope_sql = "account_id_hash=?"
            scope_value = account_hash
        else:
            scope_sql = "identity_key=?"
            scope_value = identity_key_value or "unknown"
        bounds = [scope_value]
        time_sql = ""
        if start_text:
            time_sql += " AND ts>=?"
            bounds.append(start_text)
        if end_text:
            time_sql += " AND ts<=?"
            bounds.append(end_text)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(estimated_api_cost_usd), 0) observed_cost_usd,
                   COALESCE(SUM(call_count), 0) calls
              FROM usage_events
             WHERE {scope_sql} {time_sql}
            """,
            tuple(bounds),
        ).fetchone()
        observed = float(row["observed_cost_usd"] or 0.0)
        used = _percent_value(window.get("used_percent"))
        projected: float | None = None
        method = "observed_floor_only"
        confidence = "low"
        if observed > 0 and used is not None and used >= QUOTA_ESTIMATE_MIN_USED_PERCENT:
            fraction = max(used / 100.0, 0.01)
            projected = observed / fraction
            if used >= QUOTA_ESTIMATE_CAP_CONFIDENCE_PERCENT:
                # Only a provider-reported 100% is a hard cap.  At 95% used,
                # the percentage projection remains useful (5% is remaining).
                projected = observed
                method = "current_window_near_cap"
                confidence = "high"
            else:
                method = "current_window_percent_projection"
                confidence = (
                    "high"
                    if used >= 95.0
                    else ("medium" if used >= QUOTA_ESTIMATE_STABLE_USED_PERCENT else "initial")
                )
        if projected is None:
            previous = self._previous_provider_window_estimate(conn, entry, window)
            if previous is not None:
                projected, method, confidence = previous
        return {
            "current_window_observed_usd": observed,
            "current_window_calls": int(row["calls"] or 0),
            "quota_used_percent": used,
            "quota_estimate_method": method,
            "quota_estimate_confidence": confidence,
            "current_window_full_quota_usd": projected,
            "current_cycle_floor_usd": observed if observed > 0 else None,
        }

    @staticmethod
    def _previous_provider_window_estimate(
        conn: sqlite3.Connection,
        entry: Mapping[str, Any],
        current_window: Mapping[str, Any],
    ) -> tuple[float, str, str] | None:
        """Use a measured prior provider window as a conservative prior.

        This is intentionally separate from the current-window percentage
        projection.  A prior window can be used only when its own snapshot
        reached at least 50% and local event cost covers that snapshot.  The
        highest-usage prior observation wins; at 100% we use observed spend
        directly, otherwise we project from that prior percentage.  Missing
        prior events (for example, a collector started after the reset) do
        not create a fabricated quota.
        """

        current_reset_text = normalize_optional_timestamp(current_window.get("reset_at"))
        current_reset: datetime | None = None
        if current_reset_text:
            try:
                current_reset = datetime.fromisoformat(current_reset_text.replace("Z", "+00:00"))
            except ValueError:
                current_reset = None
        window_seconds = as_nonnegative_int(current_window.get("window_seconds"))
        if not window_seconds:
            return None
        account_hash = safe_text(entry.get("account_id_hash"), 64)
        identity_key_value = safe_text(entry.get("identity_key"), 300)
        if identity_key_value and identity_key_value.startswith("subscription:"):
            scope_sql = "identity_key=?"
            scope_value = identity_key_value
        elif account_hash:
            scope_sql = "account_id_hash=?"
            scope_value = account_hash
        else:
            scope_sql = "identity_key=?"
            scope_value = identity_key_value or "unknown"
        rows = conn.execute(
            f"""
            SELECT reset_at, fetched_at, used_percent,
                   estimated_full_quota_usd
              FROM subscription_quota_snapshots
             WHERE {scope_sql}
               AND window_kind=?
               AND reset_at IS NOT NULL
             ORDER BY fetched_at DESC, id DESC
            """,
            (scope_value, current_window.get("window_kind")),
        ).fetchall()
        best: tuple[float, float, str, str, str] | None = None
        seen_resets: set[str] = set()
        for snapshot in rows:
            reset_text = normalize_optional_timestamp(snapshot["reset_at"])
            if not reset_text or reset_text in seen_resets or reset_text == current_reset_text:
                continue
            seen_resets.add(reset_text)
            try:
                reset_at = datetime.fromisoformat(reset_text.replace("Z", "+00:00"))
            except ValueError:
                continue
            if current_reset is not None and reset_at >= current_reset:
                continue
            used = _percent_value(snapshot["used_percent"])
            if used is None or used < 50.0:
                continue
            fetched_at = normalize_timestamp(snapshot["fetched_at"])
            start_at = reset_at - timedelta(seconds=window_seconds)
            persisted_full = snapshot["estimated_full_quota_usd"]
            observed: float | None = None
            if persisted_full is not None:
                try:
                    persisted_value = float(persisted_full)
                    if math.isfinite(persisted_value) and persisted_value > 0:
                        observed = persisted_value
                except (TypeError, ValueError, OverflowError):
                    observed = None
            if observed is None:
                event_row = conn.execute(
                    f"""
                    SELECT SUM(estimated_api_cost_usd) observed_cost_usd
                      FROM usage_events
                     WHERE {scope_sql}
                       AND ts>=? AND ts<=? AND estimated_api_cost_usd IS NOT NULL
                    """,
                    (scope_value, start_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                     min(datetime.fromisoformat(fetched_at.replace("Z", "+00:00")), reset_at)
                     .isoformat(timespec="microseconds").replace("+00:00", "Z")),
                ).fetchone()
                if event_row["observed_cost_usd"] is not None:
                    observed = float(event_row["observed_cost_usd"])
            if observed is None or not math.isfinite(observed) or observed <= 0:
                continue
            estimate = observed if used >= QUOTA_ESTIMATE_CAP_CONFIDENCE_PERCENT else observed / (used / 100.0)
            confidence = "high" if used >= QUOTA_ESTIMATE_CAP_CONFIDENCE_PERCENT else "medium"
            candidate = (used, estimate, confidence, reset_text, fetched_at)
            if best is None or (candidate[0], candidate[4]) > (best[0], best[4]):
                best = candidate
        if best is None:
            return None
        return best[1], "previous_window_transfer", best[2]

    def grouped(self, period: str, dimension: str) -> list[dict[str, Any]]:
        start = period_start(period)
        dimensions = {
            "account": (
                "identity_key",
                "identity_key, MAX(usage_alias) usage_alias, MAX(account_id_tail) account_id_tail, "
                "MAX(account_id_hash) account_id_hash, MAX(auth_fingerprint) auth_fingerprint",
            ),
            "model": ("COALESCE(model, '(unknown)')", "COALESCE(model, '(unknown)') model"),
            "session": ("COALESCE(session_id, '(unknown)')", "COALESCE(session_id, '(unknown)') session_id"),
            "date": ("date(ts, 'localtime')", "date(ts, 'localtime') date"),
        }
        if dimension not in dimensions:
            raise ValueError(f"unknown dimension: {dimension}")
        group_expression, select_expression = dimensions[dimension]
        source_table = (
            "usage_statistics" if dimension in {"model", "date"} else "usage_events"
        )
        if source_table == "usage_statistics":
            non_cached_input_expression = "non_cached_input_token_count"
            codex_status_expression = "codex_status_token_count"
            account_attempt_expression = "account_attempt_count"
        else:
            non_cached_input_expression = (
                "MAX(COALESCE(input_tokens, 0) - COALESCE(cached_tokens, 0), 0)"
            )
            codex_status_expression = (
                "MAX(COALESCE(input_tokens, 0) - COALESCE(cached_tokens, 0), 0) "
                "+ COALESCE(output_tokens, 0)"
            )
            account_attempt_expression = (
                "CASE WHEN COALESCE(account_attempt, 1)=1 THEN call_count ELSE 0 END"
            )
        extra_filter = ""
        if dimension == "account":
            extra_filter = """
              AND identity_key IS NOT NULL AND identity_key!='unknown'
              AND NOT EXISTS (
                    SELECT 1 FROM active_subscription_registry registry
                     WHERE registry.identity_key=usage_events.identity_key
                       AND registry.state='suspect_missing'
                  )
              AND NOT EXISTS (
                    SELECT 1 FROM retired_subscription_tombstones tombstone
                     WHERE tombstone.identity_key=usage_events.identity_key
                  )
            """
        elif dimension == "model":
            # A request rejected by the local gateway before account/model
            # selection remains failure history, not model consumption.
            extra_filter = "AND account_attempt_count>0"
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {select_expression},
                  COALESCE(SUM(call_count), 0) calls,
                  COALESCE(SUM({account_attempt_expression}), 0) account_attempts,
                  COUNT(DISTINCT CASE WHEN {account_attempt_expression}>0
                                      THEN request_id END)
                    + COALESCE(SUM(CASE WHEN {account_attempt_expression}>0
                                         AND request_id IS NULL
                                        THEN {account_attempt_expression} ELSE 0 END), 0)
                    logical_requests,
                  COALESCE(SUM(CASE WHEN ok=1 THEN call_count ELSE 0 END), 0) successful_calls,
                  COALESCE(SUM(CASE WHEN ok=0 THEN call_count ELSE 0 END), 0) failed_calls,
                  COALESCE(SUM(input_tokens), 0) input_tokens,
                  COALESCE(SUM(cached_tokens), 0) cached_tokens,
                  COALESCE(SUM(output_tokens), 0) output_tokens,
                  COALESCE(SUM(reasoning_tokens), 0) reasoning_tokens,
                  COALESCE(SUM(total_tokens), 0) total_tokens,
                  COALESCE(SUM(CASE WHEN long_context_pricing_applied=1 THEN call_count ELSE 0 END), 0)
                    long_context_priced_calls,
                  COALESCE(SUM({non_cached_input_expression}), 0)
                    non_cached_input_tokens,
                  COALESCE(SUM({codex_status_expression}), 0)
                    codex_status_tokens,
                  COALESCE(SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)), 0)
                    api_processed_tokens,
                  SUM(estimated_api_cost_usd) estimated_api_cost_usd,
                  SUM(non_cached_input_cost_usd) non_cached_input_cost_usd,
                  SUM(cached_input_cost_usd) cached_input_cost_usd,
                  SUM(output_cost_usd) output_cost_usd,
                  COALESCE(SUM(CASE WHEN usage_missing=1 THEN call_count ELSE 0 END), 0) usage_missing_calls
                FROM {source_table} WHERE ts>=? {extra_filter}
                GROUP BY {group_expression}
                ORDER BY calls DESC, total_tokens DESC
                """,
                (start,),
            ).fetchall()
        result = [dict(row) for row in rows]
        for row in result:
            input_tokens = int(row.get("input_tokens") or 0)
            cached_tokens = int(row.get("cached_tokens") or 0)
            row["cache_hit_rate_percent"] = (
                cached_tokens / input_tokens * 100.0 if input_tokens else None
            )
            row["retry_attempts"] = max(
                int(row.get("account_attempts") or 0) - int(row.get("logical_requests") or 0), 0
            )
        return result

    def recent(self, count: int) -> list[dict[str, Any]]:
        """Return recent persisted calls, including pre-account API rejects."""

        return self._recent(count, account_attempts_only=False)

    def recent_account_attempts(self, count: int) -> list[dict[str, Any]]:
        """Return recent calls that reached subscription account selection."""

        return self._recent(count, account_attempts_only=True)

    def _recent(
        self,
        count: int,
        *,
        account_attempts_only: bool,
    ) -> list[dict[str, Any]]:
        count = max(1, min(int(count), 500))
        attempt_filter = (
            "AND COALESCE(account_attempt, 1)=1" if account_attempts_only else ""
        )
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ts, identity_key, usage_alias, account_id_tail, account_id_hash,
                  auth_fingerprint, session_id, model, endpoint, method,
                  status_code, ok, duration_ms, stream, input_tokens,
                  cached_tokens, cache_write_tokens, output_tokens, reasoning_tokens, total_tokens,
                  estimated_api_cost_usd, non_cached_input_cost_usd,
                  cached_input_cost_usd, output_cost_usd, long_context_pricing_applied,
                  usage_missing, error_type,
                  error_message_redacted, source, request_id, account_attempt
                FROM usage_events
                WHERE NOT EXISTS (
                      SELECT 1 FROM active_subscription_registry registry
                       WHERE registry.identity_key=usage_events.identity_key
                         AND registry.state='suspect_missing'
                    )
                  AND NOT EXISTS (
                      SELECT 1 FROM retired_subscription_tombstones tombstone
                       WHERE tombstone.identity_key=usage_events.identity_key
                    )
                  {attempt_filter}
                ORDER BY ts DESC, id DESC LIMIT ?
                """,
                (count,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _percentile(values: Sequence[float], fraction: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        weight = position - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    def quota_summary(self, period: str = "30d") -> list[dict[str, Any]]:
        start = period_start(period)
        with self.connect() as conn:
            identities = conn.execute(
                """
                SELECT identity_key, MAX(usage_alias) usage_alias,
                       MAX(account_id_tail) account_id_tail,
                       MAX(account_id_hash) account_id_hash,
                       MAX(auth_fingerprint) auth_fingerprint
                FROM (
                  SELECT identity_key, usage_alias, account_id_tail, account_id_hash, auth_fingerprint, ts
                    FROM usage_events WHERE identity_key IS NOT NULL
                  UNION ALL
                  SELECT identity_key, usage_alias, account_id_tail, account_id_hash, NULL, ts
                    FROM quota_events WHERE identity_key IS NOT NULL
                  UNION ALL
                  SELECT identity_key, usage_alias, account_id_tail, account_id_hash, NULL, cycle_end_ts AS ts
                    FROM account_quota_cycles WHERE identity_key IS NOT NULL
                )
                WHERE identity_key NOT IN (
                      SELECT identity_key FROM active_subscription_registry
                       WHERE state='suspect_missing'
                    )
                  AND identity_key NOT IN (
                      SELECT identity_key FROM retired_subscription_tombstones
                    )
                GROUP BY identity_key ORDER BY MAX(ts) DESC
                """
            ).fetchall()
            results: list[dict[str, Any]] = []
            for identity in identities:
                key = identity["identity_key"]
                reset_ts = self._latest_event_ts(conn, key, RESET_EVENT_TYPES)
                if reset_ts is None:
                    first = conn.execute(
                        "SELECT MIN(ts) first_ts FROM usage_events WHERE identity_key=? AND ok=1", (key,)
                    ).fetchone()
                    if not first or first["first_ts"] is None:
                        first = conn.execute(
                            "SELECT MIN(ts) first_ts FROM usage_events WHERE identity_key=?", (key,)
                        ).fetchone()
                    reset_ts = first["first_ts"] if first else None
                hit_ts = self._latest_event_ts(conn, key, QUOTA_EVENT_TYPES)
                current = conn.execute(
                    """
                    SELECT COALESCE(SUM(call_count), 0) calls,
                           COALESCE(SUM(total_tokens), 0) total_tokens,
                           SUM(estimated_api_cost_usd) observed_floor_usd
                    FROM usage_events
                    WHERE identity_key=? AND ts>=?
                      AND (? IS NULL OR ts<=?)
                    """,
                    (
                        key,
                        reset_ts or "",
                        hit_ts if hit_ts and (not reset_ts or hit_ts >= reset_ts) else None,
                        hit_ts if hit_ts and (not reset_ts or hit_ts >= reset_ts) else None,
                    ),
                ).fetchone()
                if str(period).strip().lower() in ALL_TIME_PERIODS:
                    complete = conn.execute(
                        """SELECT * FROM account_quota_cycles
                           WHERE identity_key=? AND is_complete_cycle=1
                           ORDER BY cycle_end_ts DESC, id DESC""",
                        (key,),
                    ).fetchall()
                else:
                    complete = conn.execute(
                        """SELECT * FROM account_quota_cycles
                           WHERE identity_key=? AND is_complete_cycle=1 AND cycle_end_ts>=?
                           ORDER BY cycle_end_ts DESC, id DESC""",
                        (key, start),
                    ).fetchall()
                values = [
                    float(row["api_equivalent_quota_usd"])
                    for row in complete
                    if row["api_equivalent_quota_usd"] is not None
                ]
                results.append(
                    {
                        "identity_key": key,
                        "usage_alias": identity["usage_alias"],
                        "account_id_tail": identity["account_id_tail"],
                        "account_id_hash": identity["account_id_hash"],
                        "auth_fingerprint": identity["auth_fingerprint"],
                        "current_cycle_start_ts": reset_ts,
                        "current_cycle_calls": current["calls"],
                        "current_cycle_tokens": current["total_tokens"],
                        "current_cycle_observed_floor_usd": current["observed_floor_usd"],
                        "currently_quota_hit": int(bool(hit_ts and (not reset_ts or hit_ts >= reset_ts))),
                        "complete_cycles_in_period": len(complete),
                        "last_complete_cycle_api_equivalent_quota_usd": (
                            complete[0]["api_equivalent_quota_usd"] if complete else None
                        ),
                        "historical_min_usd": min(values) if values else None,
                        "historical_p20_usd": self._percentile(values, 0.20),
                        "historical_p50_usd": self._percentile(values, 0.50),
                        "historical_p80_usd": self._percentile(values, 0.80),
                        "historical_max_usd": max(values) if values else None,
                    }
                )
        return results


class OfficialPriceSyncError(RuntimeError):
    """A safe, user-facing official pricing fetch/parse failure."""


class _PricingHTMLParser(HTMLParser):
    """Extract rows from one server-rendered OpenAI pricing table.

    The public page is server-rendered and has changed its surrounding Astro
    component markup a few times.  This parser intentionally depends only on
    ordinary ``table``/``tr``/``td`` elements, not on generated CSS classes or
    JavaScript bundles.  The caller selects the standard-tier table fragment.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._table_depth = 0
        self._table_done = False
        self._cell_tag: str | None = None
        self._cell_parts: list[str] = []
        self._row: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table" and not self._table_done:
            self._table_depth += 1
            return
        if self._table_depth == 0 or self._table_done:
            return
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_tag = tag
            self._cell_parts = []
        elif tag == "br" and self._cell_tag is not None:
            self._cell_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._table_depth == 0 or self._table_done:
            return
        if tag in {"td", "th"} and self._cell_tag is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell_parts).split()))
            self._cell_tag = None
            self._cell_parts = []
        elif tag == "tr":
            if self._row:
                self.rows.append(self._row)
            self._row = None
        elif tag == "table":
            self._table_depth -= 1
            if self._table_depth == 0:
                self._table_done = True

    def handle_data(self, data: str) -> None:
        if self._cell_tag is not None:
            self._cell_parts.append(data)


def _pricing_number(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip().replace(",", "")
    if text.lower() in {"-", "—", "–", "n/a", "na", "none", "null"}:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group(0))
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _normalize_official_model(value: str | None) -> str | None:
    if not value:
        return None
    text = " ".join(value.split())
    # The standard table annotates some rows with a context-length parenthesis;
    # the model id itself is the stable prefix used in API requests.
    text = re.sub(r"\s*\([^)]*context(?:\s+length)?[^)]*\)", "", text, flags=re.IGNORECASE)
    text = text.strip()
    if not text or text.lower() in {"model", "models"}:
        return None
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*", text):
        return None
    return text


def _parse_pricing_props_rows(fragment: str) -> list[dict[str, Any]]:
    """Read the complete model list from the page's SSR pricing props.

    The rendered table intentionally shows only a few rows until the browser
    expands it, while the Astro props retain the complete standard-tier list.
    This small, deliberately constrained matcher handles the tagged scalar
    representation used by the page and falls back to rendered rows when the
    representation changes.
    """

    row_pattern = re.compile(
        r'\[1,\[\[0,"((?:\\.|[^"\\])*)"\],'
        r'((?:\[0,(?:"(?:\\.|[^"\\])*"|null|-?\d+(?:\.\d+)?)\],?)+)\]\]',
        re.DOTALL,
    )
    scalar_pattern = re.compile(r'\[0,("(?:\\.|[^"\\])*"|null|-?\d+(?:\.\d+)?)\]')
    parsed: dict[str, dict[str, Any]] = {}
    for match in row_pattern.finditer(fragment):
        raw_model = match.group(1)
        try:
            model = json.loads(f'"{raw_model}"')
        except (json.JSONDecodeError, TypeError):
            continue
        model = _normalize_official_model(model)
        if not model:
            continue
        values: list[Any] = []
        for raw in scalar_pattern.findall(match.group(2)):
            if raw == "null":
                values.append(None)
            elif raw.startswith('"'):
                try:
                    values.append(json.loads(raw))
                except json.JSONDecodeError:
                    values.append(None)
            else:
                try:
                    values.append(float(raw))
                except ValueError:
                    values.append(None)
        if len(values) < 3:
            continue
        input_rate = _pricing_number(str(values[0]) if values[0] is not None else None)
        cached_rate = _pricing_number(str(values[1]) if values[1] is not None else None)
        cache_write_rate = (
            _pricing_number(str(values[2]) if values[2] is not None else None)
            if len(values) >= 4
            else None
        )
        output_index = 3 if len(values) >= 4 else 2
        output_rate = _pricing_number(str(values[output_index]) if values[output_index] is not None else None)
        if input_rate is None and output_rate is None:
            continue
        parsed.setdefault(
            model,
            {
                "model_pattern": model,
                "input_per_million": input_rate,
                "output_per_million": output_rate,
                "cached_input_per_million": cached_rate,
                "cache_write_per_million": cache_write_rate,
            },
        )
    return list(parsed.values())


def _complete_context_tiers(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Derive documented Standard long tiers when the page omits props fields.

    GPT-5.4/5.5 1.05M models and the GPT-5.6 family charge 2x input and
    1.5x output above 272K input tokens. GPT-6 Astra uses the same rates at
    every context length. Cache writes remain 1.25x input for GPT-5.6/Astra.
    Apply these rules only to the exact model IDs.
    """

    eligible = {
        "gpt-5.4",
        "gpt-5.4-pro",
        "gpt-5.5",
        "gpt-5.5-pro",
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "gpt-6-astra",
    }
    completed: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        model = row.get("model_pattern")
        if model not in eligible:
            completed.append(row)
            continue
        input_rate = row.get("input_per_million")
        cached_rate = row.get("cached_input_per_million")
        output_rate = row.get("output_per_million")
        cache_write_rate = row.get("cache_write_per_million")
        if (
            cache_write_rate is None
            and input_rate is not None
            and (str(model).startswith("gpt-5.6-") or model == "gpt-6-astra")
        ):
            cache_write_rate = float(input_rate) * 1.25
            row["cache_write_per_million"] = cache_write_rate
        if model == "gpt-6-astra":
            for field in (
                "long_context_threshold_tokens", "long_input_per_million",
                "long_cached_input_per_million", "long_cache_write_per_million",
                "long_output_per_million",
            ):
                row[field] = None
            completed.append(row)
            continue
        row["long_context_threshold_tokens"] = LONG_CONTEXT_THRESHOLD_TOKENS
        row["long_input_per_million"] = (
            float(input_rate) * 2.0 if input_rate is not None else None
        )
        row["long_cached_input_per_million"] = (
            float(cached_rate) * 2.0 if cached_rate is not None else None
        )
        row["long_cache_write_per_million"] = (
            float(cache_write_rate) * 2.0 if cache_write_rate is not None else None
        )
        row["long_output_per_million"] = (
            float(output_rate) * 1.5 if output_rate is not None else None
        )
        completed.append(row)
    return completed


def _parse_grouped_context_pricing_rows(fragment: str) -> list[dict[str, Any]]:
    """Read the Standard grouped short/long-context table from Astro props."""

    if "Short context input" not in fragment or "Long context input" not in fragment:
        return []
    heading_pos = fragment.find("Short context input")
    island_start = fragment.rfind("<astro-island", 0, heading_pos)
    # In raw HTML the heading text lives inside &quot; entities, so callers
    # that unescape first can move it before the literal opening tag boundary.
    # When that happens, take the first island containing the heading.
    if island_start < 0:
        island_start = fragment.find("<astro-island")
    island_end = fragment.find("</astro-island>", heading_pos)
    if island_start >= 0 and island_start < heading_pos and island_end > island_start:
        fragment = fragment[island_start : island_end + len("</astro-island>")]
    group_pattern = re.compile(
        r'"model":\[0,"((?:\\.|[^"\\])*)"\].*?'
        r'"rows":\[1,\[\[1,\[(.*?)\]\]\]\]',
        re.DOTALL,
    )
    scalar_pattern = re.compile(r'\[0,("(?:\\.|[^"\\])*"|null|-?\d+(?:\.\d+)?)\]')
    parsed: dict[str, dict[str, Any]] = {}
    for match in group_pattern.finditer(fragment):
        try:
            model = _normalize_official_model(json.loads(f'"{match.group(1)}"'))
        except (json.JSONDecodeError, TypeError):
            continue
        if not model:
            continue
        values: list[Any] = []
        values_fragment = match.group(2)
        if values_fragment and not values_fragment.endswith("]"):
            values_fragment += "]"
        for raw in scalar_pattern.findall(values_fragment):
            if raw == "null":
                values.append(None)
            elif raw.startswith('"'):
                try:
                    values.append(json.loads(raw))
                except json.JSONDecodeError:
                    values.append(None)
            else:
                try:
                    values.append(float(raw))
                except ValueError:
                    values.append(None)
        if len(values) < 8:
            continue
        rates = [_pricing_number(str(value) if value is not None else None) for value in values[:8]]
        if rates[0] is None and rates[3] is None:
            continue
        has_long_tier = any(value is not None for value in rates[4:8])
        parsed[model] = {
            "model_pattern": model,
            "input_per_million": rates[0],
            "cached_input_per_million": rates[1],
            "cache_write_per_million": rates[2],
            "output_per_million": rates[3],
            "long_context_threshold_tokens": (
                LONG_CONTEXT_THRESHOLD_TOKENS if has_long_tier else None
            ),
            "long_input_per_million": rates[4],
            "long_cached_input_per_million": rates[5],
            "long_cache_write_per_million": rates[6],
            "long_output_per_million": rates[7],
        }
    return list(parsed.values())


def parse_official_pricing_html(document: str | bytes) -> list[dict[str, Any]]:
    """Parse Standard short- and long-context prices from the official page.

    Returned prices are USD per million tokens.  The sidecar does not infer a
    price for a model absent from this table.  Long-context pricing is enabled
    only when the official table provides a complete tier for that model.
    """

    if isinstance(document, bytes):
        try:
            text = document.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OfficialPriceSyncError("pricing page is not UTF-8 HTML") from exc
    else:
        text = document
    if not text or len(text) > 20 * 1024 * 1024:
        raise OfficialPriceSyncError("pricing page is empty or too large")

    standard_marker = 'data-content-switcher-pane="true" data-value="standard"'
    standard_pos = text.find(standard_marker)
    # The current page renders the compact TextTokenPricingTables and the
    # complete grouped context table as separate islands.  Locate the unique
    # grouped table by its explicit headings instead of assuming adjacency.
    grouped_rows = _parse_grouped_context_pricing_rows(html.unescape(text))

    marker = 'component-export="TextTokenPricingTables"'
    marker_pos = text.find(marker, max(0, standard_pos)) if standard_pos >= 0 else -1
    if marker_pos < 0:
        marker_pos = text.find(marker)
    if marker_pos >= 0:
        start = text.rfind("<astro-island", 0, marker_pos)
        end = text.find("</astro-island>", marker_pos)
        fragment = text[start : end + len("</astro-island>")] if start >= 0 and end >= 0 else text
    else:
        fragment = text

    unescaped_fragment = html.unescape(fragment)
    props_rows = _parse_pricing_props_rows(unescaped_fragment)
    if grouped_rows:
        by_model = {row["model_pattern"]: row for row in props_rows}
        for grouped_row in grouped_rows:
            model = grouped_row["model_pattern"]
            by_model[model] = grouped_row
        return _complete_context_tiers(list(by_model.values()))
    if props_rows:
        return _complete_context_tiers(props_rows)

    parser = _PricingHTMLParser()
    try:
        parser.feed(fragment)
        parser.close()
    except Exception as exc:  # HTMLParser should be forgiving; make failure explicit.
        raise OfficialPriceSyncError("pricing HTML parser failed") from exc
    rows = parser.rows
    if not rows:
        raise OfficialPriceSyncError("standard pricing table not found")

    header: list[str] | None = None
    for row in rows:
        lowered = [cell.strip().lower() for cell in row]
        if lowered and lowered[0] == "model" and any("output" in cell for cell in lowered):
            header = lowered
            break
    if header is None:
        # A compact/future page may omit explicit headers in the fragment; the
        # current standard table's first data layout remains model,input,cached,
        # cache-writes,output (or model,input,cached,output).
        header = ["model", "input", "cached input", "cache writes", "output"]

    try:
        model_index = header.index("model")
    except ValueError:
        model_index = 0
    input_indexes = [index for index, value in enumerate(header) if value == "input"]
    cached_indexes = [index for index, value in enumerate(header) if value.startswith("cached input")]
    output_indexes = [index for index, value in enumerate(header) if value == "output"]
    input_index = input_indexes[0] if input_indexes else 1
    cached_index = cached_indexes[0] if cached_indexes else 2
    output_index = output_indexes[0] if output_indexes else (4 if len(header) >= 5 else 3)

    parsed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row is header:
            continue
        if model_index >= len(row):
            continue
        model = _normalize_official_model(row[model_index])
        if not model:
            continue
        if max(input_index, cached_index, output_index) >= len(row):
            continue
        input_rate = _pricing_number(row[input_index])
        output_rate = _pricing_number(row[output_index])
        cached_rate = _pricing_number(row[cached_index])
        # A row with no numeric input/output is a malformed/header row, not a
        # usable official price.  Retain a legitimate '-' output as NULL only
        # when the input side is present (e.g. pro variants).
        if input_rate is None and output_rate is None:
            continue
        parsed.setdefault(
            model,
            {
                "model_pattern": model,
                "input_per_million": input_rate,
                "output_per_million": output_rate,
                "cached_input_per_million": cached_rate,
            },
        )
    if not parsed:
        raise OfficialPriceSyncError("standard pricing table contained no usable model rows")
    return _complete_context_tiers(list(parsed.values()))


def _validate_official_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname not in OFFICIAL_PRICING_HOSTS:
        raise OfficialPriceSyncError("pricing URL must be an HTTPS official OpenAI documentation URL")
    if parsed.username or parsed.password:
        raise OfficialPriceSyncError("pricing URL must not contain credentials")
    return url


def fetch_official_pricing_html(url: str = OFFICIAL_PRICING_URL, timeout: float = 20.0) -> tuple[str, bytes, str]:
    """Fetch an official pricing page with bounded size and host validation."""

    _validate_official_url(url)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": "codex-usage-observatory-official-price-sync/1",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=max(float(timeout), 1.0)) as response:
            final_url = response.geturl() or url
            _validate_official_url(final_url)
            body = response.read(20 * 1024 * 1024 + 1)
    except OfficialPriceSyncError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise OfficialPriceSyncError(f"official pricing fetch error: {type(exc).__name__}") from exc
    if len(body) > 20 * 1024 * 1024:
        raise OfficialPriceSyncError("official pricing page exceeds size limit")
    return final_url, body, hashlib.sha256(body).hexdigest()


def sync_official_prices(
    repo: UsageRepository,
    *,
    url: str = OFFICIAL_PRICING_URL,
    timeout: float = 20.0,
) -> dict[str, Any]:
    """Fetch, parse, and atomically install official prices.

    Any fetch/parse failure records redacted metadata and leaves the previous
    model price rows untouched.  This function is deliberately explicit; the
    long-running proxy never performs network I/O at startup.
    """

    requested_at = utc_now()
    content_sha256: str | None = None
    final_url = url
    try:
        final_url, body, content_sha256 = fetch_official_pricing_html(url, timeout)
        rows = parse_official_pricing_html(body)
        repriced_events = repo.replace_official_prices(
            rows,
            source_url=final_url,
            fetched_at=requested_at,
            content_sha256=content_sha256,
            parser_version=OFFICIAL_PRICE_PARSER_VERSION,
        )
        return {
            "status": "ok",
            "source_url": final_url,
            "fetched_at": requested_at,
            "content_sha256": content_sha256,
            "parser_version": OFFICIAL_PRICE_PARSER_VERSION,
            "model_count": len(rows),
            "repriced_events": repriced_events,
        }
    except Exception as exc:
        try:
            repo.record_price_sync(
                source_url=final_url,
                fetched_at=requested_at,
                content_sha256=content_sha256,
                parser_version=OFFICIAL_PRICE_PARSER_VERSION,
                status="error",
                model_count=0,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        except Exception:
            # Preserve the original sync failure; database diagnostics must not
            # leak or mask it.
            pass
        if isinstance(exc, OfficialPriceSyncError):
            raise
        raise OfficialPriceSyncError(f"official pricing sync error: {type(exc).__name__}") from exc


def detect_quota_event(status_code: int, error_type: str | None, message: str | None) -> str | None:
    combined = f"{error_type or ''} {message or ''}".lower()
    if "auth_unavailable" in combined and not any(
        word in combined for word in ("quota", "usage limit", "cooldown", "rate limit", "limit reached")
    ):
        return None
    if "cooldown" in combined:
        return "cooldown_hit"
    if "usage limit" in combined or "usage_limit" in combined or "limit reached" in combined or "额度" in combined:
        return "usage_limit_hit"
    if "insufficient_quota" in combined or "quota" in combined:
        return "quota_hit"
    if "rate limited" in combined or "rate-limit" in combined or "rate limit" in combined or status_code == 429:
        return "rate_limit_hit"
    return None


def _queue_headers_are_streaming(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    for key, raw in value.items():
        if str(key).lower() != "content-type":
            continue
        values = raw if isinstance(raw, list) else [raw]
        return any("text/event-stream" in str(item).lower() for item in values)
    return False


def queue_record_event(
    record: Mapping[str, Any],
    resolver: AccountResolver,
    repo: UsageRepository,
) -> tuple[UsageEvent, RequestInfo] | None:
    """Convert one CLIProxyAPI usage-queue record into the local safe schema.

    The upstream payload contains an ``api_key`` field.  It is intentionally
    never read, copied, logged, or passed to any identity resolver.
    """

    if not isinstance(record, Mapping):
        return None
    token_block = record.get("tokens")
    if not isinstance(token_block, Mapping):
        token_block = record.get("token_breakdown") if isinstance(record.get("token_breakdown"), Mapping) else {}
    usage = NormalizedUsage(
        input_tokens=as_nonnegative_int(token_block.get("input_tokens")),
        output_tokens=as_nonnegative_int(token_block.get("output_tokens")),
        cached_tokens=as_nonnegative_int(
            token_block.get("cached_tokens")
            if token_block.get("cached_tokens") is not None
            else token_block.get("cache_read_tokens")
        ),
        cache_write_tokens=as_nonnegative_int(token_block.get("cache_write_tokens")),
        reasoning_tokens=as_nonnegative_int(token_block.get("reasoning_tokens")),
        total_tokens=as_nonnegative_int(token_block.get("total_tokens")),
    )
    model = safe_text(record.get("model") or record.get("alias"), 200)
    endpoint = "/v1/usage"
    auth_index = safe_text(record.get("auth_index"), 512)
    digest_raw = safe_text(record.get("access_token_sha256"), 128)
    digest = digest_raw.lower() if digest_raw and re.fullmatch(r"[0-9a-f]{64}", digest_raw.lower()) else None
    identity = resolver.resolve_queue(auth_index, digest, safe_text(record.get("alias"), 200))
    usage_alias = identity.usage_alias
    key = (
        f"subscription:{identity.subscription_id_hash}"
        if identity.subscription_id_hash
        else "unknown"
    )
    info = RequestInfo(
        endpoint=endpoint,
        method="POST",
        model=model,
        stream=int(_queue_headers_are_streaming(record.get("response_headers"))),
        session_id=None,
        thread_id=None,
        turn_id=None,
        installation_id=None,
        window_id=None,
        usage_alias=usage_alias,
        usage_project=None,
        auth_fingerprint=None,
        account_id_hash=identity.account_id_hash,
        account_id_tail=identity.account_id_tail,
        identity_key=key,
    )
    fail_block = record.get("fail") if isinstance(record.get("fail"), Mapping) else {}
    failed = bool(record.get("failed"))
    status_value = as_nonnegative_int(fail_block.get("status_code"))
    status_code = status_value if status_value and status_value >= 100 else (500 if failed else 200)
    error_message = redact_text(fail_block.get("body")) if failed else None
    components = repo.price_components_for(model, usage)
    event = UsageEvent(
        ts=normalize_timestamp(record.get("timestamp")),
        identity_key=key,
        endpoint=endpoint,
        method="POST",
        model=model,
        status_code=status_code,
        ok=int(not failed and 200 <= status_code < 300),
        duration_ms=as_nonnegative_int(record.get("latency_ms")) or 0,
        stream=info.stream,
        session_id=None,
        thread_id=None,
        turn_id=None,
        installation_id=None,
        window_id=None,
        usage_alias=usage_alias,
        usage_project=None,
        auth_fingerprint=None,
        account_id_hash=identity.account_id_hash,
        account_id_tail=identity.account_id_tail,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_tokens=usage.cached_tokens,
        cache_write_tokens=usage.cache_write_tokens,
        reasoning_tokens=usage.reasoning_tokens,
        total_tokens=usage.total_tokens,
        estimated_api_cost_usd=components.total_cost_usd if components else None,
        non_cached_input_cost_usd=(
            components.non_cached_input_cost_usd if components else None
        ),
        cached_input_cost_usd=components.cached_input_cost_usd if components else None,
        output_cost_usd=components.output_cost_usd if components else None,
        long_context_pricing_applied=int(
            components.long_context_pricing_applied if components else False
        ),
        subscription_amortized_cost_usd=None,
        api_equivalent_quota_usd=None,
        usage_missing=int(usage.missing),
        error_type="upstream_error" if failed else None,
        error_message_redacted=error_message,
        request_bytes=0,
        response_bytes=0,
        source="usage_queue",
        request_id=None,
    )
    return event, info


def _percent_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not 0 <= number <= 100:
        return None
    return number


def quota_window_kind(window_seconds: Any, fallback: str) -> str:
    """Classify a Codex quota window by duration, not legacy field name."""

    seconds = as_nonnegative_int(window_seconds)
    if seconds == 18_000:
        return "five_hour"
    if seconds == 604_800:
        return "weekly"
    if seconds and 2_419_200 <= seconds <= 2_678_400:
        return "monthly"
    return fallback


def parse_codex_quota_windows(payload: Any, fetched_at: str | None = None) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    rate_limit = first_present(payload, ("rate_limit", "rateLimit"))
    if not isinstance(rate_limit, Mapping):
        return []
    provider_allowed = rate_limit.get("allowed")
    provider_limit_reached = first_present(
        rate_limit, ("limit_reached", "limitReached")
    )
    provider_allowed = provider_allowed if isinstance(provider_allowed, bool) else None
    provider_limit_reached = (
        provider_limit_reached
        if isinstance(provider_limit_reached, bool)
        else None
    )
    primary = first_present(rate_limit, ("primary_window", "primaryWindow"))
    secondary = first_present(rate_limit, ("secondary_window", "secondaryWindow"))
    candidates = [primary, secondary]
    windows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(candidates):
        if not isinstance(raw, Mapping):
            continue
        window_seconds = as_nonnegative_int(
            first_present(raw, ("limit_window_seconds", "limitWindowSeconds"))
        )
        kind = quota_window_kind(
            window_seconds,
            "five_hour" if index == 0 else "weekly",
        )
        if kind in seen:
            continue
        used = _percent_value(first_present(raw, ("used_percent", "usedPercent")))
        if used is None:
            limit_reached = first_present(rate_limit, ("limit_reached", "limitReached"))
            allowed = rate_limit.get("allowed")
            if limit_reached is True or allowed is False:
                used = 100.0
        reset_value = first_present(raw, ("reset_at", "resetAt"))
        if reset_value is None:
            reset_after = as_nonnegative_int(
                first_present(raw, ("reset_after_seconds", "resetAfterSeconds"))
            )
            if reset_after is not None:
                reset_value = datetime.now(timezone.utc).timestamp() + reset_after
        windows.append(
            {
                "fetched_at": fetched_at or utc_now(),
                "window_kind": kind,
                "used_percent": used,
                "remaining_percent": 100.0 - used if used is not None else None,
                "window_seconds": window_seconds,
                "reset_at": normalize_optional_timestamp(reset_value),
                "provider_allowed": provider_allowed,
                "provider_limit_reached": provider_limit_reached,
            }
        )
        seen.add(kind)
    return windows


def parse_codex_app_rate_windows(
    rate_limits: Any,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize the non-secret rate-limit block written to Codex JSONL."""

    if not isinstance(rate_limits, Mapping):
        return []
    windows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, name in enumerate(("primary", "secondary")):
        raw = rate_limits.get(name)
        if not isinstance(raw, Mapping):
            continue
        minutes = as_nonnegative_int(
            first_present(raw, ("window_minutes", "windowMinutes"))
        )
        seconds = minutes * 60 if minutes is not None else None
        kind = quota_window_kind(
            seconds,
            "five_hour" if index == 0 else "weekly",
        )
        if kind in seen:
            continue
        used = _percent_value(first_present(raw, ("used_percent", "usedPercent")))
        windows.append(
            {
                "fetched_at": fetched_at or utc_now(),
                "window_kind": kind,
                "used_percent": used,
                "remaining_percent": 100.0 - used if used is not None else None,
                "window_seconds": seconds,
                "reset_at": normalize_optional_timestamp(
                    first_present(raw, ("resets_at", "resetsAt", "reset_at", "resetAt"))
                ),
            }
        )
        seen.add(kind)
    return windows


class CockpitToolsImporter:
    """Read Cockpit Tools request and quota statistics without modifying it.

    Compatibility is capability-based: Cockpit application/index version
    numbers are not gates, and additive request-log columns are ignored.
    """

    REQUIRED_REQUEST_COLUMNS = (
        "id",
        "event_key",
        "timestamp",
        "account_id",
        "model_id",
        "success",
        "http_status",
        "latency_ms",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_tokens",
        "reasoning_tokens",
        "token_breakdown_json",
        "estimated_cost_usd",
        "model_pricing_version",
        "input_usd_per_million",
        "output_usd_per_million",
        "cached_input_usd_per_million",
    )

    def __init__(
        self,
        repo: UsageRepository,
        resolver: AccountResolver,
        data_dir: Path | str | None = None,
        localstorage_db: Path | str | None = None,
        poll_seconds: float = DEFAULT_COCKPIT_TOOLS_POLL_SECONDS,
    ) -> None:
        self.repo = repo
        self.resolver = resolver
        if data_dir is not None or localstorage_db is not None:
            resolver.configure_cockpit_sources(
                data_dir=data_dir,
                localstorage_db=localstorage_db,
            )
        self.data_dir = (
            Path(data_dir).expanduser()
            if data_dir is not None
            else resolver.cockpit_tools_data_dir
        )
        self.database_path = self.data_dir / COCKPIT_TOOLS_LOG_DB_NAME
        self.poll_seconds = max(1.0, float(poll_seconds))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_poll_at: str | None = None
        self._last_success_at: str | None = None
        self._last_error_type: str | None = None
        self._last_imported = 0
        self._last_scanned = 0
        self._last_quota_rows = 0
        self._database_available = False
        self._identity_migration_marker: str | None = None
        self._inventory_result: dict[str, Any] = {
            "authoritative": False,
            "active": 0,
            "suspect": 0,
            "retired": 0,
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="cockpit-tools-import",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._thread = None

    def status(self) -> dict[str, Any]:
        persisted = self.repo.import_status(COCKPIT_TOOLS_REQUEST_SOURCE)
        inventory = {
            key: value
            for key, value in self._inventory_result.items()
            if key != "retired_keys"
        }
        return {
            "enabled": True,
            "authoritative_accounts": bool(
                self.resolver.cockpit_tools_authoritative_accounts
            ),
            "database_available": self._database_available,
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error_type": self._last_error_type,
            "last_imported": self._last_imported,
            "last_scanned": self._last_scanned,
            "last_quota_rows": self._last_quota_rows,
            "poll_seconds": self.poll_seconds,
            "inventory": inventory,
            **persisted,
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                result = self.import_once()
                self._last_poll_at = utc_now()
                self._last_success_at = self._last_poll_at
                self._last_error_type = None
                self._last_imported = result["imported"]
                self._last_scanned = result["scanned"]
                self._last_quota_rows = result["quota_rows"]
                self._database_available = bool(result["database_available"])
            except Exception as exc:
                self._last_poll_at = utc_now()
                self._last_error_type = type(exc).__name__
                self._database_available = self.database_path.is_file()
                LOG.warning("Cockpit Tools import failed: %s", type(exc).__name__)
            self._stop.wait(self.poll_seconds)

    @staticmethod
    def _safe_rate(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return (
            result
            if math.isfinite(result) and 0 <= result <= 1_000_000
            else None
        )

    @staticmethod
    def _breakdown(value: Any) -> Mapping[str, Any]:
        if not isinstance(value, str) or not value.strip():
            return {}
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
        return parsed if isinstance(parsed, Mapping) else {}

    @classmethod
    def _usage_and_components(
        cls,
        row: Mapping[str, Any],
    ) -> tuple[NormalizedUsage, PriceComponents | None]:
        input_tokens = as_nonnegative_int(row.get("input_tokens")) or 0
        output_tokens = as_nonnegative_int(row.get("output_tokens")) or 0
        total_tokens = as_nonnegative_int(row.get("total_tokens"))
        cached_tokens = as_nonnegative_int(row.get("cached_tokens")) or 0
        reasoning_tokens = as_nonnegative_int(row.get("reasoning_tokens")) or 0
        breakdown = cls._breakdown(row.get("token_breakdown_json"))
        input_breakdown = (
            breakdown.get("input")
            if isinstance(breakdown.get("input"), Mapping)
            else {}
        )
        output_breakdown = (
            breakdown.get("output")
            if isinstance(breakdown.get("output"), Mapping)
            else {}
        )
        breakdown_cached = as_nonnegative_int(
            input_breakdown.get("cache_read_tokens")
        )
        if breakdown_cached is not None and breakdown_cached <= input_tokens:
            cached_tokens = breakdown_cached
        cached_tokens = min(cached_tokens, input_tokens)
        cache_write_tokens = as_nonnegative_int(
            input_breakdown.get("cache_write_tokens")
        ) or 0
        cache_write_tokens = min(cache_write_tokens, max(input_tokens - cached_tokens, 0))
        breakdown_reasoning = as_nonnegative_int(
            output_breakdown.get("reasoning_tokens")
        )
        if breakdown_reasoning is not None and breakdown_reasoning <= output_tokens:
            reasoning_tokens = breakdown_reasoning
        reasoning_tokens = min(reasoning_tokens, output_tokens)
        if total_tokens is None:
            total_tokens = input_tokens + output_tokens
        usage = NormalizedUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=reasoning_tokens,
            total_tokens=total_tokens,
        )

        input_rate = cls._safe_rate(row.get("input_usd_per_million"))
        output_rate = cls._safe_rate(row.get("output_usd_per_million"))
        cached_rate = cls._safe_rate(row.get("cached_input_usd_per_million"))
        if cached_rate is None:
            cached_rate = input_rate
        uncached_tokens = as_nonnegative_int(input_breakdown.get("uncached_tokens"))
        if uncached_tokens is None:
            uncached_tokens = max(
                input_tokens - cached_tokens - cache_write_tokens,
                0,
            )
        elif uncached_tokens + cached_tokens + cache_write_tokens > input_tokens:
            uncached_tokens = max(
                input_tokens - cached_tokens - cache_write_tokens,
                0,
            )
        applicable_rates = [
            rate
            for tokens, rate in (
                (uncached_tokens + cache_write_tokens, input_rate),
                (cached_tokens, cached_rate),
                (output_tokens, output_rate),
            )
            if tokens > 0
        ]
        has_tokens = input_tokens > 0 or output_tokens > 0
        if (
            any(rate is None for rate in applicable_rates)
            or (
                has_tokens
                and applicable_rates
                and all(float(rate or 0.0) == 0.0 for rate in applicable_rates)
            )
        ):
            return usage, None
        components = PriceComponents(
            non_cached_input_cost_usd=(
                (uncached_tokens + cache_write_tokens)
                * float(input_rate or 0.0)
                / 1_000_000
            ),
            cached_input_cost_usd=(
                cached_tokens * float(cached_rate or 0.0) / 1_000_000
            ),
            output_cost_usd=(
                output_tokens * float(output_rate or 0.0) / 1_000_000
            ),
            long_context_pricing_applied=input_tokens > LONG_CONTEXT_THRESHOLD_TOKENS,
        )
        return usage, components

    def _event_from_row(self, row: Mapping[str, Any]) -> tuple[UsageEvent, str] | None:
        import_key = self.resolver.cockpit_event_import_key(row.get("event_key"))
        timestamp = normalize_optional_timestamp(row.get("timestamp"))
        if not import_key or not timestamp:
            return None
        storage_id = safe_text(row.get("account_id"), 512)
        identity = self.resolver.resolve_cockpit_account(storage_id)
        identity_key_value = (
            f"subscription:{identity.subscription_id_hash}"
            if identity.subscription_id_hash
            else "unknown"
        )
        usage, components = self._usage_and_components(row)
        success = bool(as_nonnegative_int(row.get("success")))
        raw_status = as_nonnegative_int(row.get("http_status"))
        status_code = (
            raw_status
            if raw_status is not None and 100 <= raw_status <= 599
            else (200 if success else 500)
        )
        model = safe_model_identifier(row.get("model_id"))
        components = self.repo.correct_astra_context_components(model, usage, components)
        total_only_snapshot = self._safe_rate(row.get("estimated_cost_usd"))
        if total_only_snapshot is not None and total_only_snapshot <= 0:
            total_only_snapshot = None
        event = UsageEvent(
            ts=timestamp,
            identity_key=identity_key_value,
            endpoint="local://cockpit-tools",
            method="LOCAL",
            model=model,
            status_code=status_code,
            ok=int(success),
            duration_ms=as_nonnegative_int(row.get("latency_ms")) or 0,
            stream=0,
            session_id=None,
            thread_id=None,
            turn_id=None,
            installation_id=None,
            window_id=None,
            usage_alias=identity.usage_alias,
            usage_project=None,
            auth_fingerprint=None,
            account_id_hash=identity.account_id_hash,
            account_id_tail=identity.account_id_tail,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cached_tokens=usage.cached_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            reasoning_tokens=usage.reasoning_tokens,
            total_tokens=usage.total_tokens,
            estimated_api_cost_usd=(
                components.total_cost_usd
                if components
                else total_only_snapshot
            ),
            non_cached_input_cost_usd=(
                components.non_cached_input_cost_usd if components else None
            ),
            cached_input_cost_usd=(
                components.cached_input_cost_usd if components else None
            ),
            output_cost_usd=(components.output_cost_usd if components else None),
            long_context_pricing_applied=int(
                bool(components and model and components.long_context_pricing_applied)
            ),
            subscription_amortized_cost_usd=None,
            api_equivalent_quota_usd=None,
            usage_missing=0,
            error_type=None,
            error_message_redacted=None,
            request_bytes=0,
            response_bytes=0,
            source=COCKPIT_TOOLS_REQUEST_SOURCE,
            request_id=None,
            # An empty Cockpit selector means its gateway rejected the client
            # before choosing a subscription. Preserve the request history,
            # but do not let it masquerade as an upstream response or
            # account-pool attempt.
            account_attempt=int(bool(storage_id)),
        )
        return event, import_key

    def _import_quota_records(self, records: Sequence[Mapping[str, Any]]) -> int:
        count = 0
        for record in records:
            if record.get("terminal_error_code") or record.get(
                "credential_cache_missing"
            ):
                continue
            fetched_at = normalize_optional_timestamp(record.get("usage_updated_at"))
            if not fetched_at:
                continue
            identity = self.resolver.resolve_cockpit_account(
                safe_text(record.get("id"), 512),
                safe_email(record.get("email")),
            )
            if not identity.subscription_id_hash:
                continue
            key = f"subscription:{identity.subscription_id_hash}"
            for prefix, kind in (("hourly", "five_hour"), ("weekly", "weekly")):
                remaining = _percent_value(record.get(f"{prefix}_percentage"))
                if record.get(f"{prefix}_window_present") is not True or remaining is None:
                    continue
                minutes = as_nonnegative_int(record.get(f"{prefix}_window_minutes"))
                window_seconds = minutes * 60 if minutes is not None else None
                kind = quota_window_kind(window_seconds, kind)
                inserted = self.repo.insert_subscription_quota_snapshot(
                    {
                        "fetched_at": fetched_at,
                        "identity_key": key,
                        "plan_type": record.get("plan_type"),
                        "subscription_active_until": record.get(
                            "subscription_active_until"
                        ),
                        "window_kind": kind,
                        "used_percent": 100.0 - remaining,
                        "remaining_percent": remaining,
                        "window_seconds": window_seconds,
                        "reset_at": record.get(f"{prefix}_reset_time"),
                        "provider_allowed": record.get("provider_allowed"),
                        "provider_limit_reached": record.get(
                            "provider_limit_reached"
                        ),
                        "source": COCKPIT_TOOLS_QUOTA_SOURCE,
                    }
                )
                count += int(inserted)
        return count

    def import_once(self) -> dict[str, Any]:
        inventory = self.resolver.cockpit_inventory(force_refresh=True)
        # Apply resolver-proven fallback lineage while the old registry key is
        # still active.  Reconciling the new inventory first would mark that
        # predecessor suspect and deliberately make privacy migration fail.
        migrations = self.resolver.identity_migrations()
        migration_marker = short_hash(
            json.dumps(sorted(migrations.items()), separators=(",", ":"))
        )
        if migration_marker != self._identity_migration_marker:
            self.repo.apply_privacy_minimization(self.resolver)
            self._identity_migration_marker = migration_marker
        if inventory["detected"] and inventory["authoritative"]:
            self._inventory_result = self.repo.reconcile_subscription_inventory(
                self.resolver.active_subscription_keys(),
                utc_now(),
                authoritative=True,
            )
        else:
            self._inventory_result = {
                "authoritative": False,
                "active": len(self.resolver.active_subscription_keys()),
                "suspect": 0,
                "retired": 0,
            }
        quota_rows = self._import_quota_records(
            self.resolver.cockpit_account_records()
        )
        if not self.database_path.is_file():
            return {
                "imported": 0,
                "scanned": 0,
                "quota_rows": quota_rows,
                "database_available": False,
            }

        with closing(open_sqlite_readonly(self.database_path)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_logs'"
            ).fetchone()
            if table is None:
                raise ValueError("cockpit_request_logs_missing")
            available_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(request_logs)")
            }
            missing = set(self.REQUIRED_REQUEST_COLUMNS) - available_columns
            if missing:
                raise ValueError("cockpit_request_logs_schema")
            maxima = connection.execute(
                """SELECT COALESCE(MAX(id), 0) AS max_id,
                          COALESCE(MAX(model_pricing_version), 0) AS max_version
                     FROM request_logs"""
            ).fetchone()
            max_id = as_nonnegative_int(maxima["max_id"]) or 0
            max_version = as_nonnegative_int(maxima["max_version"]) or 0
            state = self.repo.local_import_file_state(self.database_path) or {}
            cursor_id = as_nonnegative_int(state.get("offset")) or 0
            previous_version = as_nonnegative_int(state.get("size")) or 0
            response_backfill = self.repo.api_response_backfill_required(
                COCKPIT_TOOLS_REQUEST_SOURCE
            )
            if max_id < cursor_id or (state and max_version != previous_version):
                cursor_id = 0
            if response_backfill:
                # Versioned, idempotent replay restores the minute/status
                # observations for historical request rows whose account-level
                # usage detail was already removed by privacy retirement.
                cursor_id = 0
            columns = ", ".join(self.REQUIRED_REQUEST_COLUMNS)
            rows = connection.execute(
                f"SELECT {columns} FROM request_logs WHERE id>? AND id<=? ORDER BY id",
                (cursor_id, max_id),
            )
            imported = 0
            scanned = 0
            for sqlite_row in rows:
                scanned += 1
                converted = self._event_from_row(dict(sqlite_row))
                if converted is None:
                    continue
                event, import_key = converted
                if self.repo.sync_imported_event(
                    event,
                    import_key,
                    COCKPIT_TOOLS_REQUEST_SOURCE,
                    restore_retired_observation=response_backfill,
                ):
                    imported += 1
        try:
            database_mtime = self.database_path.stat().st_mtime_ns
        except OSError:
            database_mtime = 0
        self.repo.save_local_import_file_state(
            {
                "path": self.database_path,
                "size": max_version,
                "mtime_ns": database_mtime,
                "offset": max_id,
            }
        )
        if response_backfill:
            self.repo.mark_api_response_backfill(COCKPIT_TOOLS_REQUEST_SOURCE)
        return {
            "imported": imported,
            "scanned": scanned,
            "quota_rows": quota_rows,
            "database_available": True,
        }


class Sub2APIHTTPError(RuntimeError):
    def __init__(self, status: int):
        super().__init__(f"Sub2API HTTP {status}")
        self.status = status


class Sub2APISchemaError(ValueError):
    pass


class Sub2APIImporter:
    """Import Sub2API's account inventory and successful usage logs over HTTP.

    The management key is read only when a request is made and is never
    persisted or logged.  The importer accepts only a loopback origin because
    a Sub2API admin key is substantially more privileged than a normal gateway
    key; remote instances should be exposed through a local tunnel.
    """

    def __init__(
        self,
        repo: UsageRepository,
        resolver: AccountResolver,
        *,
        base_url: str = DEFAULT_SUB2API_BASE_URL,
        key_file: str | Path | None = None,
        key_env: str = "SUB2API_ADMIN_KEY",
        poll_seconds: float = DEFAULT_SUB2API_POLL_SECONDS,
        timeout: float = DEFAULT_SUB2API_TIMEOUT,
        page_size: int = DEFAULT_SUB2API_PAGE_SIZE,
        backfill_days: int = DEFAULT_SUB2API_BACKFILL_DAYS,
    ) -> None:
        self.repo = repo
        self.resolver = resolver
        parsed = urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or (parsed.hostname or "").casefold() not in LOOPBACK_HOSTS
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Sub2API base URL must be a loopback HTTP(S) origin")
        base_path = parsed.path.rstrip("/")
        if base_path in {"", "/"}:
            api_root = "/api/v1"
        elif base_path == "/api/v1":
            api_root = base_path
        else:
            raise ValueError("Sub2API base URL path must be empty or /api/v1")
        self.origin = parsed
        self.api_root = api_root
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self.instance_scope = (
            f"{parsed.scheme}://{(parsed.hostname or '').casefold()}:{port}{api_root}"
        )
        self.key_file = Path(key_file).expanduser() if key_file else None
        self.key_env = safe_text(key_env, 128) if key_env is not None else "SUB2API_ADMIN_KEY"
        self.poll_seconds = max(1.0, float(poll_seconds))
        self.timeout = max(1.0, float(timeout))
        self.page_size = max(1, min(int(page_size), 1000))
        self.backfill_days = max(1, min(int(backfill_days), 3650))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._warned_no_key = False
        self._warned_permissions = False
        self._backoff = self.poll_seconds
        self._last_status: int | None = None
        self._last_poll_at: str | None = None
        self._last_success_at: str | None = None
        self._last_error_type: str | None = None
        self._last_imported = 0
        self._last_scanned = 0
        self._last_changed = 0
        self._last_unchanged = 0
        self._last_retired = 0
        self._last_source_conflict = 0
        self._last_write_transactions = 0
        self._last_usage_updates = 0
        self._last_observation_updates = 0
        self._last_account_count = 0
        self._last_quota_rows = 0
        self._api_available = False
        self._inventory_result: dict[str, Any] = {
            "authoritative": False,
            "active": 0,
            "suspect": 0,
            "retired": 0,
        }

    @property
    def configured(self) -> bool:
        return bool(
            self.key_file
            or (self.key_env and os.environ.get(self.key_env, "").strip())
        )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="sub2api-import",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._thread = None

    def status(self) -> dict[str, Any]:
        key_loaded = bool(self._load_key())
        env_configured = bool(
            self.key_env and os.environ.get(self.key_env, "").strip()
        )
        inventory = {
            key: value
            for key, value in self._inventory_result.items()
            if key != "retired_keys"
        }
        return {
            "enabled": True,
            "configured": bool(self.key_file or env_configured),
            "key_source": (
                "file" if self.key_file else ("env" if env_configured else "none")
            ),
            "key_loaded": key_loaded,
            "api_available": self._api_available,
            "last_status": self._last_status,
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error_type": self._last_error_type,
            "last_imported": self._last_imported,
            "last_new": self._last_imported,
            "last_changed": self._last_changed,
            "last_unchanged": self._last_unchanged,
            "last_retired": self._last_retired,
            "last_source_conflict": self._last_source_conflict,
            "last_write_transactions": self._last_write_transactions,
            "last_usage_updates": self._last_usage_updates,
            "last_observation_updates": self._last_observation_updates,
            "last_scanned": self._last_scanned,
            "account_count": self._last_account_count,
            "last_quota_rows": self._last_quota_rows,
            "poll_seconds": self.poll_seconds,
            "backfill_days": self.backfill_days,
            "backoff_seconds": self._backoff,
            "inventory": inventory,
            **self.repo.import_status(SUB2API_REQUEST_SOURCE),
        }

    def _load_key(self) -> str | None:
        if self.key_file:
            try:
                file_stat = self.key_file.stat()
                if not self.key_file.is_file() or self.key_file.is_symlink():
                    return None
                if file_stat.st_mode & 0o077:
                    if not self._warned_permissions:
                        LOG.error("Sub2API admin key file must be owner-only (mode 600)")
                        self._warned_permissions = True
                    return None
                if file_stat.st_size > 4096:
                    return None
                with self.key_file.open("r", encoding="utf-8") as handle:
                    value = handle.read(4097).strip()
            except (OSError, UnicodeError):
                value = ""
            if value:
                return value if len(value) <= 4096 else None
        value = os.environ.get(self.key_env, "").strip() if self.key_env else ""
        return value if value and len(value) <= 4096 else None

    def _run(self) -> None:
        while not self._stop.is_set():
            key = self._load_key()
            if not key:
                if not self._warned_no_key:
                    LOG.info("Sub2API importer idle: no admin key file/environment configured")
                    self._warned_no_key = True
                self._stop.wait(30.0)
                continue
            self._warned_no_key = False
            try:
                result = self.import_once(key)
                self._last_poll_at = utc_now()
                self._last_success_at = self._last_poll_at
                self._last_error_type = None
                self._last_imported = result["imported"]
                self._last_scanned = result["scanned"]
                self._last_changed = result["changed"]
                self._last_unchanged = result["unchanged"]
                self._last_retired = result["retired"]
                self._last_source_conflict = result["source_conflict"]
                self._last_write_transactions = result["write_transactions"]
                self._last_usage_updates = result["usage_updates"]
                self._last_observation_updates = result["observation_updates"]
                self._last_account_count = result["accounts"]
                self._last_quota_rows = result["quota_rows"]
                self._api_available = True
                self._backoff = self.poll_seconds
                LOG.info(
                    "Sub2API import scanned=%d new=%d changed=%d unchanged=%d "
                    "retired=%d source_conflict=%d write_transactions=%d "
                    "usage_updates=%d observation_updates=%d",
                    result["scanned"],
                    result["new"],
                    result["changed"],
                    result["unchanged"],
                    result["retired"],
                    result["source_conflict"],
                    result["write_transactions"],
                    result["usage_updates"],
                    result["observation_updates"],
                )
            except Sub2APIHTTPError as exc:
                self._last_poll_at = utc_now()
                self._last_status = exc.status
                self._api_available = True
                self._last_error_type = (
                    "Sub2APIAuthenticationError"
                    if exc.status in {401, 403}
                    else "Sub2APIHTTPError"
                )
                if exc.status in {401, 403, 429}:
                    self._backoff = DEFAULT_MANAGEMENT_BACKOFF_SECONDS
                else:
                    self._backoff = min(
                        max(self._backoff * 2, 5.0),
                        MAX_MANAGEMENT_BACKOFF_SECONDS,
                    )
                LOG.warning("Sub2API import failed: %s", self._last_error_type)
            except Exception as exc:
                self._last_poll_at = utc_now()
                self._last_error_type = type(exc).__name__
                self._api_available = False
                self._backoff = min(
                    max(self._backoff * 2, 5.0),
                    MAX_MANAGEMENT_BACKOFF_SECONDS,
                )
                LOG.warning("Sub2API import failed: %s", type(exc).__name__)
            self._stop.wait(self._backoff)

    def _connection(self) -> http.client.HTTPConnection:
        port = self.origin.port or (443 if self.origin.scheme == "https" else 80)
        if self.origin.scheme == "https":
            return http.client.HTTPSConnection(
                self.origin.hostname,
                port,
                timeout=self.timeout,
                context=ssl.create_default_context(),
            )
        return http.client.HTTPConnection(
            self.origin.hostname,
            port,
            timeout=self.timeout,
        )

    def _request_json(
        self,
        endpoint: str,
        key: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        target = f"{self.api_root}{endpoint}"
        if params:
            target += "?" + urllib.parse.urlencode(
                [(name, str(value)) for name, value in params.items() if value is not None]
            )
        connection = self._connection()
        try:
            connection.request(
                "GET",
                target,
                headers={
                    "Accept": "application/json",
                    "Connection": "close",
                    "X-API-Key": key,
                    "X-Admin-UI-Request": "1",
                },
            )
            response = connection.getresponse()
            self._last_status = response.status
            content_length = as_nonnegative_int(response.getheader("Content-Length"))
            if content_length is not None and content_length > MAX_SUB2API_RESPONSE_BYTES:
                raise Sub2APISchemaError("Sub2API response is too large")
            body = response.read(MAX_SUB2API_RESPONSE_BYTES + 1)
            if len(body) > MAX_SUB2API_RESPONSE_BYTES:
                raise Sub2APISchemaError("Sub2API response is too large")
            if response.status != 200:
                raise Sub2APIHTTPError(response.status)
        finally:
            connection.close()
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Sub2APISchemaError("Sub2API returned invalid JSON") from exc
        if not isinstance(payload, Mapping) or payload.get("code") not in {0, None}:
            raise Sub2APISchemaError("Sub2API returned an invalid envelope")
        if "data" not in payload:
            raise Sub2APISchemaError("Sub2API response data is missing")
        return payload["data"]

    def _paginated(
        self,
        endpoint: str,
        key: str,
        params: Mapping[str, Any] | None = None,
    ) -> list[Mapping[str, Any]]:
        common = dict(params or {})
        records: list[Mapping[str, Any]] = []
        seen_ids: set[int] = set()
        expected_total: int | None = None
        for page in range(1, MAX_SUB2API_PAGES + 1):
            data = self._request_json(
                endpoint,
                key,
                {**common, "page": page, "page_size": self.page_size},
            )
            if not isinstance(data, Mapping) or not isinstance(data.get("items"), list):
                raise Sub2APISchemaError("Sub2API pagination data is invalid")
            items = data["items"]
            for item in items:
                if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                    raise Sub2APISchemaError("Sub2API item is invalid")
                try:
                    item_id = int(item.get("id"))
                except (TypeError, ValueError, OverflowError) as exc:
                    raise Sub2APISchemaError("Sub2API item ID is invalid") from exc
                if item_id <= 0 or item_id in seen_ids:
                    raise Sub2APISchemaError("Sub2API pagination is incomplete")
                seen_ids.add(item_id)
                records.append(item)
            pages = as_nonnegative_int(data.get("pages"))
            response_page = as_nonnegative_int(data.get("page"))
            if response_page is not None and response_page != page:
                raise Sub2APISchemaError("Sub2API pagination page mismatch")
            response_page_size = as_nonnegative_int(data.get("page_size"))
            if response_page_size is not None and response_page_size != self.page_size:
                raise Sub2APISchemaError("Sub2API pagination size mismatch")
            total = as_nonnegative_int(data.get("total"))
            if total is not None:
                if expected_total is None:
                    expected_total = total
                elif total != expected_total:
                    raise Sub2APISchemaError("Sub2API pagination changed during import")
            if len(items) < self.page_size or (pages is not None and page >= pages):
                if expected_total is not None and expected_total != len(records):
                    raise Sub2APISchemaError("Sub2API pagination is incomplete")
                return records
        raise Sub2APISchemaError("Sub2API pagination limit exceeded")

    @staticmethod
    def _safe_cost(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return result if math.isfinite(result) and 0 <= result <= 1_000_000_000 else None

    @staticmethod
    def _timestamp_plus_seconds(timestamp: Any, seconds: Any) -> str | None:
        base = normalize_optional_timestamp(timestamp)
        duration = as_nonnegative_int(seconds)
        if not base or duration is None:
            return None
        try:
            parsed = datetime.fromisoformat(base.replace("Z", "+00:00"))
        except ValueError:
            return None
        return (parsed + timedelta(seconds=duration)).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )

    @staticmethod
    def _account_state(account: Mapping[str, Any], now: str) -> str:
        status = (safe_text(account.get("status"), 32) or "unknown").casefold()
        if status in {"inactive", "error"}:
            return status
        for field, state in (
            ("rate_limit_reset_at", "rate_limited"),
            ("overload_until", "overloaded"),
            ("temp_unschedulable_until", "temporarily_unschedulable"),
        ):
            until = normalize_optional_timestamp(account.get(field))
            if until and until > now:
                return state
        if account.get("schedulable") is not True:
            return "unschedulable"
        return "active" if status == "active" else "unknown"

    def _import_account_snapshots(
        self,
        accounts: Sequence[Mapping[str, Any]],
    ) -> int:
        inserted = 0
        now = utc_now()
        previous_statuses = {
            str(row.get("identity_key")): row
            for row in self.repo.latest_subscription_quotas()
            if row.get("window_kind") == "account_status"
        }
        for account in accounts:
            identity = self.resolver.resolve_sub2api_account(account.get("id"))
            if not identity.subscription_id_hash:
                continue
            identity_key_value = f"subscription:{identity.subscription_id_hash}"
            extra = account.get("extra")
            extra = extra if isinstance(extra, Mapping) else {}
            state = self._account_state(account, now)
            available = (
                True
                if state == "active"
                else (
                    False
                    if state
                    in {
                        "inactive",
                        "error",
                        "rate_limited",
                        "overloaded",
                        "temporarily_unschedulable",
                        "unschedulable",
                    }
                    else None
                )
            )
            rate_limited = (
                True if state == "rate_limited" else (False if state == "active" else None)
            )
            plan_type = safe_plan_type(extra.get("plan_type"))
            active_until = normalize_optional_timestamp(account.get("expires_at"))
            common = {
                "identity_key": identity_key_value,
                "plan_type": plan_type,
                "subscription_active_until": active_until,
                "provider_allowed": available,
                "provider_limit_reached": rate_limited,
            }
            status_method = f"sub2api_{state}"
            previous = previous_statuses.get(identity_key_value)
            status_changed = previous is None or any(
                (
                    previous.get("estimate_method") != status_method,
                    previous.get("plan_type") != plan_type,
                    previous.get("subscription_active_until") != active_until,
                    previous.get("provider_allowed")
                    != (None if available is None else int(available)),
                    previous.get("provider_limit_reached")
                    != (None if rate_limited is None else int(rate_limited)),
                )
            )
            if status_changed:
                inserted += int(
                    self.repo.insert_subscription_quota_snapshot(
                        {
                            **common,
                            "fetched_at": now,
                            "window_kind": "account_status",
                            "estimate_method": status_method,
                            "source": SUB2API_ACCOUNT_SOURCE,
                        }
                    )
                )

            usage_fetched_at = normalize_optional_timestamp(
                extra.get("codex_usage_updated_at")
            )
            if not usage_fetched_at:
                continue
            for prefix, kind in (("codex_5h", "five_hour"), ("codex_7d", "weekly")):
                used = _percent_value(extra.get(f"{prefix}_used_percent"))
                if used is None:
                    continue
                minutes = as_nonnegative_int(extra.get(f"{prefix}_window_minutes"))
                reset_at = normalize_optional_timestamp(extra.get(f"{prefix}_reset_at"))
                if not reset_at:
                    reset_at = self._timestamp_plus_seconds(
                        usage_fetched_at,
                        extra.get(f"{prefix}_reset_after_seconds"),
                    )
                inserted += int(
                    self.repo.insert_subscription_quota_snapshot(
                        {
                            **common,
                            "fetched_at": usage_fetched_at,
                            "window_kind": kind,
                            "used_percent": used,
                            "remaining_percent": 100.0 - used,
                            "window_seconds": minutes * 60 if minutes is not None else None,
                            "reset_at": reset_at,
                            "source": SUB2API_QUOTA_SOURCE,
                        }
                    )
                )
        return inserted

    def _event_from_record(
        self,
        record: Mapping[str, Any],
    ) -> tuple[UsageEvent, str] | None:
        timestamp = normalize_optional_timestamp(record.get("created_at"))
        import_key = self.resolver.sub2api_event_import_key(
            self.instance_scope,
            record.get("id"),
            record.get("request_id"),
            timestamp,
        )
        if not timestamp or not import_key:
            return None
        identity = self.resolver.resolve_sub2api_account(record.get("account_id"))
        identity_key_value = (
            f"subscription:{identity.subscription_id_hash}"
            if identity.subscription_id_hash
            else "unknown"
        )
        non_cached_tokens = as_nonnegative_int(record.get("input_tokens")) or 0
        cache_write_tokens = as_nonnegative_int(record.get("cache_creation_tokens")) or 0
        cached_tokens = as_nonnegative_int(record.get("cache_read_tokens")) or 0
        output_tokens = as_nonnegative_int(record.get("output_tokens")) or 0
        input_tokens = non_cached_tokens + cache_write_tokens + cached_tokens

        non_cached_cost_parts = (
            self._safe_cost(record.get("input_cost")),
            self._safe_cost(record.get("cache_creation_cost")),
            self._safe_cost(record.get("image_input_cost")),
        )
        cached_cost = self._safe_cost(record.get("cache_read_cost"))
        output_cost_parts = (
            self._safe_cost(record.get("output_cost")),
            self._safe_cost(record.get("image_output_cost")),
        )
        declared_total = self._safe_cost(record.get("total_cost"))
        components_valid = (
            all(value is not None for value in non_cached_cost_parts)
            and cached_cost is not None
            and all(value is not None for value in output_cost_parts)
        )
        non_cached_cost: float | None = None
        output_cost: float | None = None
        estimated_cost = declared_total
        if components_valid:
            non_cached_cost = sum(float(value or 0.0) for value in non_cached_cost_parts)
            output_cost = sum(float(value or 0.0) for value in output_cost_parts)
            component_total = non_cached_cost + float(cached_cost or 0.0) + output_cost
            if declared_total is not None and not math.isclose(
                component_total,
                declared_total,
                rel_tol=1e-9,
                abs_tol=1e-12,
            ):
                non_cached_cost = None
                cached_cost = None
                output_cost = None
            else:
                # SQLite requires the three frozen components to sum exactly
                # to the stored total.  Sub2API's declared total can differ by
                # a final decimal rounding unit, so use the component sum when
                # the upstream values otherwise agree.
                estimated_cost = component_total

        model = safe_model_identifier(record.get("model"))
        long_context = bool(record.get("long_context_billing_applied"))
        if non_cached_cost is not None and cached_cost is not None and output_cost is not None:
            components = self.repo.correct_astra_context_components(
                model,
                NormalizedUsage(
                    input_tokens=input_tokens, output_tokens=output_tokens,
                    cached_tokens=cached_tokens, cache_write_tokens=cache_write_tokens,
                ),
                PriceComponents(non_cached_cost, cached_cost, output_cost, long_context),
            )
            assert components is not None
            non_cached_cost = components.non_cached_input_cost_usd
            cached_cost = components.cached_input_cost_usd
            output_cost = components.output_cost_usd
            estimated_cost = components.total_cost_usd
            long_context = components.long_context_pricing_applied

        stream = bool(record.get("stream")) or safe_text(
            record.get("request_type"), 32
        ) in {"stream", "ws_v2", "live"}
        event = UsageEvent(
            ts=timestamp,
            identity_key=identity_key_value,
            endpoint="local://sub2api",
            method="LOCAL",
            model=model,
            status_code=200,
            ok=1,
            duration_ms=as_nonnegative_int(record.get("duration_ms")) or 0,
            stream=int(stream),
            session_id=None,
            thread_id=None,
            turn_id=None,
            installation_id=None,
            window_id=None,
            usage_alias=identity.usage_alias,
            usage_project=None,
            auth_fingerprint=None,
            account_id_hash=identity.account_id_hash,
            account_id_tail=identity.account_id_tail,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=None,
            total_tokens=input_tokens + output_tokens,
            estimated_api_cost_usd=estimated_cost,
            non_cached_input_cost_usd=non_cached_cost,
            cached_input_cost_usd=cached_cost,
            output_cost_usd=output_cost,
            long_context_pricing_applied=int(long_context),
            subscription_amortized_cost_usd=None,
            api_equivalent_quota_usd=None,
            usage_missing=0,
            error_type=None,
            error_message_redacted=None,
            request_bytes=0,
            response_bytes=0,
            source=SUB2API_REQUEST_SOURCE,
            request_id=None,
            account_attempt=int(record.get("account_id") is not None),
        )
        return event, import_key

    def _usage_range(self, now: datetime) -> tuple[str, str]:
        state = self.repo.remote_import_state(SUB2API_REQUEST_SOURCE) or {}
        last_complete = normalize_optional_timestamp(state.get("last_complete_at"))
        start = now - timedelta(days=self.backfill_days)
        if last_complete:
            try:
                parsed = datetime.fromisoformat(last_complete.replace("Z", "+00:00"))
            except ValueError:
                parsed = None
            if parsed is not None:
                start = parsed.astimezone(timezone.utc) - timedelta(days=1)
        return start.date().isoformat(), now.date().isoformat()

    def import_once(self, key: str | None = None) -> dict[str, Any]:
        loaded_key = key or self._load_key()
        if not loaded_key:
            raise ValueError("Sub2API admin key is not configured")
        scan_started = datetime.now(timezone.utc)
        accounts = self._paginated(
            "/admin/accounts",
            loaded_key,
            {"sort_by": "id", "sort_order": "asc"},
        )
        self.resolver.register_sub2api_accounts(self.instance_scope, accounts)
        self._inventory_result = self.repo.reconcile_subscription_inventory(
            self.resolver.active_subscription_keys(),
            utc_now(),
            authoritative=True,
        )
        quota_rows = self._import_account_snapshots(accounts)

        start_date, end_date = self._usage_range(scan_started)
        usage_records = self._paginated(
            "/admin/usage",
            loaded_key,
            {
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "UTC",
                "sort_by": "id",
                "sort_order": "asc",
                "exact_total": "true",
            },
        )
        converted_events: list[tuple[UsageEvent, str]] = []
        for record in usage_records:
            converted = self._event_from_record(record)
            if converted is None:
                continue
            converted_events.append(converted)
        sync_result = self.repo.sync_imported_events(
            converted_events,
            SUB2API_REQUEST_SOURCE,
        )
        self.repo.save_remote_import_state(
            SUB2API_REQUEST_SOURCE,
            scan_started,
        )
        return {
            "imported": sync_result["new"],
            "scanned": len(usage_records),
            "accounts": len(accounts),
            "quota_rows": quota_rows,
            **{
                key: sync_result[key]
                for key in (
                    "new",
                    "changed",
                    "unchanged",
                    "retired",
                    "source_conflict",
                    "write_transactions",
                    "usage_updates",
                    "observation_updates",
                )
            },
        }


@dataclass
class CodexAppFileScan:
    ok: bool
    state: dict[str, Any] | None
    events: list[tuple[UsageEvent, str]]
    quota_snapshots: list[dict[str, Any]]


@dataclass(frozen=True)
class CodexAppHomeContext:
    identity: AccountIdentity
    usage_alias: str | None
    default_plan: str | None
    active_until: str | None
    binding_key: str
    auth_signature: tuple[int, int, int, int]


class CodexAppLocalImporter:
    """Incrementally import safe usage fields from local Codex JSONL files.

    Dynamic mode follows the current auth identity in the configured Codex
    home and every Cockpit Codex instance. A keyed, opaque per-home binding is
    persisted so an account switch first advances file cursors to a safe
    boundary; records that predate that boundary are never guessed to belong
    to the newly selected account. Supplying an explicit alias retains the
    original strict single-home matching mode.
    """

    def __init__(
        self,
        repo: UsageRepository,
        resolver: AccountResolver,
        codex_home: Path | str = DEFAULT_CODEX_APP_HOME,
        alias: str | None = DEFAULT_CODEX_APP_ALIAS,
        poll_seconds: float = DEFAULT_CODEX_APP_POLL_SECONDS,
        max_files: int = DEFAULT_CODEX_APP_MAX_FILES,
    ) -> None:
        self.repo = repo
        self.resolver = resolver
        self.codex_home = Path(codex_home).expanduser().resolve()
        self.alias = safe_alias(alias)
        self.poll_seconds = max(5.0, float(poll_seconds))
        self.max_files = max(1, min(int(max_files), 10_000))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_poll_at: str | None = None
        self._last_success_at: str | None = None
        self._last_error_type: str | None = None
        self._last_imported = 0
        self._last_scanned_files = 0
        self._last_baselined_files = 0
        self._discovered_homes = 0
        self._matched_homes = 0
        self._failed_homes = 0
        self._rejected_homes = 0
        self._account_match: bool | None = None
        self._member_match: bool | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="codex-app-local-import", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._thread = None

    def status(self) -> dict[str, Any]:
        persisted = self.repo.import_status("codex_app_local")
        return {
            "enabled": True,
            "codex_home_configured": True,
            "account_mode": "fixed" if self.alias else "dynamic",
            "usage_alias": self.alias,
            "account_match": self._account_match,
            "member_match": self._member_match,
            "discovered_homes": self._discovered_homes,
            "matched_homes": self._matched_homes,
            "failed_homes": self._failed_homes,
            "rejected_homes": self._rejected_homes,
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error_type": self._last_error_type,
            "last_imported": self._last_imported,
            "last_scanned_files": self._last_scanned_files,
            "last_baselined_files": self._last_baselined_files,
            **persisted,
        }

    def _remember_result(self, result: Mapping[str, Any]) -> None:
        self._last_imported = as_nonnegative_int(result.get("imported")) or 0
        self._last_scanned_files = (
            as_nonnegative_int(result.get("scanned_files")) or 0
        )
        self._last_baselined_files = (
            as_nonnegative_int(result.get("baselined_files")) or 0
        )
        self._discovered_homes = (
            as_nonnegative_int(result.get("discovered_homes")) or 0
        )
        self._matched_homes = as_nonnegative_int(result.get("matched_homes")) or 0
        self._failed_homes = as_nonnegative_int(result.get("failed_homes")) or 0
        self._rejected_homes = as_nonnegative_int(result.get("rejected_homes")) or 0

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                result = self.import_once()
                self._last_poll_at = utc_now()
                if self._matched_homes:
                    self._last_success_at = self._last_poll_at
                if self._failed_homes or self._rejected_homes:
                    self._last_error_type = (
                        "PartialHomeFailure"
                        if self._matched_homes
                        else "CodexHomeUnavailable"
                    )
                else:
                    self._last_error_type = None
                self._remember_result(result)
            except Exception as exc:
                self._last_poll_at = utc_now()
                self._last_error_type = type(exc).__name__
                LOG.warning("Codex app local import failed: %s", type(exc).__name__)
            self._stop.wait(self.poll_seconds)

    @staticmethod
    def _read_auth_identity(
        path: Path,
    ) -> tuple[
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ]:
        data = AccountResolver._read_json(path)
        if not isinstance(data, Mapping):
            return None, None, None, None, None, None
        (
            account,
            _access_tokens,
            email,
            principal_id,
            provider_subscription_id,
        ) = AccountResolver._account_and_tokens(data)
        nested = data.get("tokens") if isinstance(data.get("tokens"), Mapping) else {}
        raw_id_token = nested.get("id_token") or data.get("id_token")
        claims = (
            raw_id_token
            if isinstance(raw_id_token, Mapping)
            else _decode_jwt_claims_unverified(raw_id_token)
        )
        auth_claims = claims.get("https://api.openai.com/auth")
        auth_claims = auth_claims if isinstance(auth_claims, Mapping) else {}
        plan = safe_plan_type(
            auth_claims.get("chatgpt_plan_type")
            or claims.get("chatgpt_plan_type")
            or claims.get("plan_type")
        )
        active_until = normalize_optional_timestamp(
            auth_claims.get("chatgpt_subscription_active_until")
            or claims.get("chatgpt_subscription_active_until")
        )
        return (
            account,
            email,
            principal_id,
            provider_subscription_id,
            plan,
            active_until,
        )

    @staticmethod
    def _usage_from_token_count(payload: Mapping[str, Any]) -> NormalizedUsage:
        info = payload.get("info") if isinstance(payload.get("info"), Mapping) else {}
        raw = info.get("last_token_usage")
        if not isinstance(raw, Mapping):
            return NormalizedUsage()
        return NormalizedUsage(
            input_tokens=as_nonnegative_int(raw.get("input_tokens")),
            output_tokens=as_nonnegative_int(raw.get("output_tokens")),
            cached_tokens=as_nonnegative_int(raw.get("cached_input_tokens")),
            cache_write_tokens=as_nonnegative_int(raw.get("cache_write_input_tokens")),
            reasoning_tokens=as_nonnegative_int(raw.get("reasoning_output_tokens")),
            total_tokens=as_nonnegative_int(raw.get("total_tokens")),
        )

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        return path == root or root in path.parents

    def _discover_homes(self) -> tuple[list[Path], int]:
        candidates: list[Path | str] = [self.codex_home]
        if self.alias is None:
            store = AccountResolver._read_json(
                self.resolver.cockpit_tools_data_dir
                / COCKPIT_TOOLS_CODEX_INSTANCES_NAME
            )
            instances = store.get("instances") if isinstance(store, Mapping) else None
            if isinstance(instances, list):
                for item in instances[:1000]:
                    if not isinstance(item, Mapping):
                        continue
                    raw_home = safe_text(item.get("userDataDir"), 4096)
                    if raw_home:
                        candidates.append(raw_home)

        try:
            user_home = self.resolver.home.expanduser().resolve()
        except OSError:
            return [], len(candidates)
        homes: list[Path] = []
        seen: set[Path] = set()
        rejected = 0
        for candidate in candidates:
            try:
                raw = Path(candidate).expanduser()
                if not raw.is_absolute():
                    raise ValueError("Codex home must be absolute")
                resolved = raw.resolve()
            except (OSError, RuntimeError, ValueError):
                rejected += 1
                continue
            if not self._is_within(resolved, user_home):
                rejected += 1
                continue
            if resolved in seen:
                continue
            seen.add(resolved)
            homes.append(resolved)
        return homes, rejected

    @staticmethod
    def _auth_signature(path: Path) -> tuple[int, int, int, int] | None:
        try:
            stat = path.stat()
            if not path.is_file() or stat.st_size > 5 * 1024 * 1024:
                return None
        except OSError:
            return None
        return (int(stat.st_dev), int(stat.st_ino), int(stat.st_size), int(stat.st_mtime_ns))

    def _dynamic_home_context(self, home: Path) -> CodexAppHomeContext | None:
        auth_path = home / "auth.json"
        try:
            if not self._is_within(auth_path.resolve(), home):
                return None
        except (OSError, RuntimeError):
            return None
        before = self._auth_signature(auth_path)
        if before is None:
            return None
        (
            account,
            email,
            principal_id,
            provider_subscription_id,
            plan,
            active_until,
        ) = self._read_auth_identity(auth_path)
        after = self._auth_signature(auth_path)
        if before != after or not account:
            return None
        identity = self.resolver.resolve_auth_file(
            None,
            account,
            email,
            principal_id,
            provider_subscription_id,
        )
        normalized_email = normalize_email_identity(email)
        if normalized_email:
            binding_parts = ("email", account, normalized_email)
        elif principal_id:
            binding_parts = ("principal", account, principal_id)
        elif provider_subscription_id:
            binding_parts = ("provider", provider_subscription_id)
        else:
            binding_parts = ("workspace", account)
        binding_key = "binding:" + self.resolver._private_hash(
            "codex-app-home-binding-v1", *binding_parts
        )
        return CodexAppHomeContext(
            identity=identity,
            usage_alias=identity.usage_alias,
            default_plan=plan,
            active_until=active_until,
            binding_key=binding_key,
            auth_signature=after,
        )

    def _home_key(self, home: Path) -> str:
        return "home:" + self.resolver._private_hash(
            "codex-app-home-path-v1", str(home)
        )

    def _session_paths(self, home: Path) -> tuple[list[Path], bool]:
        try:
            sessions = (home / "sessions").resolve()
            if not self._is_within(sessions, home):
                return [], False
            if not sessions.exists():
                return [], True
            if not sessions.is_dir():
                return [], False
            ranked: list[tuple[int, Path]] = []
            for path in sessions.rglob("*.jsonl"):
                if path.is_symlink():
                    continue
                stat = path.stat()
                resolved = path.resolve()
                if (
                    not path.is_file()
                    or not self._is_within(resolved, sessions)
                    or stat.st_size > MAX_CODEX_APP_JSONL_BYTES
                ):
                    continue
                ranked.append((int(stat.st_mtime_ns), resolved))
        except OSError:
            return [], False
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [path for _, path in ranked[: self.max_files]], True

    def import_once(self) -> dict[str, int]:
        result = (
            self._fixed_import_once()
            if self.alias is not None
            else self._dynamic_import_once()
        )
        self._remember_result(result)
        return result

    def _fixed_import_once(self) -> dict[str, int]:
        assert self.alias is not None
        (
            app_account,
            app_email,
            app_principal_id,
            app_provider_subscription_id,
            default_plan,
            active_until,
        ) = self._read_auth_identity(self.codex_home / "auth.json")
        alias_identity = self.resolver.resolve(self.alias, None)
        alias_account_hash = alias_identity.account_id_hash
        self._account_match = bool(
            app_account
            and alias_account_hash
            and short_hash(app_account) == alias_account_hash
        )
        if not self._account_match:
            raise ValueError("codex app account does not match configured alias")
        app_identity = self.resolver.resolve_auth_file(
            None,
            app_account,
            app_email,
            app_principal_id,
            app_provider_subscription_id,
        )
        app_key = app_identity.subscription_id_hash
        alias_key = alias_identity.subscription_id_hash
        app_email_value = normalize_email_identity(app_email)
        alias_email_value = normalize_email_identity(alias_identity.account_email)
        if app_email_value and alias_email_value:
            member_match: bool | None = app_email_value == alias_email_value
        elif app_principal_id and alias_identity.principal_id_hash:
            member_match = (
                short_hash(app_principal_id) == alias_identity.principal_id_hash
            )
        elif app_key and alias_key and app_key == alias_key:
            member_match = True
        else:
            member_match = None
        self._member_match = member_match
        if member_match is False:
            raise ValueError("codex app member does not match configured alias")
        import_identity = (
            alias_identity
            if member_match is True
            else AccountIdentity(None, None, None)
        )
        paths, _sessions_available = self._session_paths(self.codex_home)
        file_states = self.repo.local_import_file_states(paths)
        scans = [
            self._scan_file(
                path,
                import_identity,
                self.alias,
                default_plan,
                active_until,
                prior_state,
            )
            for path, prior_state in zip(paths, file_states)
        ]
        imported, quota_rows = self._commit_scans(
            scan for scan in scans if scan.ok
        )
        return {
            "imported": imported,
            "quota_rows": quota_rows,
            "scanned_files": len(paths),
            "baselined_files": 0,
            "discovered_homes": 1,
            "matched_homes": 1,
            "failed_homes": 0,
            "rejected_homes": 0,
        }

    def _dynamic_import_once(self) -> dict[str, int]:
        self._account_match = None
        self._member_match = None
        homes, rejected = self._discover_homes()
        self.resolver.retain_codex_app_homes({self._home_key(home) for home in homes})
        imported = 0
        quota_rows = 0
        scanned_files = 0
        baselined_files = 0
        matched_homes = 0
        failed_homes = 0
        for home in homes:
            context = self._dynamic_home_context(home)
            if context is None:
                failed_homes += 1
                continue
            paths, sessions_available = self._session_paths(home)
            if not sessions_available:
                failed_homes += 1
                continue
            scanned_files += len(paths)
            home_key = self._home_key(home)
            binding = self.repo.local_import_binding(home_key)
            binding_changed = (
                binding is None
                or binding.get("binding_key") != context.binding_key
            )
            file_states = self.repo.local_import_file_states(paths)
            file_needs_baseline = [
                binding_changed or prior_state is None
                for prior_state in file_states
            ]
            scans = [
                self._scan_file(
                    path,
                    context.identity,
                    context.usage_alias,
                    context.default_plan,
                    context.active_until,
                    prior_state,
                    collect_usage=not needs_baseline,
                )
                for path, prior_state, needs_baseline in zip(
                    paths, file_states, file_needs_baseline
                )
            ]
            refreshed = self._dynamic_home_context(home)
            stable_auth = bool(
                refreshed
                and refreshed.binding_key == context.binding_key
                and refreshed.auth_signature == context.auth_signature
            )
            if not stable_auth or any(not scan.ok for scan in scans):
                failed_homes += 1
                continue
            self.resolver.register_codex_app_home(home_key, context.identity)
            admitted = self.repo.register_codex_app_subscription(
                resolved_identity_key(context.identity)
            )
            if context.identity.subscription_id_hash and not admitted:
                # A provider refresh may have sampled the inventory before
                # this home was registered. Retain cursors until its next
                # complete union admits the member instead of losing linkage.
                failed_homes += 1
                continue
            imported_delta, quota_delta = self._commit_scans(scans)
            imported += imported_delta
            quota_rows += quota_delta
            if binding_changed:
                self.repo.save_local_import_binding(home_key, context.binding_key)
            baselined_files += sum(file_needs_baseline)
            matched_homes += 1
        return {
            "imported": imported,
            "quota_rows": quota_rows,
            "scanned_files": scanned_files,
            "baselined_files": baselined_files,
            "discovered_homes": len(homes),
            "matched_homes": matched_homes,
            "failed_homes": failed_homes,
            "rejected_homes": rejected,
        }

    def _commit_scans(self, scans: Iterable[CodexAppFileScan]) -> tuple[int, int]:
        imported = 0
        quota_rows = 0
        for scan in scans:
            for event, import_key in scan.events:
                if self.repo.record_imported_event(
                    event, import_key, "codex_app_local"
                ):
                    imported += 1
            for snapshot in scan.quota_snapshots:
                quota_rows += int(
                    self.repo.insert_subscription_quota_snapshot(snapshot)
                )
            if scan.state is not None:
                self.repo.save_local_import_file_state(scan.state)
        return imported, quota_rows

    def _scan_file(
        self,
        path: Path,
        identity: AccountIdentity,
        usage_alias: str | None,
        default_plan: str | None,
        active_until: str | None,
        prior_state: Mapping[str, Any] | None,
        *,
        collect_usage: bool = True,
    ) -> CodexAppFileScan:
        canonical_identity = bool(identity.subscription_id_hash)
        identity_key_value = (
            f"subscription:{identity.subscription_id_hash}"
            if canonical_identity
            else "unknown"
        )
        model_provider: str | None = None
        model: str | None = None
        events: list[tuple[UsageEvent, str]] = []
        quota_snapshots: list[dict[str, Any]] = []
        record_index = 0
        resolved_path = path.resolve()
        try:
            stat = path.stat()
            state = dict(prior_state or {})
            unchanged = (
                int(state.get("size") or -1) == int(stat.st_size)
                and int(state.get("mtime_ns") or -1) == int(stat.st_mtime_ns)
                and int(state.get("offset") or -1) == int(stat.st_size)
            )
            if unchanged:
                return CodexAppFileScan(True, None, events, quota_snapshots)
            can_resume = (
                state
                and int(state.get("offset") or 0) > 0
                and int(stat.st_size) >= int(state.get("offset") or 0)
                and int(state.get("size") or 0) <= int(stat.st_size)
            )
            start_offset = int(state.get("offset") or 0) if can_resume else 0
            if can_resume:
                model_provider = safe_text(state.get("model_provider"), 64)
                model = safe_text(state.get("model"), 200)
            handle = path.open("r", encoding="utf-8", errors="replace")
            if start_offset:
                handle.seek(start_offset)
        except OSError:
            return CodexAppFileScan(False, None, events, quota_snapshots)
        with handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(record, Mapping):
                    continue
                record_type = record.get("type")
                payload = record.get("payload")
                if not isinstance(payload, Mapping):
                    continue
                if record_type == "session_meta":
                    model_provider = safe_text(payload.get("model_provider"), 64)
                    continue
                if record_type == "turn_context":
                    model = safe_model_identifier(payload.get("model")) or model
                    continue
                if record_type == "event_msg" and payload.get("type") == "task_started":
                    continue
                if record_type != "event_msg" or payload.get("type") != "token_count":
                    continue
                record_index += 1
                if not collect_usage or (model_provider or "").lower() != "openai":
                    continue
                usage = self._usage_from_token_count(payload)
                if usage.missing:
                    continue
                timestamp = normalize_timestamp(record.get("timestamp"))
                components = self.repo.price_components_for(model, usage)
                ordinal = record.get("ordinal")
                ordinal_key = (
                    str(ordinal) if ordinal is not None else f"line-{record_index}"
                )
                import_key = "codex-app:" + (
                    short_hash(f"{resolved_path}\n{ordinal_key}\n{timestamp}")
                    or short_hash(f"{resolved_path}\n{record_index}")
                    or "unknown"
                )
                events.append(
                    (
                        UsageEvent(
                            ts=timestamp,
                            identity_key=identity_key_value,
                            endpoint="local://chatgpt-codex",
                            method="LOCAL",
                            model=model,
                            status_code=200,
                            ok=1,
                            duration_ms=0,
                            stream=0,
                            session_id=None,
                            thread_id=None,
                            turn_id=None,
                            installation_id=None,
                            window_id=None,
                            usage_alias=usage_alias,
                            usage_project=None,
                            auth_fingerprint=None,
                            account_id_hash=identity.account_id_hash,
                            account_id_tail=identity.account_id_tail,
                            input_tokens=usage.input_tokens,
                            output_tokens=usage.output_tokens,
                            cached_tokens=usage.cached_tokens,
                            cache_write_tokens=usage.cache_write_tokens,
                            reasoning_tokens=usage.reasoning_tokens,
                            total_tokens=usage.total_tokens,
                            estimated_api_cost_usd=(
                                components.total_cost_usd if components else None
                            ),
                            non_cached_input_cost_usd=(
                                components.non_cached_input_cost_usd
                                if components
                                else None
                            ),
                            cached_input_cost_usd=(
                                components.cached_input_cost_usd
                                if components
                                else None
                            ),
                            output_cost_usd=(
                                components.output_cost_usd if components else None
                            ),
                            long_context_pricing_applied=int(
                                components.long_context_pricing_applied
                                if components
                                else False
                            ),
                            subscription_amortized_cost_usd=None,
                            api_equivalent_quota_usd=None,
                            usage_missing=0,
                            error_type=None,
                            error_message_redacted=None,
                            request_bytes=0,
                            response_bytes=0,
                            source="codex_app_local",
                            request_id=None,
                        ),
                        import_key,
                    )
                )
                rate_limits = payload.get("rate_limits")
                plan = (
                    safe_plan_type(rate_limits.get("plan_type"))
                    if isinstance(rate_limits, Mapping)
                    else None
                ) or default_plan
                if not canonical_identity:
                    continue
                for window in parse_codex_app_rate_windows(rate_limits, timestamp):
                    window.update(
                        {
                            "identity_key": identity_key_value,
                            "plan_type": plan,
                            "subscription_active_until": active_until,
                            "source": "codex_app_local",
                        }
                    )
                    quota_snapshots.append(window)
            final_offset = handle.tell()
        try:
            final_stat = path.stat()
        except OSError:
            return CodexAppFileScan(False, None, [], [])
        return CodexAppFileScan(
            True,
            {
                "path": resolved_path,
                "size": final_stat.st_size,
                "mtime_ns": final_stat.st_mtime_ns,
                "offset": min(final_offset, final_stat.st_size),
                "model_provider": model_provider,
                "model": model,
            },
            events,
            quota_snapshots,
        )


class CodexQuotaPoller:
    """Low-frequency, read-only Codex subscription quota snapshots via 8317."""

    def __init__(
        self,
        repo: UsageRepository,
        resolver: AccountResolver,
        upstream,
        *,
        key_loader,
        poll_seconds: float = DEFAULT_QUOTA_POLL_SECONDS,
        timeout: float = DEFAULT_QUOTA_POLL_TIMEOUT,
    ) -> None:
        self.repo = repo
        self.resolver = resolver
        self.upstream = upstream
        self.key_loader = key_loader
        self.poll_seconds = max(60.0, float(poll_seconds))
        self.timeout = max(3.0, float(timeout))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_poll_at: str | None = None
        self._last_success_at: str | None = None
        self._last_error_type: str | None = None
        self._account_count = 0
        self._window_count = 0
        self._inventory_result: dict[str, Any] = {
            "authoritative": False,
            "active": 0,
            "suspect": 0,
            "retired": 0,
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="cliproxy-codex-quota", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._thread = None

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.key_loader()),
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error_type": self._last_error_type,
            "account_count": self._account_count,
            "window_count": self._window_count,
            "inventory": {
                key: value
                for key, value in self._inventory_result.items()
                if key != "retired_keys"
            },
            "poll_seconds": self.poll_seconds,
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            key = self.key_loader()
            if not key:
                self._stop.wait(30.0)
                continue
            try:
                accounts, windows = self.poll_once(key)
                self._last_poll_at = utc_now()
                self._last_success_at = self._last_poll_at
                self._last_error_type = None
                self._account_count = accounts
                self._window_count = windows
            except Exception as exc:
                self._last_poll_at = utc_now()
                self._last_error_type = type(exc).__name__
                LOG.warning("Codex quota snapshot failed: %s", type(exc).__name__)
            self._stop.wait(self.poll_seconds)

    def _management_request(
        self,
        key: str,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> tuple[int, Any]:
        port = self.upstream.port or (443 if self.upstream.scheme == "https" else 80)
        if self.upstream.scheme == "https":
            connection: http.client.HTTPConnection = http.client.HTTPSConnection(
                self.upstream.hostname,
                port,
                timeout=self.timeout,
                context=ssl.create_default_context(),
            )
        else:
            connection = http.client.HTTPConnection(self.upstream.hostname, port, timeout=self.timeout)
        body = json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None
        headers = {
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "Connection": "close",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise ValueError("management response too large")
            try:
                parsed = json.loads(raw) if raw else None
            except (json.JSONDecodeError, UnicodeDecodeError):
                parsed = None
            return response.status, parsed
        finally:
            connection.close()

    def poll_once(self, key: str) -> tuple[int, int]:
        status, auth_payload = self._management_request(key, "GET", "/v0/management/auth-files")
        if status != 200 or not isinstance(auth_payload, Mapping):
            raise RuntimeError(f"auth_files_http_{status}")
        if "files" not in auth_payload or not isinstance(auth_payload.get("files"), list):
            raise ValueError("auth_files_inventory_incomplete")
        files = auth_payload["files"]
        # Deletion decisions require the complete inventory.  Be conservative
        # with management builds that expose pagination metadata even though
        # the common endpoint currently returns one list.
        next_page = first_present(
            auth_payload,
            ("next", "next_page", "nextPage", "next_cursor", "nextCursor"),
        )
        total_items = as_nonnegative_int(
            first_present(auth_payload, ("total", "total_count", "totalCount"))
        )
        pagination_incomplete = next_page not in (None, "", False, 0)
        if pagination_value_indicates_more(
            auth_payload.get("has_more")
        ) or pagination_value_indicates_more(auth_payload.get("hasMore")):
            pagination_incomplete = True
        if total_items is not None and total_items > len(files):
            pagination_incomplete = True
        fetched_at = utc_now()
        account_count = 0
        window_count = 0
        authoritative = not pagination_incomplete
        present_keys: set[str] = set()
        candidates: list[dict[str, Any]] = []
        # A forced scan prevents a just-deleted or just-written local auth file
        # from being hidden behind the resolver's normal 60-second cache.
        resolver_present_keys = self.resolver.active_subscription_keys(
            force_refresh=True
        )
        cockpit_inventory = self.resolver.cockpit_inventory()
        if (
            cockpit_inventory.get("detected")
            and not cockpit_inventory.get("authoritative")
        ):
            authoritative = False
        for item in files:
            if not isinstance(item, Mapping):
                authoritative = False
                continue
            provider = safe_text(item.get("provider") or item.get("type"), 64)
            if not provider:
                authoritative = False
                continue
            if provider.lower() != "codex":
                continue
            auth_names = [
                value
                for raw in (
                    item.get("name"),
                    item.get("id"),
                    item.get("filename"),
                    item.get("file_name"),
                )
                if (value := safe_text(raw, 512))
            ]
            auth_index = safe_text(item.get("auth_index") or item.get("authIndex"), 512)
            parsed_item = dict(item)
            # Older management responses used ``account`` for an id, while
            # current OAuth responses use it for an email.  Never send an
            # email as Chatgpt-Account-Id.
            legacy_account = safe_text(item.get("account"), 256)
            if legacy_account and "@" in legacy_account:
                if not parsed_item.get("email"):
                    parsed_item["email"] = legacy_account
            elif legacy_account and not parsed_item.get("account_id"):
                parsed_item["account_id"] = legacy_account
            (
                account_id,
                _access_tokens,
                account_email,
                principal_id,
                provider_subscription_id,
            ) = self.resolver._account_and_tokens(parsed_item)
            identity = AccountIdentity(None, None, None)
            known_auth_names = list(
                dict.fromkeys(
                    name
                    for name in auth_names
                    if self.resolver.auth_file_known(name)
                )
            )
            if known_auth_names:
                resolved_known: list[AccountIdentity] = []
                exact_conflict = False
                for auth_name in known_auth_names:
                    resolved = self.resolver.resolve_auth_file(
                        auth_name,
                        account_id,
                        account_email,
                        principal_id,
                        provider_subscription_id,
                    )
                    if not resolved.subscription_id_hash:
                        exact_conflict = True
                        break
                    resolved_known.append(resolved)
                resolved_keys = {
                    resolved.subscription_id_hash for resolved in resolved_known
                }
                if exact_conflict or len(resolved_keys) != 1:
                    authoritative = False
                    continue
                identity = resolved_known[0]
            else:
                # A management-only structured identity is an acceptable
                # fallback only when none of its advertised filenames exists
                # locally.  Exact local conflicts above are fail-closed.
                identity = self.resolver.resolve_auth_file(
                    None,
                    account_id,
                    account_email,
                    principal_id,
                    provider_subscription_id,
                )
            if not identity.subscription_id_hash:
                authoritative = False
                continue
            persisted_key = f"subscription:{identity.subscription_id_hash}"
            present_keys.add(persisted_key)
            if item.get("disabled") is True:
                continue
            if not auth_index or not account_id:
                authoritative = False
                continue
            raw_claims = item.get("id_token")
            claims = (
                raw_claims
                if isinstance(raw_claims, Mapping)
                else _decode_jwt_claims_unverified(raw_claims)
            )
            candidates.append(
                {
                    "auth_index": auth_index,
                    "account_id": account_id,
                    "claims": claims,
                    "identity": identity,
                    "persisted_key": persisted_key,
                }
            )

        # A migration can temporarily split the authoritative inventory
        # across CLIProxyAPI and Cockpit Tools.  Reconcile only their union so
        # neither collector can retire identities owned by the other.
        present_keys.update(resolver_present_keys)
        self._inventory_result = self.repo.reconcile_subscription_inventory(
            present_keys,
            fetched_at,
            authoritative=authoritative,
        )
        seen_subscriptions: set[str] = set()
        for candidate in candidates:
            auth_index = candidate["auth_index"]
            account_id = candidate["account_id"]
            claims = candidate["claims"]
            identity = candidate["identity"]
            persisted_key = candidate["persisted_key"]
            if persisted_key in seen_subscriptions:
                continue
            usage_alias = identity.usage_alias
            headers = {
                "Authorization": "Bearer $TOKEN$",
                "Content-Type": "application/json",
                "User-Agent": "codex_cli_rs/0.76.0 (Darwin; arm64)",
                "Chatgpt-Account-Id": account_id,
            }
            call = {
                "auth_index": auth_index,
                "method": "GET",
                "url": CODEX_USAGE_URL,
                "header": headers,
            }
            outer_status, outer = self._management_request(
                key, "POST", "/v0/management/api-call", call
            )
            if outer_status != 200 or not isinstance(outer, Mapping):
                continue
            upstream_status = as_nonnegative_int(outer.get("status_code"))
            if upstream_status != 200:
                continue
            raw_body = outer.get("body")
            try:
                usage_payload = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
            except json.JSONDecodeError:
                continue
            if not isinstance(usage_payload, Mapping):
                continue
            auth_claims = claims.get("https://api.openai.com/auth")
            auth_claims = auth_claims if isinstance(auth_claims, Mapping) else {}
            plan = safe_text(
                usage_payload.get("plan_type")
                or usage_payload.get("planType")
                or claims.get("plan_type")
                or auth_claims.get("plan_type"),
                64,
            )
            active_until = (
                claims.get("chatgpt_subscription_active_until")
                or auth_claims.get("chatgpt_subscription_active_until")
            )
            windows = parse_codex_quota_windows(usage_payload, fetched_at)
            if not windows:
                continue
            seen_subscriptions.add(persisted_key)
            account_count += 1
            for window in windows:
                window.update(
                    {
                        "identity_key": persisted_key,
                        "account_id_hash": identity.account_id_hash,
                        "account_id_tail": identity.account_id_tail,
                        "usage_alias": usage_alias,
                        "plan_type": plan,
                        "subscription_active_until": active_until,
                        "source": "cliproxy_wham_usage",
                    }
                )
                self.repo.insert_subscription_quota_snapshot(window)
                window_count += 1
        return account_count, window_count


class UsageQueuePoller:
    """Safely drain CLIProxyAPI's local management usage queue.

    The poller is opt-in: without a key file or explicitly named environment
    variable it does nothing.  Authentication failures are backed off instead
    of retried in a tight loop, which protects CLIProxyAPI's five-failure IP
    ban from being triggered by a misconfigured sidecar.
    """

    def __init__(
        self,
        repo: UsageRepository,
        resolver: AccountResolver,
        upstream,
        *,
        key_file: str | None = None,
        key_env: str = "CLIPROXY_MANAGEMENT_KEY",
        queue_path: str = DEFAULT_USAGE_QUEUE_PATH,
        count: int = DEFAULT_USAGE_QUEUE_COUNT,
        poll_seconds: float = DEFAULT_USAGE_QUEUE_POLL_SECONDS,
        timeout: float = 10.0,
        quota_guard: QuotaRoutingGuard | None = None,
    ) -> None:
        self.repo = repo
        self.resolver = resolver
        self.upstream = upstream
        self.key_file = Path(key_file).expanduser() if key_file else None
        self.key_env = safe_text(key_env, 128) if key_env is not None else "CLIPROXY_MANAGEMENT_KEY"
        self.queue_path = queue_path if queue_path.startswith("/") else "/" + queue_path
        self.count = max(1, min(int(count), 1000))
        self.poll_seconds = max(0.5, float(poll_seconds))
        self.timeout = max(1.0, float(timeout))
        self.quota_guard = quota_guard
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._warned_no_key = False
        self._warned_permissions = False
        self._backoff = self.poll_seconds
        self._last_status: int | None = None
        self._last_success_at: str | None = None
        self._last_error_type: str | None = None
        self._last_poll_at: str | None = None
        self._accepted = 0
        self._skipped = 0

    @property
    def configured(self) -> bool:
        return bool(self.key_file or (self.key_env and os.environ.get(self.key_env, "").strip()))

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="cliproxy-usage-queue", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._thread = None

    def status(self) -> dict[str, Any]:
        key_loaded = bool(self._load_key())
        env_configured = bool(self.key_env and os.environ.get(self.key_env, "").strip())
        configured = bool(self.key_file or env_configured)
        return {
            "enabled": key_loaded,
            "configured": configured,
            "key_source": "file" if self.key_file else ("env" if env_configured else "none"),
            "key_loaded": key_loaded,
            "last_status": self._last_status,
            "last_poll_at": self._last_poll_at,
            "last_success_at": self._last_success_at,
            "last_error_type": self._last_error_type,
            "accepted": self._accepted,
            "skipped": self._skipped,
            "backoff_seconds": self._backoff,
            "quota_routing_guard": (
                self.quota_guard.status()
                if self.quota_guard is not None
                else {"enabled": False, "active_locks": 0}
            ),
        }

    def _load_key(self) -> str | None:
        if self.key_file:
            try:
                file_stat = self.key_file.stat()
                mode = file_stat.st_mode & 0o777
                if mode & 0o077:
                    if not self._warned_permissions:
                        LOG.error("management key file must be owner-only (mode 600): %s", self.key_file)
                        self._warned_permissions = True
                    return None
                if file_stat.st_size > 4096:
                    return None
                with self.key_file.open("r", encoding="utf-8") as handle:
                    value = handle.read(4097).strip()
            except (OSError, UnicodeError):
                value = ""
            if value:
                return value if len(value) <= 4096 else None
        value = os.environ.get(self.key_env, "").strip() if self.key_env else ""
        return value if value and len(value) <= 4096 else None

    def _run(self) -> None:
        while not self._stop.is_set():
            key = self._load_key()
            if not key:
                if not self._warned_no_key:
                    LOG.info("usage queue poller idle: no management key file/environment configured")
                    self._warned_no_key = True
                self._stop.wait(30.0)
                continue
            self._warned_no_key = False
            try:
                status, body, retry_after = self._poll_once(key)
                self._last_status = status
                self._last_poll_at = utc_now()
                self._last_error_type = None if status == 200 else f"http_{status}"
                if status == 200:
                    self._last_success_at = self._last_poll_at
                    self._backoff = self.poll_seconds
                    if self.quota_guard is not None:
                        self.quota_guard.reconcile(key)
                elif status in {401, 403}:
                    # 403 is also the response used during CLIProxyAPI's IP
                    # ban; use a long backoff and never inspect/log the body.
                    self._backoff = MAX_MANAGEMENT_BACKOFF_SECONDS if status == 403 else DEFAULT_MANAGEMENT_BACKOFF_SECONDS
                elif status == 429:
                    self._backoff = min(max(retry_after or DEFAULT_MANAGEMENT_BACKOFF_SECONDS, self.poll_seconds), MAX_MANAGEMENT_BACKOFF_SECONDS)
                else:
                    self._backoff = min(max(self._backoff * 2, self.poll_seconds), MAX_MANAGEMENT_BACKOFF_SECONDS)
            except Exception as exc:
                self._last_poll_at = utc_now()
                self._last_error_type = type(exc).__name__
                self._backoff = min(max(self._backoff * 2, 5.0), MAX_MANAGEMENT_BACKOFF_SECONDS)
                LOG.warning("usage queue poll failed: %s", type(exc).__name__)
            self._stop.wait(self._backoff)

    def _poll_once(self, key: str) -> tuple[int, bytes, float | None]:
        parsed = self.upstream
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        connection: http.client.HTTPConnection
        if parsed.scheme == "https":
            connection = http.client.HTTPSConnection(
                parsed.hostname, port, timeout=self.timeout, context=ssl.create_default_context()
            )
        else:
            connection = http.client.HTTPConnection(parsed.hostname, port, timeout=self.timeout)
        target = self.queue_path + f"?count={self.count}"
        try:
            connection.request(
                "GET",
                target,
                headers={
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            body = response.read(4 * 1024 * 1024 + 1)
            status = response.status
            retry_after = None
            raw_retry = response.getheader("Retry-After")
            if raw_retry:
                try:
                    retry_after = float(raw_retry)
                except ValueError:
                    retry_after = None
            if status == 200:
                self._consume(body, key)
            return status, body, retry_after
        finally:
            connection.close()

    def _consume(self, body: bytes, key: str) -> None:
        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            LOG.warning("usage queue returned invalid JSON; records skipped")
            return
        if not isinstance(payload, list):
            LOG.warning("usage queue returned a non-array payload; records skipped")
            return
        accepted = 0
        skipped = 0
        for item in payload:
            converted = queue_record_event(item, self.resolver, self.repo) if isinstance(item, Mapping) else None
            if converted is None:
                skipped += 1
                continue
            event, info = converted
            try:
                self.repo.record_event(event, info, source="usage_queue")
                if self.quota_guard is not None:
                    self.quota_guard.observe_record(item, event, key)
                accepted += 1
            except Exception as exc:
                skipped += 1
                LOG.error("usage queue event persistence failed: %s", type(exc).__name__)
        if accepted or skipped:
            self._accepted += accepted
            self._skipped += skipped
            LOG.info("usage queue drained: accepted=%d skipped=%d", accepted, skipped)


def display_identity(row: Mapping[str, Any]) -> str:
    if row.get("legacy_ambiguous"):
        return "legacy workspace"
    if row.get("usage_alias"):
        return str(row["usage_alias"])
    if row.get("account_email"):
        return str(row["account_email"])
    if str(row.get("identity_key") or "").startswith("subscription:"):
        return "订阅账号"
    return "unknown"


def dashboard_identity(row: Mapping[str, Any]) -> str:
    """Return the dashboard's email-only account label.

    Aliases remain authoritative internal routing keys, but they are local
    implementation details and should not be presented as account identity on
    ``/usage``.  Do not fall back to an alias when an email is unavailable.
    """

    return safe_email(row.get("account_email")) or "邮箱未获取"


def identity_badge(row: Mapping[str, Any]) -> tuple[str, str]:
    """Return a meaningful one-letter account class for the dashboard.

    C means the account is attached to a named local ``codex-N`` home/alias.
    A means it is only identified from an auth/account mapping (anonymous
    fallback or queue-derived identity), so it has no stable local alias.
    """

    if row.get("legacy_ambiguous"):
        return "L", "L = Legacy ambiguous（历史记录无法安全拆分）"
    windows = row.get("windows")
    if isinstance(windows, Mapping) and any(
        isinstance(window, Mapping)
        and window.get("source") in {SUB2API_ACCOUNT_SOURCE, SUB2API_QUOTA_SOURCE}
        for window in windows.values()
    ):
        return "S", "S = Sub2API account（由本机 Sub2API 只读同步）"
    alias = safe_text(row.get("usage_alias"), 128) or ""
    if re.fullmatch(r"codex-\d+", alias, re.IGNORECASE):
        return "C", "C = Codex alias（已映射本机 CODEX_HOME）"
    return "A", "A = Auth account（仅凭账号身份识别，未绑定本机 alias）"


def fmt_money(value: Any) -> str:
    if value is None:
        return "—"
    number = float(value)
    return f"${number:,.2f}" if abs(number) >= 0.01 else f"${number:,.6f}"


def fmt_rate_per_million(cost: Any, tokens: Any) -> str:
    token_count = int(tokens or 0)
    if cost is None or token_count <= 0:
        return "—"
    rate = float(cost) * 1_000_000 / token_count
    return f"${rate:,.4f}/M" if abs(rate) < 0.1 else f"${rate:,.2f}/M"


def fmt_int(value: Any) -> str:
    return f"{int(value or 0):,}"


def fmt_compact(value: Any) -> str:
    number = float(value or 0)
    for divisor, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if abs(number) >= divisor:
            return f"{number / divisor:.2f}{suffix}"
    return f"{int(number):,}"


def fmt_percent(value: Any) -> str:
    if value is None:
        return "待获取"
    number = float(value)
    return f"{number:.0f}%" if number.is_integer() else f"{number:.1f}%"


def fmt_ratio(numerator: Any, denominator: Any) -> str:
    total = int(denominator or 0)
    if total <= 0:
        return "—"
    return f"{int(numerator or 0) / total * 100:.1f}%"


def fmt_local_time(value: Any, *, seconds: bool = False) -> str:
    text = safe_text(value, 128)
    if not text:
        return "—"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    pattern = "%m-%d %H:%M:%S" if seconds else "%m-%d %H:%M"
    return parsed.astimezone().strftime(pattern)


RESPONSE_TIMELINE_CSS = """
.response-timeline-panel{overflow:hidden;background-color:var(--card);background-image:radial-gradient(var(--dot) 1px,transparent 1px);background-size:16px 16px}
.timeline-toolbar{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:18px;align-items:center;padding:17px 18px;border-bottom:2px solid var(--ink);background:color-mix(in srgb,var(--sky) 24%,var(--card))}
.timeline-kpis{display:flex;min-width:0;flex-wrap:wrap;gap:9px}.timeline-kpi{display:grid;grid-template-columns:11px auto;grid-template-rows:auto auto;column-gap:8px;min-width:122px;padding:8px 10px;border:1.5px solid var(--ink);border-radius:10px;background:var(--card);box-shadow:2px 2px 0 var(--shadow-ink)}.timeline-kpi i{grid-row:1/3;align-self:center;width:10px;height:10px;border:1.5px solid var(--ink);border-radius:50%}.timeline-kpi span{color:var(--ink-3);font:800 8px/1.1 var(--font-mono);letter-spacing:.07em;text-transform:uppercase}.timeline-kpi strong{margin-top:3px;font:900 15px/1 var(--font-display)}.timeline-kpi.ok i{background:var(--mint)}.timeline-kpi.bad i{background:var(--rose)}.timeline-kpi.rate i{background:var(--lavender)}.timeline-kpi.last i{background:var(--sun)}
.timeline-controls{display:flex;align-items:center;justify-content:flex-end;gap:9px}.timeline-range{display:inline-flex;padding:3px;border:1.5px solid var(--ink);border-radius:10px;background:var(--paper);box-shadow:2px 2px 0 var(--shadow-ink)}.timeline-range button{min-width:47px;padding:7px 9px;border:0;border-radius:7px;background:transparent;color:var(--ink-2);font:800 9px/1 var(--font-mono);cursor:pointer}.timeline-range button:hover{background:color-mix(in srgb,var(--sky) 30%,transparent);color:var(--ink)}.timeline-range button.is-active{background:var(--ink);color:var(--paper)}
.timeline-chart-wrap{position:relative;padding:12px 18px 0}.timeline-chart-scroll{overflow-x:auto;overflow-y:hidden;scrollbar-width:thin}.response-timeline-chart{display:block;width:100%;min-width:720px;height:auto;aspect-ratio:10/3;overflow:visible}.response-timeline-chart [hidden]{display:none}.timeline-plot-bg{fill:color-mix(in srgb,var(--paper) 76%,transparent);stroke:color-mix(in srgb,var(--ink) 45%,transparent);stroke-width:1;vector-effect:non-scaling-stroke}.timeline-grid-line{stroke:color-mix(in srgb,var(--ink) 20%,transparent);stroke-width:1;stroke-dasharray:4 6;vector-effect:non-scaling-stroke}.timeline-grid-line.vertical{stroke-dasharray:2 8}.timeline-axis-label{fill:var(--ink-3);font:700 20px/1 var(--font-mono)}.timeline-line{fill:none;stroke-linecap:round;stroke-linejoin:round;vector-effect:non-scaling-stroke;transition:.18s opacity}.timeline-line-200{stroke:var(--mint);stroke-width:4}.timeline-line-non-200{stroke:var(--orange);stroke-width:3.5;stroke-dasharray:8 5}.timeline-error-area{fill:url(#timeline-error-gradient);opacity:.42}.timeline-crosshair{stroke:var(--ink);stroke-width:1.5;stroke-dasharray:4 4;vector-effect:non-scaling-stroke;pointer-events:none}.timeline-marker{stroke:var(--ink);stroke-width:2.5;vector-effect:non-scaling-stroke;pointer-events:none}.timeline-marker.ok{fill:var(--mint)}.timeline-marker.bad{fill:var(--orange)}.timeline-hit-area{fill:transparent;cursor:crosshair}.timeline-empty{fill:var(--ink-3);font:900 24px/1 var(--font-display);text-anchor:middle}.response-timeline-panel.is-loading .timeline-line{opacity:.48}
.timeline-tooltip{position:absolute;z-index:6;width:190px;padding:11px 12px;border:var(--border);border-radius:10px;background:var(--card);box-shadow:var(--shadow-sm);pointer-events:none}.timeline-tooltip strong{display:block;margin-bottom:8px;font:900 12px/1.2 var(--font-display)}.timeline-tooltip div{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:6px;color:var(--ink-2);font:700 10px/1.2 var(--font-mono)}.timeline-tooltip span{display:flex;align-items:center;gap:6px}.timeline-tooltip i{width:8px;height:8px;border:1px solid var(--ink);border-radius:50%}.timeline-tooltip i.ok{background:var(--mint)}.timeline-tooltip i.bad{background:var(--orange)}.timeline-tooltip b{color:var(--ink)}.timeline-tooltip small{display:block;margin-top:8px;padding-top:7px;border-top:1px solid color-mix(in srgb,var(--ink) 28%,transparent);color:var(--ink-3);font:700 9px/1.3 var(--font-mono)}
.timeline-meta{display:flex;min-width:0;align-items:center;justify-content:space-between;gap:16px;padding:9px 18px 14px}.timeline-legend{display:flex;flex-wrap:wrap;gap:12px 18px;color:var(--ink-2);font:700 10px/1.2 var(--font-mono)}.timeline-legend span{display:flex;align-items:center;gap:7px}.timeline-legend i{width:22px;height:4px;border:1px solid var(--ink);border-radius:999px}.timeline-legend .ok{background:var(--mint)}.timeline-legend .bad{height:3px;background:repeating-linear-gradient(90deg,var(--orange) 0 7px,transparent 7px 11px)}.timeline-sync{min-width:0;color:var(--ink-3);font:700 9px/1.3 var(--font-mono);text-align:right;overflow-wrap:anywhere}.timeline-footnote{padding:11px 18px;border-top:1.5px solid color-mix(in srgb,var(--ink) 42%,transparent);background:color-mix(in srgb,var(--watch) 42%,var(--card));color:var(--ink-2);font-size:11px}.timeline-footnote code{padding:2px 5px;border:1px solid color-mix(in srgb,var(--ink) 42%,transparent);border-radius:5px;background:var(--paper);color:var(--ink);font:700 9px/1 var(--font-mono)}
@media(max-width:920px){.timeline-toolbar{grid-template-columns:minmax(0,1fr)}.timeline-controls{justify-content:flex-start}}
@media(max-width:620px){.timeline-toolbar{padding:14px}.timeline-kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}.timeline-kpi{min-width:0}.timeline-controls{align-items:stretch}.timeline-range{display:grid;grid-template-columns:repeat(3,1fr);width:100%}.timeline-chart-wrap{padding-left:10px;padding-right:10px}.timeline-meta{align-items:flex-start;flex-direction:column}.timeline-sync{text-align:left}}
"""


RESPONSE_TIMELINE_SCRIPT = """
(() => {
  const root = document.querySelector('[data-role="response-timeline"]');
  const bootstrapNode = document.getElementById('response-timeline-data');
  if (!root || !bootstrapNode) return;

  const svg = root.querySelector('[data-role="timeline-svg"]');
  const grid = root.querySelector('[data-role="timeline-grid"]');
  const line200 = root.querySelector('[data-role="timeline-line-200"]');
  const lineNon200 = root.querySelector('[data-role="timeline-line-non-200"]');
  const errorArea = root.querySelector('[data-role="timeline-error-area"]');
  const crosshair = root.querySelector('[data-role="timeline-crosshair"]');
  const marker200 = root.querySelector('[data-role="timeline-marker-200"]');
  const markerNon200 = root.querySelector('[data-role="timeline-marker-non-200"]');
  const hitArea = root.querySelector('[data-role="timeline-hit-area"]');
  const emptyState = root.querySelector('[data-role="timeline-empty"]');
  const tooltip = root.querySelector('[data-role="timeline-tooltip"]');
  const chartWrap = root.querySelector('.timeline-chart-wrap');
  const syncLabel = root.querySelector('[data-role="timeline-sync"]');
  if (!svg || !grid || !line200 || !lineNon200 || !errorArea || !hitArea || !tooltip) return;

  const NS = 'http://www.w3.org/2000/svg';
  const layout = {width: 1200, height: 360, left: 62, right: 1170, top: 28, bottom: 294};
  const numberFormat = new Intl.NumberFormat('zh-CN');
  const timeFormat = new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false
  });
  const compactTimeFormat = new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit', minute: '2-digit', hour12: false
  });
  let payload = null;
  let points = [];
  let activeIndex = -1;
  let requestController = null;
  let resizeTimer = null;

  function roleText(role, value) {
    const node = root.querySelector('[data-role="' + role + '"]');
    if (node) node.textContent = value;
  }

  function count(value) {
    return numberFormat.format(Number(value) || 0);
  }

  function localTime(value, compact) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return (compact ? compactTimeFormat : timeFormat).format(date).replace(/\//g, '-');
  }

  function niceMaximum(raw) {
    const value = Math.max(0, Math.ceil(raw));
    if (value <= 4) return Math.max(value, 1);
    const magnitude = Math.pow(10, Math.floor(Math.log10(value)));
    const normalized = value / magnitude;
    const factor = normalized <= 2 ? 2 : (normalized <= 5 ? 5 : 10);
    return factor * magnitude;
  }

  function xFor(index) {
    if (points.length <= 1) return (layout.left + layout.right) / 2;
    return layout.left + index / (points.length - 1) * (layout.right - layout.left);
  }

  function yFor(value, maximum) {
    return layout.bottom - (Number(value) || 0) / maximum * (layout.bottom - layout.top);
  }

  function linePath(key, maximum) {
    return points.map((point, index) => {
      const prefix = index === 0 ? 'M' : 'L';
      return prefix + xFor(index).toFixed(2) + ' ' + yFor(point[key], maximum).toFixed(2);
    }).join(' ');
  }

  function areaPath(key, maximum) {
    if (!points.length) return '';
    const body = points.map((point, index) => {
      return 'L' + xFor(index).toFixed(2) + ' ' + yFor(point[key], maximum).toFixed(2);
    }).join(' ');
    return 'M' + xFor(0).toFixed(2) + ' ' + layout.bottom + ' ' + body +
      ' L' + xFor(points.length - 1).toFixed(2) + ' ' + layout.bottom + ' Z';
  }

  function svgElement(name, attributes, text) {
    const node = document.createElementNS(NS, name);
    Object.entries(attributes || {}).forEach(([key, value]) => node.setAttribute(key, String(value)));
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function renderGrid(maximum) {
    grid.replaceChildren();
    const tickCount = Math.min(4, maximum);
    const values = [];
    for (let index = 0; index <= tickCount; index += 1) {
      values.push(Math.round(maximum * index / tickCount));
    }
    [...new Set(values)].forEach((value) => {
      const y = yFor(value, maximum);
      grid.append(
        svgElement('line', {
          class: 'timeline-grid-line', x1: layout.left, x2: layout.right, y1: y, y2: y
        }),
        svgElement('text', {
          class: 'timeline-axis-label', x: layout.left - 13, y: y + 7, 'text-anchor': 'end'
        }, count(value))
      );
    });

    const labelCount = root.clientWidth < 620 ? 4 : 6;
    const seen = new Set();
    for (let tick = 0; tick < labelCount; tick += 1) {
      const index = Math.round(tick / (labelCount - 1) * (points.length - 1));
      if (seen.has(index) || !points[index]) continue;
      seen.add(index);
      const x = xFor(index);
      const anchor = tick === 0 ? 'start' : (tick === labelCount - 1 ? 'end' : 'middle');
      grid.append(
        svgElement('line', {
          class: 'timeline-grid-line vertical', x1: x, x2: x, y1: layout.top, y2: layout.bottom
        }),
        svgElement('text', {
          class: 'timeline-axis-label', x: x, y: layout.bottom + 31, 'text-anchor': anchor
        }, localTime(points[index].ts, Number(payload.window_minutes) <= 360))
      );
    }
  }

  function hideTooltip() {
    tooltip.hidden = true;
    crosshair.setAttribute('hidden', '');
    marker200.setAttribute('hidden', '');
    markerNon200.setAttribute('hidden', '');
    activeIndex = -1;
  }

  function showPoint(index) {
    if (!points[index]) return;
    activeIndex = index;
    const point = points[index];
    const maximum = Number(svg.dataset.maximum) || 1;
    const x = xFor(index);
    const y200 = yFor(point.status_200, maximum);
    const yNon200 = yFor(point.status_non_200, maximum);
    crosshair.setAttribute('x1', x);
    crosshair.setAttribute('x2', x);
    marker200.setAttribute('cx', x);
    marker200.setAttribute('cy', y200);
    markerNon200.setAttribute('cx', x);
    markerNon200.setAttribute('cy', yNon200);
    crosshair.removeAttribute('hidden');
    marker200.removeAttribute('hidden');
    markerNon200.removeAttribute('hidden');
    tooltip.hidden = false;
    roleText('timeline-tooltip-time', localTime(point.ts, false));
    roleText('timeline-tooltip-200', count(point.status_200));
    roleText('timeline-tooltip-non-200', count(point.status_non_200));
    roleText('timeline-tooltip-total', '该分钟共 ' + count(
      Number(point.status_200) + Number(point.status_non_200)
    ) + ' 次调用');

    requestAnimationFrame(() => {
      const wrapBox = chartWrap.getBoundingClientRect();
      const svgBox = svg.getBoundingClientRect();
      const xPosition = svgBox.left - wrapBox.left + x / layout.width * svgBox.width;
      const yPosition = svgBox.top - wrapBox.top + Math.min(y200, yNon200) / layout.height * svgBox.height;
      let left = xPosition + 14;
      if (left + tooltip.offsetWidth > wrapBox.width - 8) {
        left = xPosition - tooltip.offsetWidth - 14;
      }
      const top = Math.max(9, Math.min(
        yPosition - tooltip.offsetHeight - 10,
        wrapBox.height - tooltip.offsetHeight - 9
      ));
      tooltip.style.left = Math.max(8, left) + 'px';
      tooltip.style.top = top + 'px';
    });
  }

  function render(nextPayload) {
    if (!nextPayload || !Array.isArray(nextPayload.points)) return;
    payload = nextPayload;
    points = nextPayload.points.map((point) => ({
      ts: String(point.ts || ''),
      status_200: Math.max(0, Number(point.status_200) || 0),
      status_non_200: Math.max(0, Number(point.status_non_200) || 0)
    }));
    if (!points.length) return;

    const rawMaximum = Math.max(
      0,
      ...points.map((point) => Math.max(point.status_200, point.status_non_200))
    );
    const maximum = niceMaximum(rawMaximum);
    svg.dataset.maximum = String(maximum);
    renderGrid(maximum);
    line200.setAttribute('d', linePath('status_200', maximum));
    lineNon200.setAttribute('d', linePath('status_non_200', maximum));
    errorArea.setAttribute('d', areaPath('status_non_200', maximum));

    const totals = points.reduce((result, point) => {
      result.status200 += point.status_200;
      result.statusNon200 += point.status_non_200;
      return result;
    }, {status200: 0, statusNon200: 0});
    const total = totals.status200 + totals.statusNon200;
    const rate = total ? totals.statusNon200 / total * 100 : 0;
    const latestFailure = [...points].reverse().find((point) => point.status_non_200 > 0);
    roleText('timeline-total-200', count(totals.status200));
    roleText('timeline-total-non-200', count(totals.statusNon200));
    roleText('timeline-error-rate', rate.toFixed(rate >= 10 ? 0 : 1) + '%');
    roleText('timeline-last-failure', latestFailure ? localTime(latestFailure.ts, false) : '暂无');
    emptyState.toggleAttribute('hidden', total > 0);
    svg.setAttribute(
      'aria-label',
      'API 响应时间轴，HTTP 200 共 ' + totals.status200 +
      ' 次，非 200 共 ' + totals.statusNon200 + ' 次'
    );

    root.querySelectorAll('[data-minutes]').forEach((button) => {
      button.classList.toggle(
        'is-active',
        Number(button.dataset.minutes) === Number(nextPayload.window_minutes)
      );
    });
    roleText(
      'timeline-sync',
      '账号选择后 API · 每分钟 · 更新至 ' +
      localTime(nextPayload.to || points[points.length - 1].ts, false)
    );
    hideTooltip();
  }

  async function load(minutes) {
    if (requestController) requestController.abort();
    requestController = new AbortController();
    root.classList.add('is-loading');
    if (syncLabel) syncLabel.textContent = '正在刷新时间轴…';
    try {
      const url = new URL(root.dataset.endpoint, window.location.href);
      url.searchParams.set('minutes', String(minutes));
      const response = await fetch(url, {
        cache: 'no-store',
        headers: {'Accept': 'application/json'},
        signal: requestController.signal
      });
      if (!response.ok) throw new Error('timeline response ' + response.status);
      const nextPayload = await response.json();
      render(nextPayload);
      try {
        localStorage.setItem('cliproxy-timeline-minutes', String(minutes));
      } catch (error) {}
    } catch (error) {
      if (error.name !== 'AbortError' && syncLabel) {
        syncLabel.textContent = '刷新失败 · 已保留上次数据';
      }
    } finally {
      root.classList.remove('is-loading');
    }
  }

  hitArea.addEventListener('pointermove', (event) => {
    const bounds = hitArea.getBoundingClientRect();
    const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
    showPoint(Math.round(ratio * (points.length - 1)));
  });
  hitArea.addEventListener('pointerleave', hideTooltip);
  svg.addEventListener('keydown', (event) => {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const fallback = event.key === 'ArrowLeft' ? points.length - 1 : 0;
    const nextIndex = activeIndex < 0
      ? fallback
      : Math.max(0, Math.min(points.length - 1, activeIndex + (event.key === 'ArrowLeft' ? -1 : 1)));
    showPoint(nextIndex);
  });
  root.querySelectorAll('[data-minutes]').forEach((button) => {
    button.addEventListener('click', () => {
      load(Number(button.dataset.minutes));
    });
  });
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      if (payload) render(payload);
    }, 120);
  });

  try {
    render(JSON.parse(bootstrapNode.textContent));
  } catch (error) {
    if (syncLabel) syncLabel.textContent = '时间轴数据暂不可用';
    return;
  }

  let preferredMinutes = Number(payload.window_minutes) || 1440;
  try {
    const storedMinutes = Number(localStorage.getItem('cliproxy-timeline-minutes'));
    if ([60, 360, 1440].includes(storedMinutes)) preferredMinutes = storedMinutes;
  } catch (error) {}
  if (preferredMinutes !== Number(payload.window_minutes)) {
    load(preferredMinutes);
  }
  window.setInterval(() => {
    if (!document.hidden && payload) {
      load(Number(payload.window_minutes) || 1440);
    }
  }, 30000);
})();
"""


def response_timeline_chart_html(timeline: Mapping[str, Any]) -> str:
    """Render the status chart shell and a compact, safe bootstrap payload."""

    points = timeline.get("points")
    points = points if isinstance(points, list) else []
    compact_payload = dict(timeline)
    compact_payload["points"] = [
        {
            "ts": safe_text(point.get("ts"), 64) or "",
            "status_200": int(point.get("status_200") or 0),
            "status_non_200": int(point.get("status_non_200") or 0),
        }
        for point in points
        if isinstance(point, Mapping)
    ]
    payload_json = json.dumps(
        compact_payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    payload_json = (
        payload_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    totals = timeline.get("totals")
    totals = totals if isinstance(totals, Mapping) else {}
    total_200 = int(totals.get("status_200") or 0)
    total_non_200 = int(totals.get("status_non_200") or 0)
    total = total_200 + total_non_200
    error_rate = total_non_200 / total * 100.0 if total else 0.0
    last_failure = next(
        (
            point
            for point in reversed(points)
            if isinstance(point, Mapping)
            and int(point.get("status_non_200") or 0) > 0
        ),
        None,
    )
    last_failure_text = (
        fmt_local_time(last_failure.get("ts"))
        if isinstance(last_failure, Mapping)
        else "暂无"
    )
    return f"""
<div class="section-title"><div><h2>API 响应时间轴</h2><p>已进入账号选择阶段的 API HTTP 200 与非 200 双折线；连续到每一分钟，快速定位无响应与异常时段。</p></div><small>默认近 24 小时</small></div>
<section class="panel response-timeline-panel" data-role="response-timeline" data-endpoint="/usage/timeline">
  <div class="timeline-toolbar">
    <div class="timeline-kpis" aria-live="polite">
      <div class="timeline-kpi ok"><i></i><span>HTTP 200</span><strong data-role="timeline-total-200">{fmt_int(total_200)}</strong></div>
      <div class="timeline-kpi bad"><i></i><span>非 200</span><strong data-role="timeline-total-non-200">{fmt_int(total_non_200)}</strong></div>
      <div class="timeline-kpi rate"><i></i><span>异常比例</span><strong data-role="timeline-error-rate">{error_rate:.1f}%</strong></div>
      <div class="timeline-kpi last"><i></i><span>最近非 200</span><strong data-role="timeline-last-failure">{html.escape(last_failure_text)}</strong></div>
    </div>
    <div class="timeline-controls">
      <div class="timeline-range" role="group" aria-label="时间轴范围">
        <button type="button" data-minutes="60">1 小时</button>
        <button type="button" data-minutes="360">6 小时</button>
        <button type="button" data-minutes="1440" class="is-active">24 小时</button>
      </div>
    </div>
  </div>
  <div class="timeline-chart-wrap">
    <div class="timeline-chart-scroll">
      <svg class="response-timeline-chart" data-role="timeline-svg" viewBox="0 0 1200 360" role="img" tabindex="0" aria-label="API 响应时间轴">
        <defs><linearGradient id="timeline-error-gradient" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="var(--rose)" stop-opacity=".72"/><stop offset="1" stop-color="var(--rose)" stop-opacity="0"/></linearGradient></defs>
        <rect class="timeline-plot-bg" x="62" y="28" width="1108" height="266" rx="12"></rect>
        <g data-role="timeline-grid" aria-hidden="true"></g>
        <path class="timeline-error-area" data-role="timeline-error-area" aria-hidden="true"></path>
        <path class="timeline-line timeline-line-200" data-role="timeline-line-200" aria-hidden="true"></path>
        <path class="timeline-line timeline-line-non-200" data-role="timeline-line-non-200" aria-hidden="true"></path>
        <line class="timeline-crosshair" data-role="timeline-crosshair" y1="28" y2="294" hidden></line>
        <circle class="timeline-marker ok" data-role="timeline-marker-200" r="6" hidden></circle>
        <circle class="timeline-marker bad" data-role="timeline-marker-non-200" r="6" hidden></circle>
        <text class="timeline-empty" data-role="timeline-empty" x="616" y="168">当前范围暂无 API 调用记录</text>
        <rect class="timeline-hit-area" data-role="timeline-hit-area" x="62" y="28" width="1108" height="266"></rect>
      </svg>
    </div>
    <div class="timeline-tooltip" data-role="timeline-tooltip" hidden>
      <strong data-role="timeline-tooltip-time">—</strong>
      <div><span><i class="ok"></i>HTTP 200</span><b data-role="timeline-tooltip-200">0</b></div>
      <div><span><i class="bad"></i>非 200</span><b data-role="timeline-tooltip-non-200">0</b></div>
      <small data-role="timeline-tooltip-total">该分钟共 0 次调用</small>
    </div>
  </div>
  <div class="timeline-meta"><div class="timeline-legend"><span><i class="ok"></i>HTTP 200</span><span><i class="bad"></i>非 200 / 无上游响应</span></div><div class="timeline-sync" data-role="timeline-sync">账号选择后 API · 每分钟</div></div>
  <div class="timeline-footnote">时间轴固定合并 Sub2API、Cockpit Tools、Codex 本地、8327 代理与 8317 队列中已进入账号选择阶段的请求；账号选择前的网关拒绝不进入时间轴。Codex 本地成功用量归入 200 线，状态为推定成功；本地日志不提供完整 HTTP 失败记录，手动汇总不计入。分钟观测不含账号信息，因此账号明细退役不会抹掉响应曲线。无上游响应时，8327 会记录为 <b>502</b> 并进入非 200 线；两条线同时为 0 仍只表示该分钟没有采集到已完成请求。JSON：<code>/usage/timeline?minutes=1440</code>。</div>
</section>
<script id="response-timeline-data" type="application/json">{payload_json}</script>
"""


def token_mix_html(summary: Mapping[str, Any]) -> str:
    components = [
        (
            "非缓存输入",
            int(summary.get("non_cached_input_tokens") or 0),
            summary.get("non_cached_input_cost_usd"),
            "cyan",
        ),
        (
            "缓存输入",
            int(summary.get("cached_tokens") or 0),
            summary.get("cached_input_cost_usd"),
            "violet",
        ),
        (
            "输出",
            int(summary.get("output_tokens") or 0),
            summary.get("output_cost_usd"),
            "amber",
        ),
    ]
    denominator = sum(value for _, value, _, _ in components) or 1
    bars = "".join(
        f'<span class="mix-segment {tone}" style="width:{value / denominator * 100:.4f}%"></span>'
        for _, value, _, tone in components
        if value
    )
    legend = "".join(
        f'<div class="mix-item"><i class="dot {tone}"></i><span>{html.escape(label)}</span>'
        f'<strong>{fmt_compact(value)} · {fmt_money(cost)}</strong></div>'
        for label, value, cost, tone in components
    )
    reasoning = fmt_compact(summary.get("reasoning_tokens"))
    return (
        f'<div class="mix-bar">{bars}</div><div class="mix-legend">{legend}'
        f'<div class="mix-item"><i class="dot mint"></i><span>推理（输出子集）</span><strong>{reasoning}</strong></div>'
        "</div>"
    )


def period_card_html(label: str, data: Mapping[str, Any]) -> str:
    return (
        '<article class="period-card">'
        '<div class="period-head"><div>'
        f'<span>{html.escape(label)}</span>'
        f'<strong>{fmt_compact(data.get("codex_status_tokens"))} tokens</strong>'
        '</div>'
        f'<em>{fmt_money(data.get("split_cost_total_usd"))}</em></div>'
        '<div class="period-meta">'
        f'<span>非缓存输入：{fmt_int(data.get("non_cached_input_tokens"))} · '
        f'{fmt_money(data.get("non_cached_input_cost_usd"))}</span>'
        f'<span>输出：{fmt_int(data.get("output_tokens"))} · '
        f'{fmt_money(data.get("output_cost_usd"))}</span>'
        f'<span>缓存输入：{fmt_int(data.get("cached_tokens"))} · '
        f'{fmt_money(data.get("cached_input_cost_usd"))}</span>'
        f'<span>API 原始处理：{fmt_int(data.get("api_processed_tokens"))}</span>'
        f'<span>缓存命中率：{fmt_percent(data.get("cache_hit_rate_percent"))}</span>'
        f'<span>账号尝试：{fmt_int(data.get("account_attempts"))}</span>'
        f'<span>失败尝试：{fmt_int(data.get("failed_attempts"))}</span>'
        '</div>'
        f'{token_mix_html(data)}'
        '</article>'
    )


def window_meter_html(window: Mapping[str, Any] | None, label: str) -> str:
    if not window:
        return (
            f'<div class="quota-row muted-row"><div><b>{html.escape(label)}</b>'
            '<span>尚未获取窗口数据</span></div><strong>—</strong></div>'
        )
    remaining = window.get("remaining_percent")
    pct = min(max(float(remaining), 0.0), 100.0) if remaining is not None else 0.0
    used = window.get("used_percent")
    tone = "critical" if pct <= 10 else ("warning" if pct <= 30 else "healthy")
    reset = fmt_local_time(window.get("reset_at"))
    return (
        f'<div class="quota-row"><div class="quota-copy"><div><b>{html.escape(label)}</b>'
        f'<span>已用 {fmt_percent(used)} · 剩余 {fmt_percent(remaining)} · {html.escape(reset)} 重置</span></div>'
        f'<strong class="{tone}">{fmt_percent(remaining)}</strong></div>'
        f'<div class="meter"><span class="{tone}" style="width:{pct:.2f}%"></span></div></div>'
    )


def html_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    head = "".join(f"<th>{html.escape(str(item))}</th>" for item in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{html.escape(str(item if item is not None else '—'))}</td>" for item in row) + "</tr>")
    body = "".join(body_rows) or f'<tr><td colspan="{len(headers)}" class="empty">No data yet</td></tr>'
    return f"<div class=table-wrap><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def dashboard_html(
    repo: UsageRepository,
    queue_status: Mapping[str, Any] | None = None,
    quota_status: Mapping[str, Any] | None = None,
    codex_app_status: Mapping[str, Any] | None = None,
    account_resolver: AccountResolver | None = None,
    cockpit_status: Mapping[str, Any] | None = None,
    sub2api_status: Mapping[str, Any] | None = None,
) -> str:
    today = repo.token_breakdown("today")
    week = repo.token_breakdown("7d")
    all_time = repo.token_breakdown("all")
    daily = repo.daily_usage(7)
    response_timeline = repo.response_timeline()
    response_timeline_chart = response_timeline_chart_html(response_timeline)
    subscriptions = repo.subscription_dashboard_rows()
    persisted_quota_accounts = sum(1 for row in subscriptions if row.get("windows"))
    models = repo.grouped("7d", "model")
    recent = repo.recent_account_attempts(50)
    if account_resolver is not None:
        for row in [*subscriptions, *recent]:
            identity = account_resolver.resolve_identity_key(row.get("identity_key"))
            if identity.usage_alias:
                row["usage_alias"] = identity.usage_alias
            if identity.account_email:
                row["account_email"] = identity.account_email
    coverage = repo.coverage()
    price_sync = repo.price_sync_status()
    queue_status = queue_status or {}
    quota_status = quota_status or {}
    codex_app_status = codex_app_status or {}
    cockpit_status = cockpit_status or {}
    sub2api_status = sub2api_status or {}

    hero_metrics = "".join(
        f'<article class="hero-card {tone}"><span>{html.escape(label)}</span>'
        f'<strong>{html.escape(value)}</strong><small>{html.escape(note)}</small></article>'
        for label, value, note, tone in (
            (
                "非缓存输入 Tokens",
                fmt_compact(all_time["non_cached_input_tokens"]),
                f"输入成本 {fmt_money(all_time['non_cached_input_cost_usd'])} · "
                f"有效 {fmt_rate_per_million(all_time['non_cached_input_cost_usd'], all_time['non_cached_input_tokens'])} · "
                f"{fmt_int(all_time['non_cached_input_tokens'])}",
                "cyan-card",
            ),
            (
                "输出 Tokens",
                fmt_compact(all_time["output_tokens"]),
                f"输出成本 {fmt_money(all_time['output_cost_usd'])} · "
                f"有效 {fmt_rate_per_million(all_time['output_cost_usd'], all_time['output_tokens'])} · "
                f"{fmt_int(all_time['output_tokens'])}",
                "output-card",
            ),
            (
                "缓存命中 Tokens",
                fmt_compact(all_time["cached_tokens"]),
                f"缓存成本 {fmt_money(all_time['cached_input_cost_usd'])} · "
                f"有效 {fmt_rate_per_million(all_time['cached_input_cost_usd'], all_time['cached_tokens'])} · "
                f"命中率 {fmt_percent(all_time['cache_hit_rate_percent'])}",
                "mint-card",
            ),
            (
                "API 原始处理量",
                fmt_compact(all_time["api_processed_tokens"]),
                "输入（含缓存）+ 输出 · 用于吞吐与计费",
                "blue-card",
            ),
            (
                "API 等价成本",
                fmt_money(all_time["estimated_api_cost_usd"]),
                "非缓存输入 + 缓存输入 + 输出 · 按采集时价格快照",
                "violet-card",
            ),
            (
                "账号尝试",
                fmt_int(all_time["account_attempts"]),
                f"成功 {fmt_int(all_time['successful_attempts'])} · "
                f"失败 {fmt_int(all_time['failed_attempts'])} · 请求关联不落库",
                "amber-card",
            ),
        )
    )

    period_cards = "".join(
        period_card_html(label, data)
        for label, data in (("今天", today), ("近 7 天", week), ("全部记录", all_time))
    )

    max_tokens = max((int(row["codex_status_tokens"] or 0) for row in daily), default=0) or 1
    trend_bars = "".join(
        f'<div class="trend-column"><div class="bar-tooltip">实际消耗 {fmt_int(row["codex_status_tokens"])}<br>'
        f'非缓存输入 {fmt_int(row["non_cached_input_tokens"])}<br>'
        f'输出 {fmt_int(row["output_tokens"])}<br>'
        f'缓存命中 {fmt_int(row["cached_tokens"])}<br>'
        f'原始处理 {fmt_int(row["api_processed_tokens"])}<br>'
        f'{fmt_money(row["estimated_api_cost_usd"])}</div><div class="trend-track">'
        f'<span style="height:{max(3.0, int(row["codex_status_tokens"] or 0) / max_tokens * 100):.2f}%"></span></div>'
        f'<b>{html.escape(row["date"][5:])}</b><small>{fmt_compact(row["codex_status_tokens"])}</small></div>'
        for row in daily
    )

    subscription_cards: list[str] = []
    for row in subscriptions:
        legacy_ambiguous = bool(row.get("legacy_ambiguous"))
        windows = row.get("windows") or {}
        weekly = windows.get("weekly") or windows.get("monthly")
        five_hour = windows.get("five_hour")
        account_status = windows.get("account_status")
        account_status = account_status if isinstance(account_status, Mapping) else {}
        account_state_method = safe_alias(account_status.get("estimate_method")) or ""
        sub2api_account_state = (
            account_state_method.removeprefix("sub2api_")
            if account_status.get("source") == SUB2API_ACCOUNT_SOURCE
            and account_state_method.startswith("sub2api_")
            else ""
        )
        full_quota = row.get("current_window_full_quota_usd")
        floor = row.get("current_cycle_floor_usd")
        badge, badge_title = identity_badge(row)
        quota_text = (
            fmt_money(full_quota)
            if full_quota is not None else "观测中"
        )
        quota_note = (
            f"{row.get('quota_estimate_method') or '历史事件'} · "
            f"{row.get('quota_estimate_confidence') or 'unknown'}"
            if full_quota is not None else f"当前已观测 ≥ {fmt_money(floor)} · 低置信度"
        )
        account_label = dashboard_identity(row)
        plan = "LEGACY" if legacy_ambiguous else str(row.get("plan_type") or "unknown").upper()
        upstream_reports_zero = any(
            (
                _percent_value(window.get("remaining_percent")) is not None
                and float(window.get("remaining_percent")) <= 0.0001
            )
            or (
                _percent_value(window.get("used_percent")) is not None
                and float(window.get("used_percent")) >= 99.9999
            )
            for window in windows.values()
            if isinstance(window, Mapping)
        )
        execution_availability = row.get("execution_availability")
        if legacy_ambiguous:
            status_text = "历史归属不确定"
            status_class = "reported"
        elif sub2api_account_state:
            status_text, status_class = {
                "active": ("Sub2API 可调度", "available"),
                "rate_limited": ("Sub2API 限流中", "confirmed"),
                "inactive": ("Sub2API 已停用", "confirmed"),
                "error": ("Sub2API 账号异常", "confirmed"),
                "overloaded": ("Sub2API 过载中", "reported"),
                "temporarily_unschedulable": ("Sub2API 暂不可调度", "reported"),
                "unschedulable": ("Sub2API 不可调度", "reported"),
                "unknown": ("Sub2API 状态未知", "reported"),
            }.get(
                sub2api_account_state,
                ("Sub2API 状态未知", "reported"),
            )
        elif execution_availability == "confirmed_exhausted":
            status_text = "已确认耗尽 · 冷却中"
            status_class = "confirmed"
        elif upstream_reports_zero and execution_availability == "provider_available":
            status_text = "上游 0% · 仍允许调用"
            status_class = "available"
        elif upstream_reports_zero and execution_availability == "recent_success":
            status_text = "上游 0% · 实测可用"
            status_class = "available"
        elif execution_availability == "provider_available":
            status_text = "上游确认可用"
            status_class = "available"
        elif execution_availability == "recent_success":
            status_text = "最近实测可用"
            status_class = "available"
        elif upstream_reports_zero:
            status_text = "上游报告 0%"
            status_class = "reported"
        else:
            status_text = "实时额度" if row.get("fetched_at") else "等待额度快照"
            status_class = "reported"
        observation_parts: list[str] = []
        if row.get("last_execution_at"):
            observation_parts.append(
                f"最后账号尝试 {fmt_local_time(row.get('last_execution_at'), seconds=True)}"
                f" · HTTP {int(row.get('last_execution_status') or 0)}"
            )
        quota_signal_at = (
            row.get("provider_gate_at")
            or row.get("reported_zero_at")
            or row.get("fetched_at")
        )
        if quota_signal_at:
            observation_parts.append(
                f"额度信号 {fmt_local_time(quota_signal_at, seconds=True)}"
            )
        observation_text = " · ".join(observation_parts)
        observation_html = (
            f'<span class="account-observation">{html.escape(observation_text)}</span>'
            if observation_text else ""
        )
        quota_label = (
            "上周期满额 API 等价参考"
            if row.get("quota_estimate_method") == "previous_window_transfer"
            and float(row.get("quota_used_percent") or 0.0) == 0.0
            else "API 等价额度估算"
        )
        if legacy_ambiguous:
            quota_label = "历史记录"
            quota_text = "不参与额度"
            quota_note = "无法安全拆分 · 已保留调用统计"
        quota_rows: list[str] = []
        if five_hour:
            quota_rows.append(window_meter_html(five_hour, "5 小时额度"))
        if weekly:
            quota_rows.append(
                window_meter_html(
                    weekly,
                    "月额度" if windows.get("monthly") else "周额度",
                )
            )
        if not quota_rows:
            quota_rows.append(window_meter_html(None, "额度窗口"))
        subscription_cards.append(
            f'<article class="subscription-card"><div class="account-head"><div class="avatar" title="{html.escape(badge_title)}">{badge}</div>'
            f'<div class="account-copy"><h3>{html.escape(account_label)}</h3>'
            f'<span class="account-meta">{html.escape(plan)} · {html.escape(badge_title)}</span>{observation_html}</div>'
            f'<i class="{status_class}" title="{html.escape(observation_text)}">{html.escape(status_text)}</i></div>'
            f'{"".join(quota_rows)}'
            f'<div class="account-usage"><span>总调用 <b>{fmt_int(row.get("all_time_account_attempts"))}</b></span>'
            f'<span class="account-success">成功 <b>{fmt_int(row.get("all_time_successful_calls"))}</b></span>'
            f'<span class="account-failure">失败 <b>{fmt_int(row.get("all_time_failed_calls"))}</b></span>'
            f'<span>非缓存输入 <b>{fmt_compact(row.get("all_time_non_cached_input_tokens"))}</b></span>'
            f'<span>输出 <b>{fmt_compact(row.get("all_time_output_tokens"))}</b></span>'
            f'<span>缓存输入 <b>{fmt_compact(row.get("all_time_cached_tokens"))}</b></span></div>'
            f'<div class="quota-value"><div><span>{html.escape(quota_label)}</span><strong>{quota_text}</strong></div>'
            f'<small>{html.escape(quota_note)}<br>累计消费 {fmt_money(row.get("all_time_cost_usd"))}</small></div></article>'
        )
    subscriptions_html = "".join(subscription_cards) or (
        '<div class="empty-state">账号与额度快照尚未就绪，采集器正在等待上游数据。</div>'
    )

    model_rows = "".join(
        f'<tr><td><b>{html.escape(str(row["model"]))}</b></td><td>{fmt_int(row["logical_requests"])}</td>'
        f'<td>{fmt_int(row["account_attempts"])}</td><td>{fmt_int(row["non_cached_input_tokens"])}</td>'
        f'<td>{fmt_int(row["output_tokens"])}</td><td>{fmt_int(row["cached_tokens"])}</td>'
        f'<td>{fmt_int(row["long_context_priced_calls"])}</td>'
        f'<td>{fmt_money(row["non_cached_input_cost_usd"])}</td>'
        f'<td>{fmt_money(row["output_cost_usd"])}</td>'
        f'<td>{fmt_money(row["cached_input_cost_usd"])}</td>'
        f'<td>{fmt_money(row["estimated_api_cost_usd"])}</td></tr>'
        for row in models
    ) or '<tr><td colspan="11" class="empty">暂无数据</td></tr>'
    account_rows = "".join(
        f'<tr><td><b>{html.escape(dashboard_identity(row))}</b></td>'
        f'<td>{fmt_int(row.get("all_time_logical_requests"))}</td>'
        f'<td>{fmt_int(row.get("all_time_account_attempts"))}</td>'
        f'<td>{fmt_int(row.get("all_time_non_cached_input_tokens"))}</td>'
        f'<td>{fmt_int(row.get("all_time_output_tokens"))}</td>'
        f'<td>{fmt_int(row.get("all_time_cached_tokens"))}</td>'
        f'<td>{fmt_money(row.get("all_time_non_cached_input_cost_usd"))}</td>'
        f'<td>{fmt_money(row.get("all_time_output_cost_usd"))}</td>'
        f'<td>{fmt_money(row.get("all_time_cached_input_cost_usd"))}</td>'
        f'<td>{fmt_money(row.get("all_time_cost_usd"))}</td></tr>'
        for row in subscriptions
    ) or '<tr><td colspan="10" class="empty">暂无数据</td></tr>'
    def _status_class(row: Mapping[str, Any]) -> str:
        return "ok" if row.get("ok") else "bad"

    recent_rows = "".join(
        f'<tr><td>{html.escape(fmt_local_time(row["ts"], seconds=True))}</td><td>{html.escape(dashboard_identity(row))}</td>'
        f'<td>{html.escape(str(row["model"] or "—"))}</td><td><span class="status-pill {_status_class(row)}">{row["status_code"]}</span></td>'
        f'<td>{fmt_int(max(int(row["input_tokens"] or 0) - int(row["cached_tokens"] or 0), 0))}</td>'
        f'<td>{fmt_int(row["output_tokens"])}</td><td>{fmt_int(row["cached_tokens"])}</td>'
        f'<td>{"统一费率" if row.get("model") == "gpt-6-astra" else ("长上下文" if row.get("long_context_pricing_applied") else "短上下文")}</td>'
        f'<td>{fmt_money(row["non_cached_input_cost_usd"])}</td>'
        f'<td>{fmt_money(row["output_cost_usd"])}</td>'
        f'<td>{fmt_money(row["cached_input_cost_usd"])}</td>'
        f'<td>{fmt_money(row["estimated_api_cost_usd"])}</td></tr>'
        for row in recent
    ) or '<tr><td colspan="12" class="empty">暂无数据</td></tr>'

    session_notice = (
        '<div class="notice warning-notice"><b>会话关联已按隐私策略关闭</b>'
        '<span>session/thread/turn/request ID 不写入 SQLite；本页是跨 session 的 token 汇总，'
        '不能直接与某个 tmux /status 做同范围比较。“实际消耗”只统一了 token 算法'
        '（非缓存输入 + 输出）。</span></div>'
    )

    cockpit_notice = (
        '<div class="notice app-import-notice"><b>Cockpit Tools 只读导入</b>'
        f'<span>已导入 {fmt_int(cockpit_status.get("imported_events"))} 条请求统计；'
        '账号缓存只在内存中用于身份与额度映射，凭据、请求 ID、错误正文和原始账号 ID 均不落库。'
        '迁移后请让客户端直连 Cockpit；若同一请求仍先经过 8327，再开启此导入会产生双计数。</span></div>'
        if cockpit_status.get("enabled")
        else ""
    )

    sub2api_enabled = bool(sub2api_status.get("enabled"))
    sub2api_configured = bool(sub2api_status.get("configured"))
    sub2api_key_loaded = bool(sub2api_status.get("key_loaded"))
    sub2api_last_success = sub2api_status.get("last_success_at")
    sub2api_error_type = sub2api_status.get("last_error_type")
    sub2api_auth_failed = sub2api_error_type in {
        "Sub2APIAuthenticationError"
    }
    if sub2api_enabled:
        if not sub2api_configured or not sub2api_key_loaded:
            sub2api_notice_title = "Sub2API 同步等待密钥"
            sub2api_notice_class = ""
            sub2api_notice_body = (
                "当前未载入可用的 owner-only 管理密钥，Sub2API 调用和新增账号尚未同步。"
            )
        elif sub2api_auth_failed:
            sub2api_notice_title = "Sub2API 管理认证失败"
            sub2api_notice_class = ""
            sub2api_notice_body = (
                "管理 API 返回 401/403；采集器已进入长退避，8327 看板其余来源继续工作。"
            )
        elif sub2api_error_type:
            sub2api_notice_title = "Sub2API 同步异常"
            sub2api_notice_class = ""
            sub2api_notice_body = (
                "最近一次完整同步未完成；采集器会退避重试，详情可在脱敏健康状态中查看。"
            )
        elif sub2api_last_success:
            sub2api_notice_title = "Sub2API 只读同步正常"
            sub2api_notice_class = " app-import-notice"
            sub2api_notice_body = (
                f'已发现 {fmt_int(sub2api_status.get("account_count"))} 个账号，累计导入 '
                f'{fmt_int(sub2api_status.get("imported_events"))} 条调用；本轮扫描 '
                f'{fmt_int(sub2api_status.get("last_scanned"))} 条、新增 '
                f'{fmt_int(sub2api_status.get("last_imported"))} 条，并写入 '
                f'{fmt_int(sub2api_status.get("last_quota_rows"))} 条账号/额度变化。'
                "管理员密钥、请求 ID、原始账号 ID 与账号名称不会写入 meter 数据库。"
            )
        else:
            sub2api_notice_title = "Sub2API 同步连接中"
            sub2api_notice_class = ""
            sub2api_notice_body = (
                "管理密钥已载入，正在等待首次完整账号清单和用量分页。"
            )
        sub2api_notice = (
            f'<div class="notice{sub2api_notice_class}"><b>{sub2api_notice_title}</b>'
            f'<span>{sub2api_notice_body}</span></div>'
        )
    else:
        sub2api_notice = ""

    codex_dynamic = codex_app_status.get("account_mode", "dynamic") == "dynamic"
    manual_usage_alias = safe_alias(codex_app_status.get("usage_alias"))
    manual_account_email: str | None = None
    if not codex_dynamic and manual_usage_alias and account_resolver is not None:
        manual_account_email = safe_email(
            account_resolver.resolve(manual_usage_alias, None).account_email
        )
    codex_discovered = as_nonnegative_int(
        codex_app_status.get("discovered_homes")
    ) or 0
    codex_matched = as_nonnegative_int(codex_app_status.get("matched_homes")) or 0
    codex_failed = (
        as_nonnegative_int(codex_app_status.get("failed_homes")) or 0
    ) + (as_nonnegative_int(codex_app_status.get("rejected_homes")) or 0)
    codex_baselined = as_nonnegative_int(
        codex_app_status.get("last_baselined_files")
    ) or 0
    codex_imported_now = as_nonnegative_int(
        codex_app_status.get("last_imported")
    ) or 0
    if codex_dynamic:
        codex_mode_text = "自动跟随当前账号"
        binding_text = (
            f"本轮为 {fmt_int(codex_baselined)} 个文件建立了安全边界；边界前无法证明归属的记录不会回填。"
            if codex_baselined
            else "账号切换时会先建立安全边界，避免把旧会话归到新账号。"
        )
    else:
        codex_mode_text = html.escape(manual_account_email or "邮箱未获取")
        binding_text = "当前为固定账号严格匹配模式。"
    codex_notice = (
        f'<div class="notice {"app-import-notice" if codex_matched else ""}">'
        '<b>ChatGPT Codex 本地监控</b>'
        f'<span>{codex_mode_text}；已发现 {fmt_int(codex_discovered)} 个安全 CODEX_HOME，'
        f'当前跟踪 {fmt_int(codex_matched)} 个，异常/拒绝 {fmt_int(codex_failed)} 个。'
        f'本轮新增 {fmt_int(codex_imported_now)} 条，累计导入 '
        f'{fmt_int(codex_app_status.get("imported_events"))} 条。{binding_text}'
        '只读取 token_count / rate_limits 元数据；提示词、代码、推理、工具输出和凭据均不保存。'
        '“API 等价成本”不是 Pro 订阅实际扣款。</span></div>'
    )
    manual_import = ""
    if not codex_dynamic and manual_usage_alias:
        manual_alias = html.escape(manual_usage_alias, quote=True)
        manual_account_label = html.escape(
            manual_account_email or "邮箱未获取", quote=True
        )
        manual_import = f"""<details class="manual-import"><summary>手动补录用量 <span>跨设备或本地日志缺失时使用</span></summary><form method="post" action="/usage/manual-import"><input type="hidden" name="usage_alias" value="{manual_alias}"><div class="form-grid"><label>账号<input value="{manual_account_label}" readonly></label><label>模型<input name="model" value="gpt-5.6-sol" maxlength="200" required></label><label>时间<input name="ts" type="datetime-local"></label><label>调用数<input name="call_count" type="number" value="1" min="1" max="100000" required></label><label>输入 tokens<input name="input_tokens" type="number" min="0" required></label><label>缓存 tokens<input name="cached_tokens" type="number" value="0" min="0" required></label><label>输出 tokens<input name="output_tokens" type="number" min="0" required></label><label>推理 tokens<input name="reasoning_tokens" type="number" value="0" min="0"></label></div><button type="submit">导入并估价</button><p>只接收模型、时间、调用数和 token 统计，不接收备注、提示词或代码。输入应包含缓存 token，系统按 max(输入−缓存, 0) + 缓存 + 输出分别套用价格。</p></form></details>"""

    collector_ok = queue_status.get("key_loaded") and queue_status.get("last_status") == 200
    quota_ok = quota_status.get("last_success_at") is not None or persisted_quota_accounts > 0
    codex_app_ok = (
        codex_app_status.get("last_success_at") is not None and codex_matched > 0
    )
    cockpit_ok = (
        cockpit_status.get("last_success_at") is not None
        or int(cockpit_status.get("imported_events") or 0) > 0
    )
    sub2api_ok = bool(
        sub2api_last_success
        or int(sub2api_status.get("imported_events") or 0) > 0
    )
    if not sub2api_enabled:
        sub2api_strip_text = "关闭"
    elif not sub2api_configured or not sub2api_key_loaded:
        sub2api_strip_text = "等待密钥"
    elif sub2api_auth_failed:
        sub2api_strip_text = "认证失败"
    elif sub2api_error_type:
        sub2api_strip_text = "同步异常"
    elif sub2api_ok:
        sub2api_strip_text = (
            f'同步中 · {fmt_int(sub2api_status.get("account_count"))} 账号'
        )
    else:
        sub2api_strip_text = "连接中"
    guard_status = queue_status.get("quota_routing_guard")
    guard_status = guard_status if isinstance(guard_status, Mapping) else {}
    guard_enabled = bool(guard_status.get("enabled"))
    guard_locks = as_nonnegative_int(guard_status.get("active_locks")) or 0
    price_ok = price_sync.get("status") == "ok"
    generated = datetime.now().astimezone().isoformat(timespec="seconds")
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30"><title>Codex Usage Observatory</title>
<script>(()=>{{try{{const t=localStorage.getItem('cliproxy-usage-theme');if(t==='light'||t==='dark')document.documentElement.dataset.theme=t}}catch(e){{}}}})()</script>
<style>
:root{{--paper:#fff4dd;--ink:#26201a;--ink-2:#5c5347;--ink-3:#877b6b;--card:#fffdf7;--sun:#ffd84d;--rose:#ffb9cc;--sky:#a5dcff;--mint:#a8edc4;--orange:#ff6b3d;--lavender:#cabdff;--cream:#f1e3c4;--watch:#fff0bd;--shadow-ink:#26201a;--dot:rgba(38,32,26,.08);--on-color:#26201a;--border:2px solid var(--ink);--shadow:5px 5px 0 var(--shadow-ink);--shadow-sm:3px 3px 0 var(--shadow-ink);--radius:14px;--font-display:"Arial Rounded MT Bold",ui-rounded,system-ui,-apple-system,sans-serif;--font-body:system-ui,-apple-system,"Segoe UI",sans-serif;--font-mono:ui-monospace,"SF Mono",Menlo,monospace}}
:root[data-theme="dark"]{{--paper:#211d19;--ink:#f5ead9;--ink-2:#c8baa8;--ink-3:#a69784;--card:#302a24;--sun:#e1bd45;--rose:#ce839a;--sky:#75b5dc;--mint:#78c899;--orange:#e9613b;--lavender:#a395da;--cream:#44392c;--watch:#3f351f;--shadow-ink:#080706;--dot:rgba(245,234,217,.07);--on-color:#211d19}}
*{{box-sizing:border-box}}html{{color-scheme:light}}:root[data-theme="dark"]{{color-scheme:dark}}body{{margin:0;min-width:0;min-height:100vh;color:var(--ink);font:14px/1.48 var(--font-body);background-color:var(--paper);background-image:radial-gradient(var(--dot) 1px,transparent 1px);background-size:22px 22px;overflow-x:hidden}}button{{font:inherit}}main{{width:100%;max-width:1440px;margin:auto;padding:40px 30px 74px}}
header{{display:flex;min-width:0;align-items:center;justify-content:space-between;gap:22px;margin-bottom:30px}}.brand-lockup{{display:flex;min-width:0;align-items:center;gap:15px}}.brand-lockup>div:last-child{{min-width:0}}.brand-mark{{display:grid;place-items:center;width:54px;height:54px;flex:0 0 auto;border:var(--border);border-radius:50%;background:var(--orange);color:var(--on-color);box-shadow:var(--shadow-sm);font:900 16px/1 var(--font-display);transform:rotate(-4deg)}}.eyebrow{{color:var(--ink-3);font:700 10px/1.2 var(--font-mono);letter-spacing:.14em;text-transform:uppercase}}h1{{font:900 clamp(32px,4vw,54px)/.95 var(--font-display);margin:5px 0 7px;letter-spacing:-.035em;overflow-wrap:anywhere}}.subtitle{{max-width:780px;color:var(--ink-2);overflow-wrap:anywhere}}.header-actions{{display:flex;flex:0 0 auto;align-items:center;gap:12px}}.live,.theme-toggle{{border:var(--border);background:var(--card);color:var(--ink);box-shadow:var(--shadow-sm)}}.live{{display:flex;align-items:center;gap:8px;padding:9px 13px;border-radius:999px;font:800 11px/1 var(--font-mono);white-space:nowrap}}.live:before{{content:"";width:9px;height:9px;border:1.5px solid var(--ink);border-radius:50%;background:var(--mint)}}.theme-toggle{{display:grid;place-items:center;width:39px;height:39px;border-radius:50%;cursor:pointer;transition:.12s transform,.12s box-shadow}}.theme-toggle:hover{{transform:translate(-1px,-1px);box-shadow:4px 4px 0 var(--shadow-ink)}}.theme-toggle:active{{transform:translate(2px,2px);box-shadow:none}}.theme-toggle svg{{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round}}.theme-icon-sun{{display:none}}:root[data-theme="dark"] .theme-icon-moon{{display:none}}:root[data-theme="dark"] .theme-icon-sun{{display:block}}
.hero-grid{{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:15px}}.hero-grid>*,.period-grid>*,.subscription-grid>*,.two-col>*{{min-width:0}}.hero-card,.period-card,.subscription-card,.panel{{position:relative;min-width:0;border:var(--border);border-radius:var(--radius);box-shadow:var(--shadow);background:var(--card);overflow:hidden}}.hero-card{{min-height:164px;padding:21px;color:var(--on-color);transition:.14s transform,.14s box-shadow}}.hero-card:hover,.period-card:hover,.subscription-card:hover{{transform:translate(-2px,-2px);box-shadow:7px 7px 0 var(--shadow-ink)}}.hero-card.cyan-card{{background:var(--sun)}}.hero-card.output-card{{background:var(--orange)}}.hero-card.mint-card{{background:var(--mint)}}.hero-card.blue-card{{background:var(--sky)}}.hero-card.violet-card{{background:var(--rose)}}.hero-card.amber-card{{background:var(--lavender)}}.hero-card span,.period-head span,.quota-value span{{font:800 10px/1.2 var(--font-mono);letter-spacing:.09em;text-transform:uppercase}}.hero-card span{{opacity:.68}}.hero-card strong{{display:block;margin:18px 0 9px;font:900 clamp(27px,2.7vw,42px)/1 var(--font-display);letter-spacing:-.035em}}.hero-card small{{display:block;opacity:.72;font-size:10px}}
.notice{{display:flex;gap:12px;align-items:flex-start;margin:18px 0 0;padding:13px 15px;border:var(--border);border-radius:12px;background:var(--watch);box-shadow:var(--shadow-sm)}}.notice b{{white-space:nowrap;font-family:var(--font-display)}}.notice span{{color:var(--ink-2);font-size:12px}}.notice:before{{content:"!";display:grid;place-items:center;width:21px;height:21px;flex:0 0 auto;border:1.5px solid var(--ink);border-radius:50%;background:var(--orange);color:var(--on-color);font-weight:900}}
.app-import-notice{{background:color-mix(in srgb,var(--mint) 36%,var(--card))}}.app-import-notice:before{{content:"✓";background:var(--mint)}}.manual-import{{margin-top:15px;border:var(--border);border-radius:12px;background:var(--card);box-shadow:var(--shadow-sm);overflow:hidden}}.manual-import summary{{padding:13px 15px;cursor:pointer;font-family:var(--font-display)}}.manual-import summary span{{margin-left:8px;color:var(--ink-3);font:600 10px/1.2 var(--font-mono)}}.manual-import form{{padding:16px;border-top:1.5px solid var(--ink)}}.form-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}}.manual-import label{{display:grid;gap:5px;color:var(--ink-2);font:700 10px/1.2 var(--font-mono)}}.manual-import input{{min-width:0;padding:9px 10px;border:1.5px solid var(--ink);border-radius:8px;background:var(--paper);color:var(--ink);font:600 12px/1.2 var(--font-mono)}}.note-label{{margin-top:12px}}.manual-import button{{margin-top:12px;padding:9px 14px;border:var(--border);border-radius:9px;background:var(--orange);color:var(--on-color);box-shadow:var(--shadow-sm);font-weight:900;cursor:pointer}}.manual-import p{{margin:11px 0 0;color:var(--ink-3);font-size:11px}}
.section-title{{display:flex;justify-content:space-between;align-items:end;margin:38px 0 14px}}.section-title h2{{margin:0;font:900 22px/1.1 var(--font-display);letter-spacing:-.015em}}.section-title p{{margin:5px 0 0;color:var(--ink-2)}}.section-title small{{color:var(--ink-3);font-family:var(--font-mono)}}.period-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:15px}}.period-card{{padding:20px;transition:.14s transform,.14s box-shadow}}.period-card:nth-child(1) .period-head span{{background:var(--sun)}}.period-card:nth-child(2) .period-head span{{background:var(--rose)}}.period-card:nth-child(3) .period-head span{{background:var(--sky)}}.period-head{{display:flex;min-width:0;align-items:end;justify-content:space-between;gap:14px}}.period-head>div{{min-width:0}}.period-head span{{display:inline-block;padding:5px 7px;border:1.5px solid var(--ink);border-radius:7px;color:var(--on-color)}}.period-head strong{{display:block;margin-top:9px;font:900 25px/1 var(--font-display);overflow-wrap:anywhere}}.period-head em{{flex:0 0 auto;font:900 20px/1 var(--font-display);font-style:normal;color:var(--ink)}}.period-meta{{display:flex;flex-wrap:wrap;gap:6px 8px;margin-top:14px;color:var(--ink-3);font:600 9px/1.3 var(--font-mono)}}.period-meta span{{max-width:100%;padding:4px 6px;border:1px solid color-mix(in srgb,var(--ink) 38%,transparent);border-radius:6px;background:var(--paper);overflow-wrap:anywhere}}.mix-bar{{display:flex;height:11px;margin:16px 0 14px;border:1.5px solid var(--ink);border-radius:999px;overflow:hidden;background:var(--cream)}}.mix-segment.cyan,.dot.cyan{{background:var(--sky)}}.mix-segment.violet,.dot.violet{{background:var(--lavender)}}.mix-segment.amber,.dot.amber{{background:var(--orange)}}.dot.mint{{background:var(--mint)}}.mix-legend{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:9px 18px}}.mix-item{{display:grid;min-width:0;grid-template-columns:9px minmax(0,1fr) auto;align-items:center;gap:7px;color:var(--ink-2);font-size:11px}}.mix-item span{{min-width:0;overflow-wrap:anywhere}}.mix-item strong{{color:var(--ink);font-family:var(--font-mono)}}.dot{{width:8px;height:8px;border:1px solid var(--ink);border-radius:3px}}
.trend-panel{{padding:22px 22px 17px;background-color:var(--card);background-image:radial-gradient(var(--dot) 1px,transparent 1px);background-size:16px 16px}}.trend{{height:230px;display:grid;grid-template-columns:repeat(7,1fr);gap:14px;align-items:end;padding-top:24px}}.trend-column{{position:relative;display:grid;grid-template-rows:160px auto auto;text-align:center;gap:5px;min-width:0}}.trend-track{{height:160px;display:flex;align-items:end;border:1.5px solid var(--ink);border-radius:9px;background:var(--cream);overflow:hidden}}.trend-track span{{width:100%;min-height:3px;border-top:1.5px solid var(--ink);background:var(--orange)}}.trend-column:nth-child(3n+2) .trend-track span{{background:var(--sun)}}.trend-column:nth-child(3n+3) .trend-track span{{background:var(--sky)}}.trend-column b{{font:700 10px/1 var(--font-mono);color:var(--ink-3)}}.trend-column small{{font:900 11px/1 var(--font-mono)}}.bar-tooltip{{position:absolute;z-index:3;bottom:190px;left:50%;transform:translate(-50%,8px);opacity:0;pointer-events:none;white-space:nowrap;padding:8px 10px;border:var(--border);border-radius:8px;background:var(--card);box-shadow:var(--shadow-sm);font:700 10px/1.45 var(--font-mono);transition:.16s}}.trend-column:hover .bar-tooltip{{opacity:1;transform:translate(-50%,0)}}
.subscription-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(310px,100%),1fr));gap:15px}}.subscription-card{{padding:18px;transition:.14s transform,.14s box-shadow}}.subscription-card:nth-child(4n+1){{border-top:8px solid var(--sun)}}.subscription-card:nth-child(4n+2){{border-top:8px solid var(--rose)}}.subscription-card:nth-child(4n+3){{border-top:8px solid var(--sky)}}.subscription-card:nth-child(4n+4){{border-top:8px solid var(--mint)}}.account-head{{display:grid;min-width:0;grid-template-columns:44px minmax(0,1fr) auto;gap:11px;align-items:center;margin-bottom:18px}}.account-copy{{min-width:0}}.avatar{{display:grid;place-items:center;width:42px;height:42px;border:var(--border);border-radius:50%;background:var(--sun);box-shadow:var(--shadow-sm);color:var(--on-color);font:900 18px/1 var(--font-display)}}.subscription-card:nth-child(4n+2) .avatar{{background:var(--rose)}}.subscription-card:nth-child(4n+3) .avatar{{background:var(--sky)}}.subscription-card:nth-child(4n+4) .avatar{{background:var(--mint)}}.account-head h3{{margin:0;font:900 17px/1.1 var(--font-display);overflow-wrap:anywhere}}.account-head span{{display:block;overflow-wrap:anywhere}}.account-head .account-email{{margin-top:4px;color:var(--ink-2);font:700 10px/1.3 var(--font-mono)}}.account-head .account-email.unavailable{{color:var(--ink-3);font-weight:600}}.account-head .account-meta{{margin-top:3px;color:var(--ink-3);font:600 9px/1.3 var(--font-mono)}}.account-head .account-observation{{margin-top:3px;color:var(--ink-3);font:600 8px/1.3 var(--font-mono)}}.account-head i{{padding:5px 7px;border:1.5px solid var(--ink);border-radius:999px;background:var(--mint);color:var(--on-color);font:800 9px/1 var(--font-mono);font-style:normal}}.quota-row{{margin:14px 0}}.quota-copy{{display:flex;justify-content:space-between;align-items:end}}.quota-copy b{{display:block;font-size:12px}}.quota-copy span,.muted-row span{{display:block;margin-top:2px;color:var(--ink-3);font:600 9px/1.3 var(--font-mono)}}.quota-copy strong{{font:900 17px/1 var(--font-display)}}.healthy{{color:#18824c}}.warning{{color:#b06b00}}.critical{{color:#d6324f}}:root[data-theme="dark"] .healthy{{color:#8ce7af}}:root[data-theme="dark"] .warning{{color:#ffd36b}}:root[data-theme="dark"] .critical{{color:#ff91a3}}.meter{{height:10px;margin-top:8px;border:1.5px solid var(--ink);border-radius:999px;background:var(--cream);overflow:hidden}}.meter span{{display:block;height:100%;border-right:1.5px solid var(--ink);background:currentColor}}.muted-row{{display:flex;justify-content:space-between;color:var(--ink-3)}}.account-usage{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;margin:15px 0 5px}}.account-usage span{{min-width:0;padding:8px;border:1.5px solid var(--ink);border-radius:8px;background:var(--paper);color:var(--ink-3);font:700 8px/1.3 var(--font-mono);overflow-wrap:anywhere}}.account-usage b{{display:block;margin-top:3px;color:var(--ink);font-size:11px}}.quota-value{{display:flex;justify-content:space-between;align-items:end;margin-top:14px;padding-top:15px;border-top:2px solid var(--ink)}}.quota-value span{{color:var(--ink-3)}}.quota-value strong{{display:block;margin-top:5px;font:900 22px/1 var(--font-display)}}.quota-value small{{text-align:right;color:var(--ink-3);font:600 9px/1.4 var(--font-mono)}}
.two-col{{display:grid;grid-template-columns:1fr 1.2fr;gap:15px}}.panel{{padding:0}}.panel h3{{margin:0;padding:15px 18px;border-bottom:2px solid var(--ink);background:var(--sun);color:var(--on-color);font:900 15px/1 var(--font-display)}}.two-col .panel:nth-child(2) h3{{background:var(--sky)}}.table-note{{margin:0;padding:10px 18px;border-bottom:1.5px solid color-mix(in srgb,var(--ink) 28%,transparent);color:var(--ink-3);font:600 9px/1.45 var(--font-mono)}}.table-wrap{{overflow:auto;max-height:520px}}table{{width:100%;border-collapse:collapse}}th,td{{padding:11px 14px;text-align:left;border-bottom:1.5px solid color-mix(in srgb,var(--ink) 48%,transparent);white-space:nowrap}}th{{position:sticky;top:0;z-index:2;background:var(--cream);color:var(--ink-2);font:800 9px/1.2 var(--font-mono);letter-spacing:.06em;text-transform:uppercase}}tbody tr:nth-child(even){{background:color-mix(in srgb,var(--sky) 12%,var(--card))}}tr:last-child td{{border-bottom:0}}.status-pill{{display:inline-block;min-width:42px;padding:3px 7px;border:1.5px solid var(--ink);border-radius:999px;text-align:center;color:var(--on-color);font:900 9px/1 var(--font-mono)}}.status-pill.ok{{background:var(--mint)}}.status-pill.bad{{background:var(--rose)}}.empty,.empty-state{{padding:25px;text-align:center;color:var(--ink-3)}}
.system-strip{{display:flex;flex-wrap:wrap;gap:9px;margin-top:27px}}.system-strip span{{padding:7px 10px;border:1.5px solid var(--ink);border-radius:999px;background:var(--card);box-shadow:2px 2px 0 var(--shadow-ink);color:var(--ink-2);font:700 9px/1 var(--font-mono)}}.system-strip span:nth-child(1){{background:var(--mint);color:var(--on-color)}}.system-strip span:nth-child(2){{background:var(--sky);color:var(--on-color)}}.system-strip span:nth-child(3){{background:var(--sun);color:var(--on-color)}}.system-strip b{{color:inherit}}footer{{margin-top:24px;padding-top:16px;border-top:2px solid var(--ink);color:var(--ink-3);font:600 9px/1.65 var(--font-mono)}}
@media(max-width:1320px){{.hero-grid{{grid-template-columns:repeat(3,minmax(0,1fr))}}}}@media(max-width:920px){{main{{padding:26px 16px 55px}}header{{align-items:flex-start}}.brand-mark{{width:46px;height:46px}}.subtitle{{max-width:560px}}.hero-grid,.period-grid,.two-col{{grid-template-columns:minmax(0,1fr)}}.form-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.trend{{gap:7px}}.notice{{display:grid;grid-template-columns:auto minmax(0,1fr)}}.notice b{{white-space:normal}}.notice span{{grid-column:2;overflow-wrap:anywhere}}}}@media(max-width:620px){{header{{align-items:flex-start;flex-direction:column}}.brand-lockup{{align-items:flex-start}}h1{{font-size:clamp(28px,9vw,38px)}}.header-actions{{width:100%;justify-content:space-between}}.hero-grid,.subscription-grid,.form-grid{{grid-template-columns:minmax(0,1fr)}}.period-head{{flex-wrap:wrap}}.mix-legend{{grid-template-columns:minmax(0,1fr)}}.account-usage{{grid-template-columns:repeat(3,minmax(0,1fr))}}.trend-column small{{display:none}}.section-title{{align-items:flex-start;flex-direction:column;gap:6px}}}}@media(prefers-reduced-motion:reduce){{*{{scroll-behavior:auto!important;transition:none!important}}}}
.account-head i.confirmed{{background:var(--rose)}}.account-head i.reported{{background:var(--sun)}}.account-head i.available{{background:var(--mint)}}
{RESPONSE_TIMELINE_CSS}
</style></head><body><main>
<header><div class="brand-lockup"><div class="brand-mark" aria-hidden="true">CU</div><div><div class="eyebrow">Local · Private · Token Safe</div><h1>Codex Usage Observatory</h1><div class="subtitle">跨 Codex 订阅账号的 token、API 等价成本与实时额度；主口径与 Codex /status 对齐。</div></div></div><div class="header-actions"><div class="live">8327 LIVE</div><button class="theme-toggle" type="button" data-role="theme-toggle" aria-label="切换明暗主题" title="切换明暗主题"><svg class="theme-icon-moon" viewBox="0 0 24 24" aria-hidden="true"><path d="M20 15.2A8.5 8.5 0 0 1 8.8 4 8.5 8.5 0 1 0 20 15.2Z"/></svg><svg class="theme-icon-sun" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="3.5"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg></button></div></header>
	<section class="hero-grid">{hero_metrics}</section>
	{codex_notice}
	{cockpit_notice}
	{sub2api_notice}
	{manual_import}
{session_notice}
<div class="notice"><b>按模型计费</b><span>gpt-6-astra 长短上下文统一费率，不加收长上下文费用。其他具有长上下文价格档的模型，按每次调用完整 input tokens 判断：≤272K 使用短档，&gt;272K 使用长档；cached tokens 计入输入阈值，并按对应缓存单价计费。</span></div>
<div class="section-title"><div><h2>Token 消费总览</h2><p>输入、缓存、输出与推理 token，一眼看清今天、7 天和累计。</p></div></div>
<section class="period-grid">{period_cards}</section>
{response_timeline_chart}
<div class="section-title"><div><h2>近 7 天趋势</h2><p>柱高为非缓存输入 + 输出；悬停可看缓存和 API 原始处理量。</p></div></div>
<section class="panel trend-panel"><div class="trend">{trend_bars}</div></section>
<div class="section-title"><div><h2>订阅额度雷达</h2><p>剩余百分比来自 Codex provider 实际窗口（按时长归类为 5 小时/周/月）；美元额度优先按当前窗口估算，低使用量只显示观测下限。</p></div><small>{fmt_int(persisted_quota_accounts)} 个账号已刷新</small></div>
<section class="subscription-grid">{subscriptions_html}</section>
<div class="section-title"><div><h2>消费明细</h2><p>近 7 天模型分布与最近账号尝试；账号选择前的网关鉴权拒绝仅保留在调用/失败历史，不计入模型消费、账号尝试或 HTTP 响应时间轴。</p></div></div>
  <section class="two-col"><article class="panel"><h3>模型消费 · 7 天</h3><div class="table-wrap"><table><thead><tr><th>模型</th><th>聚合调用</th><th>账号尝试</th><th>非缓存输入</th><th>输出</th><th>缓存</th><th>长上下文调用</th><th>输入成本</th><th>输出成本</th><th>缓存成本</th><th>总成本</th></tr></thead><tbody>{model_rows}</tbody></table></div></article><article class="panel"><h3>最近 50 次账号尝试</h3><p class="table-note">这是历史完成记录（时间精确到秒）；账号进入冷却不会删除冷却前的成功记录，当前状态以账号卡的最新额度信号为准。</p><div class="table-wrap"><table><thead><tr><th>时间</th><th>账号</th><th>模型</th><th>状态</th><th>非缓存输入</th><th>输出</th><th>缓存</th><th>计费档</th><th>输入成本</th><th>输出成本</th><th>缓存成本</th><th>总成本</th></tr></thead><tbody>{recent_rows}</tbody></table></div></article></section>
<div class="section-title"><div><h2>账号累计</h2><p>每个订阅自本地 collector 启用以来的 token、请求和 API 等价成本。</p></div></div>
  <section class="panel"><div class="table-wrap"><table><thead><tr><th>账号</th><th>聚合调用</th><th>账号尝试</th><th>非缓存输入</th><th>输出</th><th>缓存</th><th>输入成本</th><th>输出成本</th><th>缓存成本</th><th>总成本</th></tr></thead><tbody>{account_rows}</tbody></table></div></section>
	<div class="system-strip"><span>CLIProxyAPI queue <b>{'正常' if collector_ok else '已暂停/等待'}</b></span><span>Cockpit Tools <b>{'只读导入中' if cockpit_ok else ('关闭' if not cockpit_status.get('enabled') else '等待')}</b></span><span>Sub2API <b>{html.escape(sub2api_strip_text)}</b></span><span>ChatGPT App <b>{f'本地监控中 · {codex_matched}/{codex_discovered}' if codex_app_ok else '等待'}</b></span><span>Quota snapshot <b>{'正常' if quota_ok else '等待'}</b></span><span>Quota guard <b>{f'开启 · {guard_locks} 锁' if guard_enabled else '关闭'}</b></span><span>Official prices <b>{'已同步' if price_ok else '待同步'}</b></span><span>账号尝试 <b>{fmt_int(all_time['account_attempts'])}</b></span><span>覆盖 <b>{fmt_local_time(coverage.get('first_event_ts'))} → {fmt_local_time(coverage.get('last_event_ts'))}</b></span></div>
<footer>自动刷新 30 秒 · 页面生成 {html.escape(generated)} · 实际消耗 = max(输入−缓存, 0)+输出，接近 Codex /status；成本优先沿用采集源冻结的逐请求价格快照，其余事件按 meter 同步的 OpenAI 官方费率逐条计算；gpt-6-astra 已核实的错误长上下文加价会校正为短档费率。gpt-6-astra 不启用长上下文加价，其他具有长档的模型仅在完整 input tokens &gt; 272K 时启用（272K 本身仍是短档），缓存命中计入这个输入阈值。API 原始处理量 = 输入（含缓存）+输出。reasoning 是输出子集，不重复相加。“API 等价成本/额度”不代表订阅现金余额。</footer>
</main><script>(()=>{{const b=document.querySelector('[data-role="theme-toggle"]');if(!b)return;b.addEventListener('click',()=>{{const r=document.documentElement;const next=r.dataset.theme==='dark'?'light':'dark';r.dataset.theme=next;try{{localStorage.setItem('cliproxy-usage-theme',next)}}catch(e){{}}}})}})()</script><script>{RESPONSE_TIMELINE_SCRIPT}</script></body></html>"""


class MeterHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        repo: UsageRepository,
        upstream: str,
        resolver: AccountResolver,
        upstream_timeout: float,
        *,
        management_key_file: str | None = None,
        management_key_env: str = "CLIPROXY_MANAGEMENT_KEY",
        usage_queue_path: str = DEFAULT_USAGE_QUEUE_PATH,
        usage_queue_count: int = DEFAULT_USAGE_QUEUE_COUNT,
        usage_queue_poll_seconds: float = DEFAULT_USAGE_QUEUE_POLL_SECONDS,
        usage_queue_timeout: float = 10.0,
        quota_routing_guard_enabled: bool = False,
        quota_routing_guard_state_file: str | Path = DEFAULT_QUOTA_GUARD_STATE_FILE,
        quota_routing_guard_timeout: float = 10.0,
        quota_poll_seconds: float = DEFAULT_QUOTA_POLL_SECONDS,
        quota_poll_timeout: float = DEFAULT_QUOTA_POLL_TIMEOUT,
        codex_app_home: str | Path = DEFAULT_CODEX_APP_HOME,
        codex_app_alias: str | None = DEFAULT_CODEX_APP_ALIAS,
        codex_app_poll_seconds: float = DEFAULT_CODEX_APP_POLL_SECONDS,
        codex_app_max_files: int = DEFAULT_CODEX_APP_MAX_FILES,
        codex_app_import_enabled: bool = True,
        cockpit_tools_data_dir: str | Path | None = None,
        cockpit_tools_localstorage_db: str | Path | None = None,
        cockpit_tools_poll_seconds: float = DEFAULT_COCKPIT_TOOLS_POLL_SECONDS,
        cockpit_tools_import_enabled: bool = True,
        cockpit_tools_authoritative_accounts: bool = False,
        sub2api_base_url: str = DEFAULT_SUB2API_BASE_URL,
        sub2api_admin_key_file: str | Path | None = None,
        sub2api_admin_key_env: str = "SUB2API_ADMIN_KEY",
        sub2api_poll_seconds: float = DEFAULT_SUB2API_POLL_SECONDS,
        sub2api_timeout: float = DEFAULT_SUB2API_TIMEOUT,
        sub2api_page_size: int = DEFAULT_SUB2API_PAGE_SIZE,
        sub2api_backfill_days: int = DEFAULT_SUB2API_BACKFILL_DAYS,
        sub2api_import_enabled: bool = True,
    ):
        super().__init__(address, UsageMeterHandler)
        parsed = urlsplit(upstream)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("upstream must be an http(s) URL")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("upstream URL must not contain query, fragment, or userinfo")
        self.repo = repo
        self.upstream = parsed
        self.resolver = resolver
        self.resolver.configure_cockpit_sources(
            data_dir=cockpit_tools_data_dir,
            localstorage_db=cockpit_tools_localstorage_db,
            enabled=cockpit_tools_import_enabled,
            authoritative_accounts=cockpit_tools_authoritative_accounts,
        )
        self.upstream_timeout = upstream_timeout
        self.quota_routing_guard = QuotaRoutingGuard(
            repo,
            parsed,
            enabled=quota_routing_guard_enabled,
            state_file=quota_routing_guard_state_file,
            timeout=quota_routing_guard_timeout,
        )
        self.queue_poller = UsageQueuePoller(
            repo,
            resolver,
            parsed,
            key_file=management_key_file,
            key_env=management_key_env,
            queue_path=usage_queue_path,
            count=usage_queue_count,
            poll_seconds=usage_queue_poll_seconds,
            timeout=usage_queue_timeout,
            quota_guard=self.quota_routing_guard,
        )
        self.quota_poller = CodexQuotaPoller(
            repo,
            resolver,
            parsed,
            key_loader=self.queue_poller._load_key,
            poll_seconds=quota_poll_seconds,
            timeout=quota_poll_timeout,
        )
        self.codex_app_importer = CodexAppLocalImporter(
            repo,
            resolver,
            codex_home=codex_app_home,
            alias=codex_app_alias,
            poll_seconds=codex_app_poll_seconds,
            max_files=codex_app_max_files,
        )
        self.codex_app_import_enabled = bool(codex_app_import_enabled)
        self.cockpit_tools_importer = CockpitToolsImporter(
            repo,
            resolver,
            data_dir=cockpit_tools_data_dir,
            localstorage_db=cockpit_tools_localstorage_db,
            poll_seconds=cockpit_tools_poll_seconds,
        )
        self.cockpit_tools_import_enabled = bool(cockpit_tools_import_enabled)
        self.sub2api_importer = Sub2APIImporter(
            repo,
            resolver,
            base_url=sub2api_base_url,
            key_file=sub2api_admin_key_file,
            key_env=sub2api_admin_key_env,
            poll_seconds=sub2api_poll_seconds,
            timeout=sub2api_timeout,
            page_size=sub2api_page_size,
            backfill_days=sub2api_backfill_days,
        )
        self.sub2api_import_enabled = bool(sub2api_import_enabled)

    def cockpit_tools_status(self) -> dict[str, Any]:
        if self.cockpit_tools_import_enabled:
            return self.cockpit_tools_importer.status()
        return {
            "enabled": False,
            "database_available": False,
            "last_poll_at": None,
            "last_success_at": None,
            "last_error_type": None,
            "last_imported": 0,
            "last_scanned": 0,
            "last_quota_rows": 0,
            "imported_events": self.repo.import_status(
                COCKPIT_TOOLS_REQUEST_SOURCE
            ).get("imported_events", 0),
        }

    def sub2api_status(self) -> dict[str, Any]:
        if self.sub2api_import_enabled:
            return self.sub2api_importer.status()
        return {
            "enabled": False,
            "configured": False,
            "key_source": "none",
            "key_loaded": False,
            "api_available": False,
            "last_status": None,
            "last_poll_at": None,
            "last_success_at": None,
            "last_error_type": None,
            "last_imported": 0,
            "last_new": 0,
            "last_changed": 0,
            "last_unchanged": 0,
            "last_retired": 0,
            "last_source_conflict": 0,
            "last_write_transactions": 0,
            "last_usage_updates": 0,
            "last_observation_updates": 0,
            "last_scanned": 0,
            "account_count": 0,
            "last_quota_rows": 0,
            **self.repo.import_status(SUB2API_REQUEST_SOURCE),
        }

    def start_queue_poller(self) -> None:
        self.queue_poller.start()
        self.quota_poller.start()
        # The local ChatGPT importer is independent of the optional 8317
        # management queue and must remain active when both are enabled.
        if self.codex_app_import_enabled:
            self.codex_app_importer.start()
        if self.cockpit_tools_import_enabled:
            self.cockpit_tools_importer.start()
        if self.sub2api_import_enabled:
            self.sub2api_importer.start()

    def start_local_importer(self) -> None:
        if self.codex_app_import_enabled:
            self.codex_app_importer.start()
        if self.cockpit_tools_import_enabled:
            self.cockpit_tools_importer.start()
        if self.sub2api_import_enabled:
            self.sub2api_importer.start()

    def server_close(self) -> None:
        self.sub2api_importer.stop()
        self.cockpit_tools_importer.stop()
        self.codex_app_importer.stop()
        self.quota_poller.stop()
        self.queue_poller.stop()
        super().server_close()


class UsageMeterHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "codex-usage-observatory/0.1"

    @property
    def meter_server(self) -> MeterHTTPServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, fmt: str, *args: Any) -> None:
        # Deliberately suppress BaseHTTPRequestHandler's request-line logging:
        # query strings can contain credentials in poorly behaved clients.
        return

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch()

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._dispatch()

    def _dispatch(self) -> None:
        path = urlsplit(self.path).path
        if path in {"/usage", "/__usage"}:
            if self.command not in {"GET", "HEAD"}:
                self._plain_response(405, b"method not allowed\n")
                return
            try:
                rebound = self.meter_server.repo.reconcile_auth_identities(self.meter_server.resolver)
                if rebound:
                    LOG.info("reconciled %d provisional auth identity event(s)", rebound)
                body = dashboard_html(
                    self.meter_server.repo,
                    self.meter_server.queue_poller.status(),
                    self.meter_server.quota_poller.status(),
                    self.meter_server.codex_app_importer.status(),
                    self.meter_server.resolver,
                    self.meter_server.cockpit_tools_status(),
                    self.meter_server.sub2api_status(),
                ).encode("utf-8")
            except Exception as exc:  # dashboard failure must not expose DB internals
                LOG.error("dashboard rendering failed: %s", type(exc).__name__)
                self._plain_response(500, b"dashboard unavailable\n")
                return
            self._send_bytes(
                200,
                "OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Cache-Control", "no-store"),
                ],
                body,
            )
            return
        if path in {"/usage/timeline", "/usage/api/timeline", "/api/usage/timeline"}:
            if self.command not in {"GET", "HEAD"}:
                self._plain_response(405, b"method not allowed\n")
                return
            self._usage_timeline()
            return
        if path == "/usage/manual-import":
            if self.command != "POST":
                self._plain_response(405, b"method not allowed\n")
                return
            self._manual_import()
            return
        if path == "/healthz":
            body = json.dumps(
                {
                    "ok": True,
                    "upstream": self.meter_server.upstream.geturl(),
                    "usage_queue": self.meter_server.queue_poller.status(),
                    "subscription_quota": self.meter_server.quota_poller.status(),
                    "quota_routing_guard": self.meter_server.quota_routing_guard.status(),
                    "codex_app_local": self.meter_server.codex_app_importer.status(),
                    "cockpit_tools": self.meter_server.cockpit_tools_status(),
                    "sub2api": self.meter_server.sub2api_status(),
                }
            ).encode()
            self._send_bytes(
                200,
                "OK",
                [("Content-Type", "application/json"), ("Cache-Control", "no-store")],
                body,
            )
            return
        if path == "/v1" or path.startswith("/v1/"):
            self._proxy(path)
            return
        self._plain_response(404, b"not found\n")

    def _usage_timeline(self) -> None:
        """Serve the minute-level response-status series used by the chart."""

        try:
            query = urllib.parse.parse_qs(
                urlsplit(self.path).query,
                keep_blank_values=True,
                max_num_fields=8,
            )
            raw_minutes = query.get("minutes", [str(DEFAULT_RESPONSE_TIMELINE_MINUTES)])
            raw_source = query.get("source")
            if set(query) - {"minutes", "source"} or len(raw_minutes) != 1:
                raise ValueError("query parameter must appear once")
            if raw_source is not None and (
                len(raw_source) != 1 or raw_source[0] not in {"", "api"}
            ):
                raise ValueError("timeline is fixed to all API sources")
            minutes = int(raw_minutes[0])
            payload = self.meter_server.repo.response_timeline(minutes)
            body = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self._send_bytes(
                200,
                "OK",
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Cache-Control", "no-store"),
                ],
                body,
            )
        except (ValueError, TypeError, OverflowError):
            body = b'{"error":"invalid timeline query"}'
            self._send_bytes(
                400,
                "Bad Request",
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Cache-Control", "no-store"),
                ],
                body,
            )
        except Exception as exc:
            # Keep SQLite and local path details out of the public loopback
            # response; the type-only log is enough for local diagnosis.
            LOG.error("response timeline failed: %s", type(exc).__name__)
            body = b'{"error":"timeline unavailable"}'
            self._send_bytes(
                500,
                "Internal Server Error",
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Cache-Control", "no-store"),
                ],
                body,
            )

    def _manual_import(self) -> None:
        try:
            body = self._read_request_body()
            if len(body) > MAX_MANUAL_IMPORT_BYTES:
                raise ValueError("form too large")
            fields = urllib.parse.parse_qs(
                body.decode("utf-8"), keep_blank_values=True, max_num_fields=20
            )

            def value(name: str) -> str:
                return fields.get(name, [""])[0]

            alias = safe_alias(value("usage_alias"))
            if alias != self.meter_server.codex_app_importer.alias:
                raise ValueError("unsupported alias")
            model = safe_model_identifier(value("model"))
            if not model:
                raise ValueError("model is required")
            input_tokens = as_nonnegative_int(value("input_tokens"))
            cached_tokens = as_nonnegative_int(value("cached_tokens"))
            output_tokens = as_nonnegative_int(value("output_tokens"))
            reasoning_tokens = as_nonnegative_int(value("reasoning_tokens"))
            call_count = as_nonnegative_int(value("call_count"))
            if input_tokens is None or cached_tokens is None or output_tokens is None:
                raise ValueError("token fields are required")
            if cached_tokens > input_tokens:
                raise ValueError("cached tokens exceed input tokens")
            if not call_count or call_count > 100_000:
                raise ValueError("invalid call count")
            raw_ts = safe_text(value("ts"), 64)
            ts = normalize_timestamp(raw_ts) if raw_ts else utc_now()
            identity = self.meter_server.resolver.resolve(alias, None)
            if not identity.subscription_id_hash:
                raise ValueError("alias is not mapped to a canonical subscription")
            usage = NormalizedUsage(
                input_tokens=input_tokens,
                cached_tokens=cached_tokens,
                output_tokens=output_tokens,
                reasoning_tokens=reasoning_tokens,
                total_tokens=input_tokens + output_tokens,
            )
            components = self.meter_server.repo.price_components_for(model, usage)
            import_key = "manual:" + short_hash(
                f"{utc_now()}\n{alias}\n{model}\n{input_tokens}\n{cached_tokens}\n{output_tokens}"
            )
            event = UsageEvent(
                ts=ts,
                identity_key=resolved_identity_key(identity, alias),
                endpoint="manual://chatgpt-codex",
                method="MANUAL",
                model=model,
                status_code=200,
                ok=1,
                duration_ms=0,
                stream=0,
                session_id=None,
                thread_id=None,
                turn_id=None,
                installation_id=None,
                window_id=None,
                usage_alias=alias,
                usage_project=None,
                auth_fingerprint=None,
                account_id_hash=identity.account_id_hash,
                account_id_tail=identity.account_id_tail,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cached_tokens=usage.cached_tokens,
                cache_write_tokens=0,
                reasoning_tokens=usage.reasoning_tokens,
                total_tokens=usage.total_tokens,
                estimated_api_cost_usd=components.total_cost_usd if components else None,
                non_cached_input_cost_usd=(
                    components.non_cached_input_cost_usd if components else None
                ),
                cached_input_cost_usd=(
                    components.cached_input_cost_usd if components else None
                ),
                output_cost_usd=components.output_cost_usd if components else None,
                long_context_pricing_applied=int(
                    components.long_context_pricing_applied if components else False
                ),
                subscription_amortized_cost_usd=None,
                api_equivalent_quota_usd=None,
                usage_missing=0,
                error_type=None,
                error_message_redacted=None,
                request_bytes=0,
                response_bytes=0,
                call_count=call_count,
                source="manual_codex_app",
                request_id=None,
            )
            self.meter_server.repo.record_imported_event(
                event, import_key, "manual_codex_app"
            )
        except (ValueError, UnicodeDecodeError) as exc:
            LOG.info("manual import rejected: %s", type(exc).__name__)
            self._plain_response(400, b"invalid manual usage import\n")
            return
        self.send_response_only(303, "See Other")
        self.send_header("Location", "/usage")
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _read_request_body(self) -> bytes:
        transfer = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" not in transfer:
            raw_length = self.headers.get("Content-Length")
            if not raw_length:
                return b""
            try:
                length = int(raw_length)
            except ValueError as exc:
                raise ValueError("invalid Content-Length") from exc
            if length < 0:
                raise ValueError("invalid Content-Length")
            return self.rfile.read(length)

        chunks = bytearray()
        while True:
            size_line = self.rfile.readline(128)
            if not size_line:
                raise ConnectionError("unexpected EOF in chunked request")
            try:
                size = int(size_line.split(b";", 1)[0].strip(), 16)
            except ValueError as exc:
                raise ValueError("invalid chunk size") from exc
            if size == 0:
                while True:
                    trailer = self.rfile.readline(65537)
                    if trailer in {b"\r\n", b"\n", b""}:
                        break
                break
            chunks.extend(self.rfile.read(size))
            ending = self.rfile.read(2)
            if ending != b"\r\n":
                raise ValueError("invalid chunk terminator")
        return bytes(chunks)

    def _proxy(self, endpoint: str) -> None:
        started = time.monotonic()
        try:
            body = self._read_request_body()
        except Exception as exc:
            self._plain_response(400, b"invalid request body\n")
            LOG.warning("rejected malformed request body: %s", type(exc).__name__)
            try:
                malformed_info = request_info(endpoint, self.command, self.headers, b"", self.meter_server.resolver)
                self._record_event(
                    self._make_event(
                        malformed_info,
                        malformed_info.model,
                        400,
                        NormalizedUsage(),
                        "invalid_request",
                        type(exc).__name__,
                        0,
                        0,
                        started,
                        force_failed=True,
                    ),
                    malformed_info,
                )
            except Exception as record_exc:
                LOG.error("meter persistence failed: %s", type(record_exc).__name__)
            return
        info = request_info(endpoint, self.command, self.headers, body, self.meter_server.resolver)
        response_started = False
        upstream_response: http.client.HTTPResponse | None = None
        connection: http.client.HTTPConnection | None = None
        try:
            connection = self._connection()
            target = self._upstream_target()
            headers = self._forward_headers(len(body))
            connection.request(self.command, target, body=body if body else None, headers=headers)
            upstream_response = connection.getresponse()
            response_content_type = upstream_response.getheader("Content-Type", "").lower()
            is_stream = bool(info.stream or "text/event-stream" in response_content_type)
            info.stream = int(is_stream)
            if is_stream and self.command != "HEAD":
                response_started = True
                self._proxy_stream(upstream_response, info, len(body), started)
            else:
                response_body = b"" if self.command == "HEAD" else upstream_response.read()
                usage = find_usage(parse_json_bytes(response_body))
                model = info.model or find_model(parse_json_bytes(response_body))
                error_type, error_message = extract_error(response_body, upstream_response.status)
                response_started = True
                self._send_upstream_bytes(upstream_response, response_body)
                event = self._make_event(
                    info,
                    model,
                    upstream_response.status,
                    usage,
                    error_type,
                    error_message,
                    len(body),
                    len(response_body),
                    started,
                )
                self._record_event(event, info)
        except (BrokenPipeError, ConnectionResetError) as exc:
            if not response_started:
                self._plain_response(502, b"upstream unavailable\n")
            event = self._make_event(
                info,
                info.model,
                upstream_response.status if upstream_response else 502,
                NormalizedUsage(),
                "client_disconnect",
                type(exc).__name__,
                len(body),
                0,
                started,
                force_failed=True,
            )
            self._record_event(event, info)
        except Exception as exc:
            if not response_started:
                self._plain_response(502, b"upstream unavailable\n")
            event = self._make_event(
                info,
                info.model,
                upstream_response.status if upstream_response else 502,
                NormalizedUsage(),
                "upstream_error",
                type(exc).__name__,
                len(body),
                0,
                started,
                force_failed=True,
            )
            self._record_event(event, info)
            LOG.warning("upstream request failed: %s", type(exc).__name__)
        finally:
            if connection is not None:
                connection.close()

    def _connection(self) -> http.client.HTTPConnection:
        parsed = self.meter_server.upstream
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if parsed.scheme == "https":
            return http.client.HTTPSConnection(
                parsed.hostname, port, timeout=self.meter_server.upstream_timeout, context=ssl.create_default_context()
            )
        return http.client.HTTPConnection(parsed.hostname, port, timeout=self.meter_server.upstream_timeout)

    def _upstream_target(self) -> str:
        base_path = self.meter_server.upstream.path.rstrip("/")
        request = urlsplit(self.path)
        target = f"{base_path}{request.path}"
        if request.query:
            target += f"?{request.query}"
        return target or "/"

    def _forward_headers(self, body_length: int) -> dict[str, str]:
        connection_tokens = {
            item.strip().lower() for item in self.headers.get("Connection", "").split(",") if item.strip()
        }
        skipped = HOP_BY_HOP_HEADERS | METER_ONLY_HEADERS | connection_tokens | {"host", "content-length"}
        headers = {key: value for key, value in self.headers.items() if key.lower() not in skipped}
        headers["Content-Length"] = str(body_length)
        return headers

    def _proxy_stream(
        self,
        response: http.client.HTTPResponse,
        info: RequestInfo,
        request_bytes: int,
        started: float,
    ) -> None:
        self.send_response_only(response.status, response.reason)
        for key, value in self._response_headers(response.getheaders(), streaming=True):
            self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        inspector = SSEInspector()
        response_bytes = 0
        error_capture = bytearray()
        body_capture = bytearray()
        client_error: Exception | None = None
        try:
            while True:
                chunk = response.read1(64 * 1024)
                if not chunk:
                    break
                response_bytes += len(chunk)
                inspector.feed(chunk)
                if len(body_capture) < MAX_INSPECT_BYTES:
                    body_capture.extend(chunk[: MAX_INSPECT_BYTES - len(body_capture)])
                if response.status >= 300 and len(error_capture) < MAX_ERROR_BYTES:
                    error_capture.extend(chunk[: MAX_ERROR_BYTES - len(error_capture)])
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError) as exc:
            client_error = exc
        finally:
            inspector.finish()
        if inspector.usage.missing:
            fallback_usage = find_usage(parse_json_bytes(bytes(body_capture)))
            if not fallback_usage.missing:
                inspector.usage = fallback_usage
                inspector.model = inspector.model or find_model(parse_json_bytes(bytes(body_capture)))
        error_type, error_message = extract_error(bytes(error_capture), response.status)
        if client_error:
            error_type = "client_disconnect"
            error_message = type(client_error).__name__
        event = self._make_event(
            info,
            info.model or inspector.model,
            response.status,
            inspector.usage,
            error_type,
            error_message,
            request_bytes,
            response_bytes,
            started,
            force_failed=bool(client_error),
        )
        self._record_event(event, info)

    def _make_event(
        self,
        info: RequestInfo,
        model: str | None,
        status_code: int,
        usage: NormalizedUsage,
        error_type: str | None,
        error_message: str | None,
        request_bytes: int,
        response_bytes: int,
        started: float,
        force_failed: bool = False,
    ) -> UsageEvent:
        components = self.meter_server.repo.price_components_for(model, usage)
        return UsageEvent(
            ts=utc_now(),
            identity_key=info.identity_key,
            endpoint=info.endpoint,
            method=info.method,
            model=model,
            status_code=status_code,
            ok=int(200 <= status_code < 300 and not force_failed),
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            stream=info.stream,
            session_id=info.session_id,
            thread_id=info.thread_id,
            turn_id=info.turn_id,
            installation_id=info.installation_id,
            window_id=info.window_id,
            usage_alias=info.usage_alias,
            usage_project=info.usage_project,
            auth_fingerprint=info.auth_fingerprint,
            account_id_hash=info.account_id_hash,
            account_id_tail=info.account_id_tail,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cached_tokens=usage.cached_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            reasoning_tokens=usage.reasoning_tokens,
            total_tokens=usage.total_tokens,
            estimated_api_cost_usd=components.total_cost_usd if components else None,
            non_cached_input_cost_usd=(
                components.non_cached_input_cost_usd if components else None
            ),
            cached_input_cost_usd=components.cached_input_cost_usd if components else None,
            output_cost_usd=components.output_cost_usd if components else None,
            long_context_pricing_applied=int(
                components.long_context_pricing_applied if components else False
            ),
            subscription_amortized_cost_usd=None,
            api_equivalent_quota_usd=None,
            usage_missing=int(usage.missing),
            error_type=safe_text(error_type, 120),
            error_message_redacted=redact_text(error_message),
            request_bytes=request_bytes,
            response_bytes=response_bytes,
        )

    def _record_event(self, event: UsageEvent, info: RequestInfo) -> None:
        try:
            self.meter_server.repo.record_event(event, info, source="sidecar")
        except Exception as exc:
            # Metering is best-effort; never corrupt a completed proxy response.
            LOG.error("meter persistence failed: %s", type(exc).__name__)

    @staticmethod
    def _response_headers(headers: Sequence[tuple[str, str]], streaming: bool) -> list[tuple[str, str]]:
        connection_tokens: set[str] = set()
        for key, value in headers:
            if key.lower() == "connection":
                connection_tokens.update(item.strip().lower() for item in value.split(",") if item.strip())
        skipped = HOP_BY_HOP_HEADERS | connection_tokens | {"content-length"}
        return [(key, value) for key, value in headers if key.lower() not in skipped]

    def _send_upstream_bytes(self, response: http.client.HTTPResponse, body: bytes) -> None:
        self.send_response_only(response.status, response.reason)
        for key, value in self._response_headers(response.getheaders(), streaming=False):
            self.send_header(key, value)
        if self.command == "HEAD":
            original_length = response.getheader("Content-Length")
            if original_length:
                self.send_header("Content-Length", original_length)
        else:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD" and body:
            self.wfile.write(body)

    def _send_bytes(
        self, status: int, reason: str, headers: Sequence[tuple[str, str]], body: bytes
    ) -> None:
        self.send_response_only(status, reason)
        for key, value in headers:
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _plain_response(self, status: int, body: bytes) -> None:
        self._send_bytes(status, "", [("Content-Type", "text/plain; charset=utf-8")], body)


def create_server(
    host: str,
    port: int,
    upstream: str,
    db_path: Path | str,
    *,
    account_resolver: AccountResolver | None = None,
    upstream_timeout: float = 900.0,
    management_key_file: str | None = None,
    management_key_env: str = "CLIPROXY_MANAGEMENT_KEY",
    usage_queue_path: str = DEFAULT_USAGE_QUEUE_PATH,
    usage_queue_count: int = DEFAULT_USAGE_QUEUE_COUNT,
    usage_queue_poll_seconds: float = DEFAULT_USAGE_QUEUE_POLL_SECONDS,
    usage_queue_timeout: float = 10.0,
    quota_routing_guard_enabled: bool = False,
    quota_routing_guard_state_file: str | Path = DEFAULT_QUOTA_GUARD_STATE_FILE,
    quota_routing_guard_timeout: float = 10.0,
    quota_poll_seconds: float = DEFAULT_QUOTA_POLL_SECONDS,
    quota_poll_timeout: float = DEFAULT_QUOTA_POLL_TIMEOUT,
    codex_app_home: str | Path = DEFAULT_CODEX_APP_HOME,
    codex_app_alias: str | None = DEFAULT_CODEX_APP_ALIAS,
    codex_app_poll_seconds: float = DEFAULT_CODEX_APP_POLL_SECONDS,
    codex_app_max_files: int = DEFAULT_CODEX_APP_MAX_FILES,
    codex_app_import_enabled: bool = True,
    cockpit_tools_data_dir: str | Path | None = None,
    cockpit_tools_localstorage_db: str | Path | None = None,
    cockpit_tools_poll_seconds: float = DEFAULT_COCKPIT_TOOLS_POLL_SECONDS,
    cockpit_tools_import_enabled: bool = True,
    cockpit_tools_authoritative_accounts: bool = False,
    sub2api_base_url: str = DEFAULT_SUB2API_BASE_URL,
    sub2api_admin_key_file: str | Path | None = None,
    sub2api_admin_key_env: str = "SUB2API_ADMIN_KEY",
    sub2api_poll_seconds: float = DEFAULT_SUB2API_POLL_SECONDS,
    sub2api_timeout: float = DEFAULT_SUB2API_TIMEOUT,
    sub2api_page_size: int = DEFAULT_SUB2API_PAGE_SIZE,
    sub2api_backfill_days: int = DEFAULT_SUB2API_BACKFILL_DAYS,
    sub2api_import_enabled: bool = True,
) -> MeterHTTPServer:
    repo = UsageRepository(db_path)
    resolver = account_resolver or AccountResolver(
        enabled=os.environ.get("CLIPROXY_USAGE_ACCOUNT_SCAN", "1").lower()
        not in {"0", "false", "no"},
        cockpit_tools_data_dir=cockpit_tools_data_dir,
        cockpit_tools_localstorage_db=cockpit_tools_localstorage_db,
        cockpit_tools_enabled=cockpit_tools_import_enabled,
        cockpit_tools_authoritative_accounts=cockpit_tools_authoritative_accounts,
    )
    if account_resolver is not None:
        resolver.configure_cockpit_sources(
            data_dir=cockpit_tools_data_dir,
            localstorage_db=cockpit_tools_localstorage_db,
            enabled=cockpit_tools_import_enabled,
            authoritative_accounts=cockpit_tools_authoritative_accounts,
        )
    repo.reconcile_auth_identities(resolver)
    repo.apply_privacy_minimization(resolver)
    return MeterHTTPServer(
        (host, port),
        repo,
        upstream,
        resolver,
        upstream_timeout,
        management_key_file=management_key_file,
        management_key_env=management_key_env,
        usage_queue_path=usage_queue_path,
        usage_queue_count=usage_queue_count,
        usage_queue_poll_seconds=usage_queue_poll_seconds,
        usage_queue_timeout=usage_queue_timeout,
        quota_routing_guard_enabled=quota_routing_guard_enabled,
        quota_routing_guard_state_file=quota_routing_guard_state_file,
        quota_routing_guard_timeout=quota_routing_guard_timeout,
        quota_poll_seconds=quota_poll_seconds,
        quota_poll_timeout=quota_poll_timeout,
        codex_app_home=codex_app_home,
        codex_app_alias=codex_app_alias,
        codex_app_poll_seconds=codex_app_poll_seconds,
        codex_app_max_files=codex_app_max_files,
        codex_app_import_enabled=codex_app_import_enabled,
        cockpit_tools_data_dir=cockpit_tools_data_dir,
        cockpit_tools_localstorage_db=cockpit_tools_localstorage_db,
        cockpit_tools_poll_seconds=cockpit_tools_poll_seconds,
        cockpit_tools_import_enabled=cockpit_tools_import_enabled,
        cockpit_tools_authoritative_accounts=cockpit_tools_authoritative_accounts,
        sub2api_base_url=sub2api_base_url,
        sub2api_admin_key_file=sub2api_admin_key_file,
        sub2api_admin_key_env=sub2api_admin_key_env,
        sub2api_poll_seconds=sub2api_poll_seconds,
        sub2api_timeout=sub2api_timeout,
        sub2api_page_size=sub2api_page_size,
        sub2api_backfill_days=sub2api_backfill_days,
        sub2api_import_enabled=sub2api_import_enabled,
    )


def print_rows(rows: Sequence[Mapping[str, Any]], json_output: bool = False) -> None:
    if json_output:
        print(json.dumps(list(rows), ensure_ascii=False, indent=2, default=str))
        return
    if not rows:
        print("No data.")
        return
    columns = list(rows[0].keys())
    rendered = [["—" if row.get(column) is None else str(row.get(column)) for column in columns] for row in rows]
    widths = [
        min(48, max(len(column), *(len(row[index]) for row in rendered))) for index, column in enumerate(columns)
    ]
    print("  ".join(column[: widths[index]].ljust(widths[index]) for index, column in enumerate(columns)))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(value[: widths[index]].ljust(widths[index]) for index, value in enumerate(row)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local-first Codex usage, cost, and quota observatory"
    )
    parser.add_argument(
        "--db", default=os.environ.get("CLIPROXY_USAGE_DB", str(DEFAULT_DB)), help="SQLite database path"
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON for queries")
    actions = parser.add_mutually_exclusive_group(required=False)
    actions.add_argument("--serve", action="store_true", help="Run the sidecar HTTP server")
    actions.add_argument("--summary", metavar="PERIOD", help="Summary for today, all, or Nd")
    actions.add_argument("--by-account", metavar="PERIOD", help="Group by account/alias (today, all, or Nd)")
    actions.add_argument("--by-model", metavar="PERIOD", help="Group by model (today, all, or Nd)")
    actions.add_argument("--by-session", metavar="PERIOD", help="Group by session (today, all, or Nd)")
    actions.add_argument("--by-date", metavar="PERIOD", help="Group by local date (today, all, or Nd)")
    actions.add_argument("--recent", metavar="N", type=int, help="Show recent N calls")
    actions.add_argument("--quota-summary", metavar="PERIOD", help="Quota-cycle summary for Nd")
    actions.add_argument("--quota-summary-by-account", action="store_true", help="Quota summary by account (30d)")
    actions.add_argument("--mark-reset", metavar="ALIAS", help="Mark a reset for an alias")
    actions.add_argument("--mark-quota-hit", metavar="ALIAS", help="Mark a quota hit for an alias")
    actions.add_argument(
        "--set-price", nargs=3, metavar=("MODEL_PATTERN", "INPUT_PER_M", "OUTPUT_PER_M"), help="Set a USD pricing row"
    )
    actions.add_argument("--list-prices", action="store_true", help="List configured model prices")
    actions.add_argument(
        "--sync-official-prices",
        action="store_true",
        help="Fetch and safely sync the standard token prices from the official OpenAI pricing page",
    )
    actions.add_argument(
        "--price-sync-status", action="store_true", help="Show the latest official pricing sync metadata"
    )
    parser.add_argument("--host", default=os.environ.get("CLIPROXY_USAGE_HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", DEFAULT_PORT)))
    parser.add_argument("--upstream", default=os.environ.get("UPSTREAM", DEFAULT_UPSTREAM))
    parser.add_argument("--upstream-timeout", type=float, default=float(os.environ.get("UPSTREAM_TIMEOUT", "900")))
    parser.add_argument(
        "--management-key-file",
        default=os.environ.get("CLIPROXY_MANAGEMENT_KEY_FILE", ""),
        help="Owner-only file containing the CLIProxyAPI management key (never logged)",
    )
    parser.add_argument(
        "--management-key-env",
        default=os.environ.get("CLIPROXY_MANAGEMENT_KEY_ENV", "CLIPROXY_MANAGEMENT_KEY"),
        help="Environment variable name for the management key (file is preferred)",
    )
    parser.add_argument(
        "--usage-queue-path",
        default=os.environ.get("CLIPROXY_USAGE_QUEUE_PATH", DEFAULT_USAGE_QUEUE_PATH),
        help="CLIProxyAPI management usage-queue path",
    )
    parser.add_argument(
        "--usage-queue-count",
        type=int,
        default=int(os.environ.get("CLIPROXY_USAGE_QUEUE_COUNT", str(DEFAULT_USAGE_QUEUE_COUNT))),
    )
    parser.add_argument(
        "--usage-queue-poll-seconds",
        type=float,
        default=float(os.environ.get("CLIPROXY_USAGE_QUEUE_POLL_SECONDS", str(DEFAULT_USAGE_QUEUE_POLL_SECONDS))),
    )
    parser.add_argument(
        "--usage-queue-timeout",
        type=float,
        default=float(os.environ.get("CLIPROXY_USAGE_QUEUE_TIMEOUT", "10")),
    )
    parser.add_argument(
        "--quota-routing-guard",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("CLIPROXY_QUOTA_ROUTING_GUARD", "0").lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Set weight zero only after an exact usage_limit_reached 429, then restore it "
            "at the provider reset deadline (requires weighted-round-robin)"
        ),
    )
    parser.add_argument(
        "--quota-routing-guard-state-file",
        default=os.environ.get(
            "CLIPROXY_QUOTA_ROUTING_GUARD_STATE_FILE",
            str(DEFAULT_QUOTA_GUARD_STATE_FILE),
        ),
        help="Owner-only state file for opaque quota routing locks",
    )
    parser.add_argument(
        "--quota-routing-guard-timeout",
        type=float,
        default=float(os.environ.get("CLIPROXY_QUOTA_ROUTING_GUARD_TIMEOUT", "10")),
        help="Loopback management timeout used by the quota routing guard",
    )
    parser.add_argument(
        "--quota-poll-seconds",
        type=float,
        default=float(os.environ.get("CLIPROXY_QUOTA_POLL_SECONDS", str(DEFAULT_QUOTA_POLL_SECONDS))),
        help="Seconds between read-only Codex subscription quota snapshots (minimum 60)",
    )
    parser.add_argument(
        "--quota-poll-timeout",
        type=float,
        default=float(os.environ.get("CLIPROXY_QUOTA_POLL_TIMEOUT", str(DEFAULT_QUOTA_POLL_TIMEOUT))),
    )
    parser.add_argument(
        "--codex-app-home",
        default=os.environ.get("CODEX_APP_HOME", str(DEFAULT_CODEX_APP_HOME)),
        help="ChatGPT/Codex local home whose safe token_count metadata is imported",
    )
    parser.add_argument(
        "--codex-app-alias",
        default=os.environ.get("CODEX_APP_USAGE_ALIAS"),
        help=(
            "Optional existing alias for strict single-home matching; omitted by "
            "default to follow current Cockpit/Codex accounts dynamically"
        ),
    )
    parser.add_argument(
        "--codex-app-poll-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CODEX_APP_USAGE_POLL_SECONDS", str(DEFAULT_CODEX_APP_POLL_SECONDS)
            )
        ),
    )
    parser.add_argument(
        "--codex-app-max-files",
        type=int,
        default=int(
            os.environ.get("CODEX_APP_USAGE_MAX_FILES", str(DEFAULT_CODEX_APP_MAX_FILES))
        ),
        help="Maximum most-recent local session JSONL files scanned per poll",
    )
    parser.add_argument(
        "--no-codex-app-import",
        action="store_true",
        help="Disable read-only import from local ChatGPT/Codex session metadata",
    )
    parser.add_argument(
        "--cockpit-tools-data-dir",
        default=os.environ.get("COCKPIT_TOOLS_DATA_DIR"),
        help=(
            "Cockpit Tools data directory (defaults to ~/.antigravity_cockpit)"
        ),
    )
    parser.add_argument(
        "--cockpit-tools-localstorage-db",
        default=os.environ.get("COCKPIT_TOOLS_LOCALSTORAGE_DB"),
        help="Optional Cockpit WebKit LocalStorage SQLite override",
    )
    parser.add_argument(
        "--cockpit-tools-poll-seconds",
        type=float,
        default=float(
            os.environ.get(
                "COCKPIT_TOOLS_USAGE_POLL_SECONDS",
                str(DEFAULT_COCKPIT_TOOLS_POLL_SECONDS),
            )
        ),
        help="Seconds between read-only Cockpit request-log imports",
    )
    parser.add_argument(
        "--no-cockpit-tools-import",
        action="store_true",
        help="Disable read-only Cockpit Tools request/quota import",
    )
    parser.add_argument(
        "--cockpit-tools-authoritative-accounts",
        action="store_true",
        default=os.environ.get(
            "COCKPIT_TOOLS_AUTHORITATIVE_ACCOUNTS", "0"
        ).lower()
        in {"1", "true", "yes"},
        help=(
            "Treat a complete Cockpit account inventory as authoritative and "
            "hide CLIProxyAPI-only credentials"
        ),
    )
    parser.add_argument(
        "--sub2api-base-url",
        default=os.environ.get("SUB2API_BASE_URL", DEFAULT_SUB2API_BASE_URL),
        help="Loopback Sub2API origin (empty path or /api/v1)",
    )
    parser.add_argument(
        "--sub2api-admin-key-file",
        default=os.environ.get("SUB2API_ADMIN_KEY_FILE", ""),
        help="Owner-only file containing the Sub2API admin API key",
    )
    parser.add_argument(
        "--sub2api-admin-key-env",
        default=os.environ.get("SUB2API_ADMIN_KEY_ENV", "SUB2API_ADMIN_KEY"),
        help="Environment variable name for the Sub2API admin API key",
    )
    parser.add_argument(
        "--sub2api-poll-seconds",
        type=float,
        default=float(
            os.environ.get("SUB2API_POLL_SECONDS", str(DEFAULT_SUB2API_POLL_SECONDS))
        ),
        help="Seconds between read-only Sub2API account and usage imports",
    )
    parser.add_argument(
        "--sub2api-timeout",
        type=float,
        default=float(os.environ.get("SUB2API_TIMEOUT", str(DEFAULT_SUB2API_TIMEOUT))),
    )
    parser.add_argument(
        "--sub2api-page-size",
        type=int,
        default=int(os.environ.get("SUB2API_PAGE_SIZE", str(DEFAULT_SUB2API_PAGE_SIZE))),
    )
    parser.add_argument(
        "--sub2api-backfill-days",
        type=int,
        default=int(
            os.environ.get("SUB2API_BACKFILL_DAYS", str(DEFAULT_SUB2API_BACKFILL_DAYS))
        ),
        help="Initial Sub2API usage history window in days",
    )
    parser.add_argument(
        "--no-sub2api-import",
        action="store_true",
        default=os.environ.get("SUB2API_IMPORT_ENABLED", "1").lower()
        in {"0", "false", "no", "off"},
        help="Disable read-only Sub2API account and usage import",
    )
    parser.add_argument("--no-usage-queue", action="store_true", help="Disable direct 8317 usage-queue polling")
    parser.add_argument("--cached-input-price", type=float, help="Cached input USD/M; defaults to input price")
    parser.add_argument("--price-source-note", help="Human-readable provenance for a manually supplied price")
    parser.add_argument(
        "--official-pricing-url",
        default=os.environ.get("OPENAI_PRICING_URL", OFFICIAL_PRICING_URL),
        help="Official OpenAI pricing URL used by --sync-official-prices",
    )
    parser.add_argument(
        "--official-pricing-timeout",
        type=float,
        default=float(os.environ.get("OPENAI_PRICING_TIMEOUT", "20")),
        help="Network timeout in seconds for --sync-official-prices",
    )
    parser.add_argument("--no-account-scan", action="store_true", help="Disable read-only local Codex auth mapping")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if not any(
        (
            args.serve,
            args.summary,
            args.by_account,
            args.by_model,
            args.by_session,
            args.by_date,
            args.recent is not None,
            args.quota_summary,
            args.quota_summary_by_account,
            args.mark_reset,
            args.mark_quota_hit,
            args.set_price,
            args.list_prices,
            args.sync_official_prices,
            args.price_sync_status,
        )
    ):
        parser.print_help()
        return 2

    repo = UsageRepository(args.db)
    resolver = AccountResolver(
        enabled=not args.no_account_scan,
        cockpit_tools_data_dir=args.cockpit_tools_data_dir,
        cockpit_tools_localstorage_db=args.cockpit_tools_localstorage_db,
        cockpit_tools_enabled=not args.no_cockpit_tools_import,
        cockpit_tools_authoritative_accounts=(
            args.cockpit_tools_authoritative_accounts
        ),
    )
    rebound = repo.reconcile_auth_identities(resolver)
    if rebound:
        LOG.info("reconciled %d provisional auth identity event(s)", rebound)
    minimized = repo.apply_privacy_minimization(resolver)
    if minimized:
        LOG.info("migrated %d usage event(s) to private subscription identities", minimized)
    if args.serve:
        server = MeterHTTPServer(
            (args.host, args.port),
            repo,
            args.upstream,
            resolver,
            args.upstream_timeout,
            management_key_file=None if args.no_usage_queue else (args.management_key_file or None),
            management_key_env="" if args.no_usage_queue else args.management_key_env,
            usage_queue_path=args.usage_queue_path,
            usage_queue_count=args.usage_queue_count,
            usage_queue_poll_seconds=args.usage_queue_poll_seconds,
            usage_queue_timeout=args.usage_queue_timeout,
            quota_routing_guard_enabled=(
                args.quota_routing_guard and not args.no_usage_queue
            ),
            quota_routing_guard_state_file=args.quota_routing_guard_state_file,
            quota_routing_guard_timeout=args.quota_routing_guard_timeout,
            quota_poll_seconds=args.quota_poll_seconds,
            quota_poll_timeout=args.quota_poll_timeout,
            codex_app_home=args.codex_app_home,
            codex_app_alias=args.codex_app_alias,
            codex_app_poll_seconds=args.codex_app_poll_seconds,
            codex_app_max_files=args.codex_app_max_files,
            codex_app_import_enabled=not args.no_codex_app_import,
            cockpit_tools_data_dir=args.cockpit_tools_data_dir,
            cockpit_tools_localstorage_db=args.cockpit_tools_localstorage_db,
            cockpit_tools_poll_seconds=args.cockpit_tools_poll_seconds,
            cockpit_tools_import_enabled=not args.no_cockpit_tools_import,
            cockpit_tools_authoritative_accounts=(
                args.cockpit_tools_authoritative_accounts
            ),
            sub2api_base_url=args.sub2api_base_url,
            sub2api_admin_key_file=(args.sub2api_admin_key_file or None),
            sub2api_admin_key_env=args.sub2api_admin_key_env,
            sub2api_poll_seconds=args.sub2api_poll_seconds,
            sub2api_timeout=args.sub2api_timeout,
            sub2api_page_size=args.sub2api_page_size,
            sub2api_backfill_days=args.sub2api_backfill_days,
            sub2api_import_enabled=not args.no_sub2api_import,
        )
        LOG.info(
            "usage meter listening on http://%s:%d; upstream=%s; db=%s",
            args.host,
            server.server_address[1],
            args.upstream,
            repo.path,
        )
        if args.quota_routing_guard and not args.no_usage_queue:
            LOG.info(
                "confirmed quota routing guard enabled; provider percentage alone never locks a credential"
            )
        if not args.no_usage_queue:
            server.start_queue_poller()
        else:
            server.start_local_importer()
        shutdown_started = threading.Event()

        def request_shutdown(_signum: int, _frame: Any) -> None:
            if shutdown_started.is_set():
                return
            shutdown_started.set()
            threading.Thread(
                target=server.shutdown,
                name="cliproxy-usage-shutdown",
                daemon=True,
            ).start()

        previous_handlers: dict[int, Any] = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, request_shutdown)
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
        return 0
    if args.summary:
        result = repo.summary(args.summary)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            for key, value in result.items():
                print(f"{key}: {value if value is not None else '—'}")
        return 0
    if args.by_account:
        print_rows(repo.grouped(args.by_account, "account"), args.json)
        return 0
    if args.by_model:
        print_rows(repo.grouped(args.by_model, "model"), args.json)
        return 0
    if args.by_session:
        print_rows(repo.grouped(args.by_session, "session"), args.json)
        return 0
    if args.by_date:
        print_rows(repo.grouped(args.by_date, "date"), args.json)
        return 0
    if args.recent is not None:
        print_rows(repo.recent(args.recent), args.json)
        return 0
    if args.quota_summary or args.quota_summary_by_account:
        print_rows(repo.quota_summary(args.quota_summary or "30d"), args.json)
        return 0
    if args.mark_reset:
        try:
            key = repo.mark_reset(args.mark_reset, resolver)
        except ValueError as exc:
            print(f"mark reset failed: {exc}", file=sys.stderr)
            return 1
        print(f"marked reset: alias={args.mark_reset} identity={key}")
        return 0
    if args.mark_quota_hit:
        try:
            key = repo.mark_quota_hit(args.mark_quota_hit, resolver)
        except ValueError as exc:
            print(f"mark quota hit failed: {exc}", file=sys.stderr)
            return 1
        print(f"marked quota hit: alias={args.mark_quota_hit} identity={key}")
        return 0
    if args.set_price:
        pattern, input_value, output_value = args.set_price
        input_rate = float(input_value)
        output_rate = float(output_value)
        cached_rate = args.cached_input_price if args.cached_input_price is not None else input_rate
        repo.set_price(pattern, input_rate, output_rate, cached_rate, args.price_source_note)
        print(f"price configured for {pattern}; USD per million input={input_rate}, cached={cached_rate}, output={output_rate}")
        return 0
    if args.list_prices:
        print_rows(repo.list_prices(), args.json)
        return 0
    if args.price_sync_status:
        result = repo.price_sync_status()
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            for key, value in result.items():
                print(f"{key}: {value if value is not None else '—'}")
        return 0
    if args.sync_official_prices:
        try:
            result = sync_official_prices(
                repo,
                url=args.official_pricing_url,
                timeout=args.official_pricing_timeout,
            )
        except OfficialPriceSyncError as exc:
            print(f"official price sync failed: {exc}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            print(
                f"official prices synced: {result['model_count']} models; "
                f"repriced_events={result['repriced_events']}; "
                f"sha256={result['content_sha256']} fetched_at={result['fetched_at']}"
            )
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
