from __future__ import annotations

from datetime import datetime, timedelta, timezone
import http.client
import importlib.util
import hashlib
import base64
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "cliproxy_usage_meter.py"
SPEC = importlib.util.spec_from_file_location("cliproxy_usage_meter", SCRIPT)
assert SPEC and SPEC.loader
meter = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = meter
SPEC.loader.exec_module(meter)
CHROME_HELPER = ROOT / "scripts" / "start_cliproxy_usage_meter_from_chrome.py"
CHROME_SPEC = importlib.util.spec_from_file_location("cliproxy_chrome_key", CHROME_HELPER)
assert CHROME_SPEC and CHROME_SPEC.loader
chrome_key = importlib.util.module_from_spec(CHROME_SPEC)
sys.modules[CHROME_SPEC.name] = chrome_key
CHROME_SPEC.loader.exec_module(chrome_key)


AUTH_SECRET = "fixture-auth-value"
ERROR_SECRET = "fixture-error-value"


def fixture_jwt(claims: dict[str, object]) -> str:
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"fixture.{payload}.signature"


def subscription_key(label: str) -> str:
    return "subscription:" + hashlib.sha256(label.encode()).hexdigest()[:32]


class FakeUpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    auth_forwarded = False
    usage_header_forwarded = False
    management_auth_forwarded = False
    management_calls = 0
    management_status = 200
    queue_payload: list[object] = []

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/v0/management/usage-queue"):
            type(self).management_calls += 1
            type(self).management_auth_forwarded = self.headers.get("Authorization") == "Bearer management-fixture"
            if type(self).management_status != 200:
                self._json(type(self).management_status, {"error": "management unavailable"})
                return
            payload = type(self).queue_payload
            type(self).queue_payload = []
            self._json(200, payload)
            return
        if self.path.startswith("/v1/models"):
            self._json(200, {"object": "list", "data": [], "requested_path": self.path})
            return
        self._json(404, {"error": {"message": "not found", "type": "not_found"}})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        try:
            request = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            request = {}
        type(self).auth_forwarded = self.headers.get("Authorization") == f"Bearer {AUTH_SECRET}"
        type(self).usage_header_forwarded = "X-Usage-Alias" in self.headers

        if self.path == "/v1/responses":
            self._json(
                200,
                {
                    "id": "resp_fake",
                    "model": request.get("model", "fake-responses"),
                    "usage": {
                        "input_tokens": 100,
                        "output_tokens": 50,
                        "total_tokens": 150,
                        "input_tokens_details": {"cached_tokens": 20},
                        "output_tokens_details": {"reasoning_tokens": 10},
                    },
                },
            )
            return
        if self.path == "/v1/chat/completions":
            self._json(
                200,
                {
                    "id": "chat_fake",
                    "model": request.get("model", "fake-chat"),
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 30,
                        "completion_tokens": 10,
                        "total_tokens": 40,
                        "prompt_tokens_details": {"cached_tokens": 5},
                        "completion_tokens_details": {"reasoning_tokens": 4},
                    },
                },
            )
            return
        if self.path == "/v1/no-usage":
            self._json(200, {"id": "no_usage", "model": request.get("model", "fake-missing")})
            return
        if self.path == "/v1/fail":
            self._json(
                429,
                {
                    "error": {
                        "type": "rate_limit_error",
                        "message": f"usage limit reached; Authorization: Bearer {AUTH_SECRET}; api_key={ERROR_SECRET}",
                    }
                },
            )
            return
        if self.path == "/v1/stream":
            chunks = [
                b'data: {"type":"response.created","response":{"model":"fake-stream"}}\n\n',
                b'data: {"type":"response.completed","response":{"model":"fake-stream","usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}}}}\n\n',
                b"data: [DONE]\n\n",
            ]
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            for chunk in chunks:
                self.wfile.write(chunk)
                self.wfile.flush()
                time.sleep(0.01)
            self.close_connection = True
            return
        if self.path == "/v1/stream-no-usage":
            body = b'data: {"type":"response.output_text.delta","delta":"hello"}\n\ndata: [DONE]\n\n'
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            self.close_connection = True
            return
        if self.path == "/v1/stream-slow":
            first = b'data: {"type":"response.output_text.delta","delta":"first"}\n\n'
            final = b'data: {"type":"response.completed","response":{"model":"fake-stream","usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}\n\ndata: [DONE]\n\n'
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(first)
            self.wfile.flush()
            time.sleep(0.35)
            self.wfile.write(final)
            self.wfile.flush()
            self.close_connection = True
            return
        self._json(404, {"error": {"message": "not found", "type": "not_found"}})

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class UsageMeterMVPTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp.name)
        self.db = self.temp_path / "usage.sqlite"

        fake_home = self.temp_path / "home"
        codex_home = fake_home / ".codex-c"
        proxy_home = fake_home / ".cli-proxy-api"
        codex_home.mkdir(parents=True)
        proxy_home.mkdir(parents=True)
        (fake_home / ".zshrc").write_text(
            'alias codex-1=\'__codex_switch "$HOME/.codex-c" codex-1\'\n', encoding="utf-8"
        )
        (codex_home / "auth.json").write_text(
            json.dumps(
                {
                    "auth_mode": "chatgpt",
                    "tokens": {
                        "account_id": "acct-unit-test-ABCDEFGH",
                        "access_token": AUTH_SECRET,
                        "id_token": fixture_jwt({"email": "fixture@example.com"}),
                    },
                }
            ),
            encoding="utf-8",
        )

        FakeUpstreamHandler.auth_forwarded = False
        FakeUpstreamHandler.usage_header_forwarded = False
        FakeUpstreamHandler.management_auth_forwarded = False
        FakeUpstreamHandler.management_calls = 0
        FakeUpstreamHandler.management_status = 200
        FakeUpstreamHandler.queue_payload = []
        self.fake = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstreamHandler)
        self.fake_thread = threading.Thread(target=self.fake.serve_forever, daemon=True)
        self.fake_thread.start()

        resolver = meter.AccountResolver(home=fake_home, refresh_seconds=0)
        self.sidecar = meter.create_server(
            "127.0.0.1",
            0,
            f"http://127.0.0.1:{self.fake.server_address[1]}",
            self.db,
            account_resolver=resolver,
            upstream_timeout=5,
        )
        self.sidecar.repo.set_price("fake-*", 1.0, 2.0, 0.5, "unit test only")
        self.sidecar_thread = threading.Thread(target=self.sidecar.serve_forever, daemon=True)
        self.sidecar_thread.start()
        self.port = self.sidecar.server_address[1]

    def tearDown(self) -> None:
        self.sidecar.shutdown()
        self.sidecar.server_close()
        self.sidecar_thread.join(timeout=2)
        self.fake.shutdown()
        self.fake.server_close()
        self.fake_thread.join(timeout=2)
        self.temp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        payload: object | None = None,
        *,
        alias: str | None = "codex-1",
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Authorization": f"Bearer {AUTH_SECRET}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if alias:
            headers["X-Usage-Alias"] = alias
            headers["X-Usage-Project"] = "unit-test"
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        response_body = response.read()
        result = (response.status, response.getheaders(), response_body)
        connection.close()
        return result

    def wait_events(self, expected: int, timeout: float = 2.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with sqlite3.connect(self.db) as conn:
                count = conn.execute("SELECT COUNT(*) FROM usage_events").fetchone()[0]
            if count >= expected:
                return
            time.sleep(0.01)
        self.fail(f"timed out waiting for {expected} usage events")

    def rows(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(sql, params).fetchall()

    @staticmethod
    def codex_auth(account: str, email: str, principal: str) -> dict[str, object]:
        return {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account,
                "id_token": fixture_jwt({"email": email, "sub": principal}),
            },
        }

    @staticmethod
    def codex_token_record(
        timestamp: str,
        ordinal: int,
        input_tokens: int,
    ) -> dict[str, object]:
        return {
            "timestamp": timestamp,
            "ordinal": ordinal,
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": input_tokens,
                        "cached_input_tokens": 1,
                        "output_tokens": 2,
                        "total_tokens": input_tokens + 2,
                    }
                },
                "rate_limits": {
                    "plan_type": "pro",
                    "primary": {
                        "used_percent": 20,
                        "window_minutes": 300,
                    },
                },
            },
        }

    def shared_workspace_fixture(self) -> dict[str, object]:
        """Create two user principals in one synthetic Team workspace."""

        home = self.temp_path / "shared-workspace-home"
        codex_home = home / ".codex-target"
        proxy_home = home / ".cli-proxy-api"
        codex_home.mkdir(parents=True)
        proxy_home.mkdir(parents=True)
        (home / ".zshrc").write_text(
            'alias codex-2=\'__codex_switch "$HOME/.codex-target" codex-2\'\n',
            encoding="utf-8",
        )

        shared_account = "acct-fixture-shared-workspace"
        principals = {
            "a": {
                "file": "member-alpha@example.cpa.2026-08-14_23-38-37.json",
                "email": "member-alpha@example.test",
                "sub": "fixture-subject-alpha",
                "user_id": "fixture-user-alpha",
                "token": "fixture-access-alpha",
            },
            "b": {
                "file": "member-beta@example.cpa.2026-08-14_22-05-22.json",
                "email": "member-beta@example.test",
                "sub": "fixture-subject-beta",
                "user_id": "fixture-user-beta",
                "token": "fixture-access-beta",
            },
        }
        for principal in principals.values():
            auth_claims = {
                "chatgpt_account_id": shared_account,
                "chatgpt_user_id": principal["user_id"],
                "user_id": principal["user_id"],
            }
            principal["id_token"] = fixture_jwt(
                {
                    "sub": principal["sub"],
                    "email": principal["email"],
                    "https://api.openai.com/auth": auth_claims,
                }
            )
            (proxy_home / str(principal["file"])).write_text(
                json.dumps(
                    {
                        "type": "codex",
                        "account_id": shared_account,
                        "access_token": principal["token"],
                        "id_token": principal["id_token"],
                        "email": principal["email"],
                        "plan_type": "team",
                    }
                ),
                encoding="utf-8",
            )

        target = principals["a"]
        (codex_home / "auth.json").write_text(
            json.dumps(
                {
                    "auth_mode": "chatgpt",
                    "tokens": {
                        "account_id": shared_account,
                        "access_token": target["token"],
                        "id_token": target["id_token"],
                    },
                }
            ),
            encoding="utf-8",
        )
        return {
            "home": home,
            "shared_account": shared_account,
            "principals": principals,
            "resolver": meter.AccountResolver(home=home, refresh_seconds=0),
        }

    def queue_identity(
        self,
        resolver: meter.AccountResolver,
        principal: dict[str, object],
        request_id: str,
        *,
        digest_token: str | None = None,
    ) -> tuple[meter.UsageEvent, meter.RequestInfo]:
        token = digest_token if digest_token is not None else str(principal["token"])
        result = meter.queue_record_event(
            {
                "timestamp": "2026-08-15T00:00:00Z",
                "auth_index": principal["file"],
                "access_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
                "model": "fixture-model",
                "endpoint": "/v1/responses",
                "tokens": {
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "total_tokens": 2,
                },
                "failed": False,
                "request_id": request_id,
            },
            resolver,
            self.sidecar.repo,
        )
        self.assertIsNotNone(result)
        assert result is not None
        return result

    def test_responses_non_streaming_usage_cost_and_account_mapping(self) -> None:
        status, _, body = self.request(
            "POST",
            "/v1/responses",
            {
                "model": "fake-responses",
                "input": "hello",
                "client_metadata": {
                    "session_id": "session-A",
                    "thread_id": "thread-A",
                    "turn_id": "turn-A",
                    "x-codex-installation-id": "install-A",
                    "x-codex-window-id": "window-A",
                },
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["id"], "resp_fake")
        self.wait_events(1)
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertEqual((row["input_tokens"], row["output_tokens"], row["total_tokens"]), (100, 50, 150))
        self.assertEqual((row["cached_tokens"], row["reasoning_tokens"]), (20, 10))
        self.assertAlmostEqual(row["estimated_api_cost_usd"], 0.00019, places=10)
        self.assertAlmostEqual(row["non_cached_input_cost_usd"], 0.00008, places=10)
        self.assertAlmostEqual(row["cached_input_cost_usd"], 0.00001, places=10)
        self.assertAlmostEqual(row["output_cost_usd"], 0.0001, places=10)
        self.assertAlmostEqual(
            row["non_cached_input_cost_usd"]
            + row["cached_input_cost_usd"]
            + row["output_cost_usd"],
            row["estimated_api_cost_usd"],
            places=12,
        )
        self.assertTrue(str(row["identity_key"]).startswith("subscription:"))
        self.assertIsNone(row["usage_alias"])
        self.assertIsNone(row["account_id_tail"])
        self.assertIsNone(row["account_id_hash"])
        self.assertEqual((row["session_id"], row["thread_id"], row["turn_id"]), (None, None, None))
        self.assertTrue(FakeUpstreamHandler.auth_forwarded)
        self.assertFalse(FakeUpstreamHandler.usage_header_forwarded)

    def test_chat_completions_normalization(self) -> None:
        status, _, _ = self.request(
            "POST", "/v1/chat/completions", {"model": "fake-chat", "messages": [], "stream": False}
        )
        self.assertEqual(status, 200)
        self.wait_events(1)
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertEqual((row["input_tokens"], row["output_tokens"], row["total_tokens"]), (30, 10, 40))
        self.assertEqual((row["cached_tokens"], row["reasoning_tokens"]), (5, 4))
        self.assertAlmostEqual(row["estimated_api_cost_usd"], 0.0000475, places=10)

    def test_frozen_component_costs_do_not_follow_later_price_changes(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(1)
        before = self.sidecar.repo.summary("all")
        self.sidecar.repo.set_price("fake-*", 9.0, 40.0, 0.9, "changed fixture price")
        after = self.sidecar.repo.summary("all")
        for key in (
            "estimated_api_cost_usd",
            "non_cached_input_cost_usd",
            "cached_input_cost_usd",
            "output_cost_usd",
            "split_cost_total_usd",
        ):
            self.assertEqual(after[key], before[key], key)
        self.assertAlmostEqual(after["split_cost_total_usd"], 0.00019, places=12)

    def test_legacy_cost_component_backfill_requires_frozen_total_match(self) -> None:
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, model, input_tokens, cached_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count)
                   VALUES ('2020-01-01T00:00:00.000000Z', 'fake-responses',
                           100, 20, 50, 150, 0.00019, 1)"""
            )
            conn.execute(
                """INSERT INTO usage_events
                   (ts, model, input_tokens, cached_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count)
                   VALUES ('2020-01-01T00:00:01.000000Z', 'fake-responses',
                           100, 20, 50, 150, 123.0, 1)"""
            )
        self.assertEqual(self.sidecar.repo.backfill_frozen_cost_components(), 1)
        rows = self.rows(
            """SELECT estimated_api_cost_usd, non_cached_input_cost_usd,
                      cached_input_cost_usd, output_cost_usd
                 FROM usage_events ORDER BY id"""
        )
        self.assertAlmostEqual(rows[0]["non_cached_input_cost_usd"], 0.00008, places=12)
        self.assertAlmostEqual(rows[0]["cached_input_cost_usd"], 0.00001, places=12)
        self.assertAlmostEqual(rows[0]["output_cost_usd"], 0.0001, places=12)
        self.assertEqual(rows[1]["estimated_api_cost_usd"], 123.0)
        self.assertIsNone(rows[1]["non_cached_input_cost_usd"])
        self.assertIsNone(rows[1]["cached_input_cost_usd"])
        self.assertIsNone(rows[1]["output_cost_usd"])
        self.assertEqual(self.sidecar.repo.backfill_frozen_cost_components(), 0)

    def test_repository_initialization_migrates_legacy_schema_and_backfills_safely(self) -> None:
        legacy_db = self.temp_path / "legacy.sqlite"
        with sqlite3.connect(legacy_db) as conn:
            conn.executescript(
                """
                CREATE TABLE usage_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT NOT NULL,
                  usage_alias TEXT,
                  model TEXT,
                  input_tokens INTEGER,
                  cached_tokens INTEGER,
                  output_tokens INTEGER,
                  estimated_api_cost_usd REAL,
                  call_count INTEGER DEFAULT 1
                );
                CREATE TABLE model_prices (
                  model_pattern TEXT PRIMARY KEY,
                  input_per_million REAL,
                  output_per_million REAL,
                  cached_input_per_million REAL,
                  reasoning_per_million REAL,
                  currency TEXT,
                  source_note TEXT,
                  source_kind TEXT,
                  updated_at TEXT
                );
                INSERT INTO model_prices VALUES
                  ('legacy-model', 5.0, 30.0, 0.5, NULL, 'USD', 'fixture', 'manual', '2020');
                INSERT INTO usage_events
                  (ts, model, input_tokens, cached_tokens, output_tokens,
                   estimated_api_cost_usd, call_count)
                VALUES
                  ('2020-01-01T00:00:00.000000Z', 'legacy-model',
                   1000000, 500000, 100000, 5.75, 1);
                """
            )
        meter.UsageRepository(legacy_db)
        with sqlite3.connect(legacy_db) as conn:
            conn.row_factory = sqlite3.Row
            columns = {row[1] for row in conn.execute("PRAGMA table_info(usage_events)")}
            row = conn.execute("SELECT * FROM usage_events").fetchone()
        self.assertTrue(
            {"non_cached_input_cost_usd", "cached_input_cost_usd", "output_cost_usd"}
            <= columns
        )
        self.assertAlmostEqual(row["non_cached_input_cost_usd"], 2.5, places=12)
        self.assertAlmostEqual(row["cached_input_cost_usd"], 0.25, places=12)
        self.assertAlmostEqual(row["output_cost_usd"], 3.0, places=12)
        self.assertAlmostEqual(row["estimated_api_cost_usd"], 5.75, places=12)

    def test_cockpit_quota_migration_classifies_window_by_duration(self) -> None:
        fixture_db = self.temp_path / "cockpit-window-migration.sqlite"
        repo = meter.UsageRepository(fixture_db)
        identity_key = subscription_key("cockpit-window-migration")
        with sqlite3.connect(fixture_db) as conn:
            conn.execute(
                """INSERT INTO subscription_quota_snapshots (
                     fetched_at, identity_key, window_kind, used_percent,
                     remaining_percent, window_seconds, source
                   ) VALUES (?, ?, 'five_hour', 100, 0, 604800,
                             'cockpit_tools_quota')""",
                ("2026-08-17T20:19:11Z", identity_key),
            )

        # Re-opening an upgraded meter repairs snapshots written by the old
        # importer, whose legacy ``hourly`` label was not authoritative.
        repo.initialize()
        with sqlite3.connect(fixture_db) as conn:
            row = conn.execute(
                """SELECT window_kind, window_seconds
                     FROM subscription_quota_snapshots"""
            ).fetchone()
        self.assertEqual(row, ("weekly", 604800))

    def test_auth_fingerprint_maps_account_without_alias_and_unpriced_stays_null(self) -> None:
        status, _, _ = self.request(
            "POST", "/v1/responses", {"model": "unpriced-model", "input": "hello"}, alias=None
        )
        self.assertEqual(status, 200)
        self.wait_events(1)
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertTrue(str(row["identity_key"]).startswith("subscription:"))
        self.assertIsNone(row["usage_alias"])
        self.assertIsNone(row["account_id_tail"])
        self.assertIsNone(row["auth_fingerprint"])
        self.assertIsNone(row["estimated_api_cost_usd"])
        self.assertIsNone(
            self.sidecar.repo.price_for("fake-responses", meter.NormalizedUsage(total_tokens=3))
        )

    def test_reconcile_provisional_auth_identity_after_auth_file_refresh(self) -> None:
        fingerprint = meter.short_hash(AUTH_SECRET)
        assert fingerprint
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, usage_alias, auth_fingerprint, model,
                    status_code, ok, call_count, usage_missing)
                   VALUES ('2026-08-12T00:00:00.000000Z', ?, ?, ?,
                           'fake-responses', 400, 0, 1, 1)""",
                (f"alias:auth:{fingerprint}", f"auth:{fingerprint}", fingerprint),
            )

        self.assertEqual(
            self.sidecar.resolver.resolve(f"auth:{fingerprint}", fingerprint).usage_alias,
            "codex-1",
        )
        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(self.sidecar.resolver), 1)
        row = self.rows("SELECT * FROM usage_events")[0]
        expected_hash = meter.short_hash("acct-unit-test-ABCDEFGH")
        expected_key = meter.resolved_identity_key(
            self.sidecar.resolver.resolve("codex-1", None)
        )
        self.assertEqual(row["identity_key"], expected_key)
        self.assertIsNone(row["usage_alias"])
        self.assertIsNone(row["auth_fingerprint"])
        self.assertIsNone(row["account_id_hash"])
        self.assertIsNone(row["account_id_tail"])
        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(self.sidecar.resolver), 0)

        # A rotated token may no longer be present in the auth file. A shared
        # workspace hash is not member evidence, so this row must never be
        # guessed into the only currently visible Team member.
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, usage_alias, auth_fingerprint,
                    account_id_hash, account_id_tail, model,
                    status_code, ok, call_count, usage_missing)
                   VALUES ('2026-08-12T00:00:01.000000Z', ?, ?, ?, ?, ?,
                           'fake-responses', 400, 0, 1, 1)""",
                (
                    "account:" + expected_hash,
                    "auth:deadbeefdeadbeef",
                    "deadbeefdeadbeef",
                    expected_hash,
                    "ABCDEFGH",
                ),
            )
        before = self.sidecar.repo.summary("all")
        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(self.sidecar.resolver), 0)
        rotated = self.rows(
            "SELECT usage_alias, auth_fingerprint, identity_key FROM usage_events ORDER BY id DESC"
        )[0]
        self.assertIsNone(rotated["usage_alias"])
        self.assertIsNone(rotated["auth_fingerprint"])
        self.assertEqual(rotated["identity_key"], "account:" + expected_hash)

        self.sidecar.repo.apply_privacy_minimization(self.sidecar.resolver)
        after = self.sidecar.repo.summary("all")
        self.assertEqual(after["calls"], before["calls"])
        self.assertEqual(after["total_tokens"], before["total_tokens"])
        self.assertEqual(
            self.rows("SELECT COUNT(*) AS count FROM usage_events WHERE identity_key=?", (expected_key,))[0]["count"],
            1,
        )
        self.assertEqual(
            self.rows("SELECT COALESCE(SUM(calls), 0) AS calls FROM anonymous_usage_daily")[0]["calls"],
            1,
        )

    def test_custom_named_team_auth_reconciles_usage_with_quota_card(self) -> None:
        proxy_home = self.temp_path / "home" / ".cli-proxy-api"
        team_token = "fixture-team-auth-value"
        team_account = "acct-team-fixture-QRSTUVWX"
        team_email = "team-fixture@example.test"
        (proxy_home / "workspace-auth-fixture.json").write_text(
            json.dumps(
                {
                    "type": "codex",
                    "account_id": team_account,
                    "access_token": team_token,
                    "email": team_email,
                    "plan_type": "team",
                    "name": "workspace-auth-fixture.json",
                }
            ),
            encoding="utf-8",
        )
        ignored_token = "fixture-unrelated-provider-value"
        (proxy_home / "unrelated-provider.json").write_text(
            json.dumps(
                {
                    "type": "gemini",
                    "account_id": "acct-unrelated-fixture",
                    "access_token": ignored_token,
                }
            ),
            encoding="utf-8",
        )

        identity = self.sidecar.resolver.resolve_queue(
            "opaque-team-auth-index",
            hashlib.sha256(team_token.encode()).hexdigest(),
        )
        expected_hash = meter.short_hash(team_account)
        self.assertEqual(identity.account_id_hash, expected_hash)
        self.assertEqual(identity.account_id_tail, "QRSTUVWX")
        self.assertEqual(identity.account_email, team_email)
        self.assertIsNone(identity.usage_alias)
        ignored = self.sidecar.resolver.resolve(None, meter.short_hash(ignored_token))
        self.assertIsNone(ignored.account_id_hash)

        fingerprint = meter.short_hash(team_token)
        assert fingerprint and expected_hash
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, usage_alias, auth_fingerprint, model,
                    status_code, ok, call_count, usage_missing)
                   VALUES ('2026-08-12T00:00:00.000000Z', ?, ?, ?,
                           'fixture-model', 200, 1, 7, 0)""",
                (f"alias:auth:{fingerprint}", f"auth:{fingerprint}", fingerprint),
            )
        # Simulate a workspace-only snapshot written by an older build.  New
        # writes reject this non-canonical identity at the repository boundary.
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO subscription_quota_snapshots (
                       fetched_at, identity_key, account_id_hash,
                       account_id_tail, plan_type, window_kind, used_percent,
                       remaining_percent, source
                     ) VALUES ('2026-08-12T00:05:00Z', ?, ?, 'QRSTUVWX',
                               'team', 'weekly', 25, 75, 'fixture')""",
                (f"account:{expected_hash}", expected_hash),
            )

        self.assertEqual(
            self.sidecar.repo.reconcile_auth_identities(self.sidecar.resolver),
            1,
        )
        # The token proves the usage row's member identity.  The old
        # workspace-only quota row does not, so privacy minimization must not
        # guess that it belongs to the currently visible Team member.
        self.sidecar.repo.apply_privacy_minimization(self.sidecar.resolver)
        subscriptions = self.sidecar.repo.subscription_dashboard_rows()
        canonical_key = meter.resolved_identity_key(identity)
        team_row = next(
            row
            for row in subscriptions
            if row["identity_key"] == canonical_key
        )
        self.assertIsNone(team_row["plan_type"])
        self.assertEqual(team_row["all_time_calls"], 7)
        self.assertEqual(team_row["all_time_successful_calls"], 7)
        self.assertEqual(team_row["all_time_failed_calls"], 0)
        self.assertIsNone(team_row["account_id_hash"])
        self.assertIsNone(team_row["account_id_tail"])
        self.assertFalse(
            any(
                str(row["identity_key"]).startswith("account:")
                for row in subscriptions
            )
        )
        self.assertIsNone(
            self.sidecar.resolver.resolve_account_hash(expected_hash).account_email
        )

    def test_shared_workspace_timestamped_auth_files_keep_distinct_stable_identities(self) -> None:
        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        principals = fixture["principals"]
        shared_account = fixture["shared_account"]
        assert isinstance(resolver, meter.AccountResolver)
        assert isinstance(principals, dict)
        principal_a = principals["a"]
        principal_b = principals["b"]

        identity_a = resolver.resolve_auth_file(str(principal_a["file"]))
        identity_b = resolver.resolve_auth_file(str(principal_b["file"]))
        self.assertEqual(identity_a.usage_alias, "codex-2")
        self.assertIsNone(identity_b.usage_alias)
        self.assertEqual(identity_a.account_email, principal_a["email"])
        self.assertEqual(identity_b.account_email, principal_b["email"])
        self.assertEqual(identity_a.account_id_hash, meter.short_hash(shared_account))
        self.assertEqual(identity_b.account_id_hash, meter.short_hash(shared_account))
        self.assertEqual(
            identity_a.principal_id_hash,
            meter.short_hash(str(principal_a["user_id"])),
        )
        self.assertEqual(
            identity_b.principal_id_hash,
            meter.short_hash(str(principal_b["user_id"])),
        )
        # Canonical subscription keys are keyed, domain-separated digests.
        # Their inputs must not be recoverable by recomputing a public short
        # hash from the workspace and JWT principal.  The old public digest is
        # retained in memory only so historical rows can be migrated.
        self.assertRegex(identity_a.subscription_id_hash or "", r"^[0-9a-f]{32}$")
        self.assertRegex(identity_b.subscription_id_hash or "", r"^[0-9a-f]{32}$")
        self.assertEqual(
            identity_a.legacy_subscription_id_hash,
            meter.short_hash(f"{shared_account}\0{principal_a['user_id']}"),
        )
        self.assertEqual(
            identity_b.legacy_subscription_id_hash,
            meter.short_hash(f"{shared_account}\0{principal_b['user_id']}"),
        )
        self.assertNotEqual(
            identity_a.subscription_id_hash,
            identity_a.legacy_subscription_id_hash,
        )
        self.assertNotEqual(
            identity_b.subscription_id_hash,
            identity_b.legacy_subscription_id_hash,
        )
        self.assertNotEqual(identity_a.principal_id_hash, identity_b.principal_id_hash)
        self.assertNotEqual(identity_a.subscription_id_hash, identity_b.subscription_id_hash)
        self.assertEqual(
            meter.resolved_identity_key(identity_a),
            f"subscription:{identity_a.subscription_id_hash}",
        )
        self.assertEqual(
            meter.resolved_identity_key(identity_b),
            f"subscription:{identity_b.subscription_id_hash}",
        )

        event_a, info_a = self.queue_identity(resolver, principal_a, "fixture-shared-a")
        event_b, info_b = self.queue_identity(resolver, principal_b, "fixture-shared-b")
        self.assertEqual(info_a.identity_key, event_a.identity_key)
        self.assertEqual(info_b.identity_key, event_b.identity_key)
        self.assertEqual(event_a.identity_key, f"subscription:{identity_a.subscription_id_hash}")
        self.assertEqual(event_b.identity_key, f"subscription:{identity_b.subscription_id_hash}")
        self.assertNotEqual(event_a.identity_key, event_b.identity_key)
        self.assertFalse(event_a.identity_key.startswith("auth:"))
        self.assertFalse(event_b.identity_key.startswith("auth:"))
        self.assertEqual(
            resolver.resolve_identity_key(event_a.identity_key),
            identity_a,
        )
        self.assertEqual(
            resolver.resolve_identity_key(event_b.identity_key),
            identity_b,
        )

        # A supplied cryptographic digest is authoritative: if it no longer
        # matches after rotation, never fall back to a reusable filename that
        # could now belong to another Team member.  Valid current digests stay
        # stable across resolver rebuilds.
        rotated_a, _ = self.queue_identity(
            resolver,
            principal_a,
            "fixture-shared-a-rotated",
            digest_token="fixture-rotated-alpha",
        )
        rebuilt = meter.AccountResolver(home=fixture["home"], refresh_seconds=0)
        rebuilt_a, _ = self.queue_identity(rebuilt, principal_a, "fixture-shared-a-rebuilt")
        rebuilt_b, _ = self.queue_identity(rebuilt, principal_b, "fixture-shared-b-rebuilt")
        self.assertEqual(rotated_a.identity_key, "unknown")
        self.assertEqual(rebuilt_a.identity_key, event_a.identity_key)
        self.assertEqual(rebuilt_b.identity_key, event_b.identity_key)

    def test_shared_workspace_quota_poller_keeps_reversed_auth_files_separate(self) -> None:
        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        principals = fixture["principals"]
        shared_account = fixture["shared_account"]
        assert isinstance(resolver, meter.AccountResolver)
        assert isinstance(principals, dict)
        principal_a = principals["a"]
        principal_b = principals["b"]
        event_a, _ = self.queue_identity(resolver, principal_a, "fixture-quota-a")
        event_b, _ = self.queue_identity(resolver, principal_b, "fixture-quota-b")

        auth_files = []
        auth_index_by_file = {
            str(principal_a["file"]): "opaque-auth-index-alpha",
            str(principal_b["file"]): "opaque-auth-index-beta",
        }
        active_until = {
            str(principal_a["file"]): "2026-09-14T09:59:00Z",
            str(principal_b["file"]): "2026-08-14T09:59:00Z",
        }
        # Intentionally return B before A.  Correctness must not depend on
        # filename order when both credentials share one Team workspace id.
        for principal in (principal_b, principal_a):
            filename = str(principal["file"])
            auth_files.append(
                {
                    "provider": "codex",
                    "type": "codex",
                    "auth_index": auth_index_by_file[filename],
                    "id": filename,
                    "name": filename,
                    "account": principal["email"],
                    "email": principal["email"],
                    "disabled": False,
                    "id_token": {
                        "chatgpt_account_id": shared_account,
                        "plan_type": "team",
                        "chatgpt_subscription_active_until": active_until[filename],
                    },
                }
            )

        quota_by_file = {
            str(principal_a["file"]): {
                "used_percent": 16,
                "reset_at": "2026-08-21T14:32:00Z",
            },
            str(principal_b["file"]): {
                "used_percent": 30,
                "reset_at": "2026-08-21T14:30:00Z",
            },
        }
        file_by_auth_index = {
            value: key for key, value in auth_index_by_file.items()
        }
        api_calls: list[dict[str, object]] = []

        def management_request(
            key: str,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> tuple[int, object]:
            self.assertEqual(key, "fixture-management")
            if method == "GET" and path == "/v0/management/auth-files":
                return 200, {"files": auth_files}
            self.assertEqual((method, path), ("POST", "/v0/management/api-call"))
            assert payload is not None
            api_calls.append(payload)
            auth_index = str(payload["auth_index"])
            quota = quota_by_file[file_by_auth_index[auth_index]]
            return 200, {
                "status_code": 200,
                "body": json.dumps(
                    {
                        "plan_type": "team",
                        "rate_limit": {
                            "secondary_window": {
                                "used_percent": quota["used_percent"],
                                "limit_window_seconds": 604800,
                                "reset_at": quota["reset_at"],
                            }
                        },
                    }
                ),
            }

        poller = meter.CodexQuotaPoller(
            self.sidecar.repo,
            resolver,
            meter.urlsplit("http://127.0.0.1:8317"),
            key_loader=lambda: "fixture-management",
        )
        with mock.patch.object(
            poller,
            "_management_request",
            side_effect=management_request,
        ), mock.patch.object(meter, "utc_now", return_value="2026-08-15T00:00:00Z"):
            self.assertEqual(poller.poll_once("fixture-management"), (2, 2))

        self.assertEqual(
            [call["auth_index"] for call in api_calls],
            [
                auth_index_by_file[str(principal_b["file"])],
                auth_index_by_file[str(principal_a["file"])],
            ],
        )
        self.assertEqual(
            [call["header"]["Chatgpt-Account-Id"] for call in api_calls],
            [shared_account, shared_account],
        )

        cards = {
            row["identity_key"]: row
            for row in self.sidecar.repo.subscription_dashboard_rows()
        }
        self.assertEqual(set(cards), {event_a.identity_key, event_b.identity_key})
        card_a = cards[event_a.identity_key]
        card_b = cards[event_b.identity_key]
        # Quota rows keep only the canonical identity key.  Human-readable
        # aliases are recovered from the local resolver at render time.
        self.assertIsNone(card_a["usage_alias"])
        self.assertIsNone(card_b["usage_alias"])
        self.assertIsNone(card_a["account_id_hash"])
        self.assertIsNone(card_b["account_id_hash"])
        self.assertEqual(
            resolver.resolve_identity_key(card_a["identity_key"]).usage_alias,
            "codex-2",
        )
        self.assertIsNone(
            resolver.resolve_identity_key(card_b["identity_key"]).usage_alias
        )
        self.assertEqual(card_a["windows"]["weekly"]["used_percent"], 16)
        self.assertEqual(card_a["windows"]["weekly"]["remaining_percent"], 84)
        self.assertEqual(
            card_a["windows"]["weekly"]["reset_at"],
            "2026-08-21T14:32:00Z",
        )
        self.assertEqual(card_b["windows"]["weekly"]["used_percent"], 30)
        self.assertEqual(card_b["windows"]["weekly"]["remaining_percent"], 70)
        self.assertEqual(
            card_b["windows"]["weekly"]["reset_at"],
            "2026-08-21T14:30:00Z",
        )
        self.assertEqual(
            card_a["subscription_active_until"],
            active_until[str(principal_a["file"])],
        )
        self.assertEqual(
            card_b["subscription_active_until"],
            active_until[str(principal_b["file"])],
        )

        sqlite_dump = "\n".join(self._sqlite_dump())
        for principal in (principal_a, principal_b):
            self.assertNotIn(str(principal["email"]), sqlite_dump)
            self.assertNotIn(str(principal["token"]), sqlite_dump)
            self.assertNotIn(str(principal["user_id"]), sqlite_dump)
            self.assertNotIn(str(principal["sub"]), sqlite_dump)
            self.assertNotIn(str(principal["file"]), sqlite_dump)
        for auth_index in auth_index_by_file.values():
            self.assertNotIn(auth_index, sqlite_dump)

    def test_shared_workspace_reconciles_provisional_queue_rows_to_distinct_identities(self) -> None:
        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        principals = fixture["principals"]
        assert isinstance(resolver, meter.AccountResolver)
        assert isinstance(principals, dict)
        principal_a = principals["a"]
        principal_b = principals["b"]
        expected_a, _ = self.queue_identity(resolver, principal_a, "fixture-history-a")
        expected_b, _ = self.queue_identity(resolver, principal_b, "fixture-history-b")
        fingerprints = {
            "a": meter.short_hash(str(principal_a["token"])),
            "b": meter.short_hash(str(principal_b["token"])),
        }
        self.assertTrue(all(fingerprints.values()))
        resolved_keys = {
            name: meter.resolved_identity_key(
                resolver.resolve(None, fingerprint)
            )
            for name, fingerprint in fingerprints.items()
        }
        self.assertEqual(resolved_keys["a"], expected_a.identity_key)
        self.assertEqual(resolved_keys["b"], expected_b.identity_key)
        self.assertNotEqual(resolved_keys["a"], resolved_keys["b"])

        with sqlite3.connect(self.db) as conn:
            for offset, name in enumerate(("a", "b")):
                fingerprint = fingerprints[name]
                conn.execute(
                    """INSERT INTO usage_events
                       (ts, identity_key, usage_alias, auth_fingerprint, model,
                        status_code, ok, call_count, usage_missing)
                       VALUES (?, ?, ?, ?, 'fixture-model', 200, 1, 1, 0)""",
                    (
                        f"2026-08-15T00:00:0{offset}.000000Z",
                        f"alias:auth:{fingerprint}",
                        f"auth:{fingerprint}",
                        fingerprint,
                    ),
                )

        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(resolver), 2)
        rows = {
            row["identity_key"]: row
            for row in self.rows(
                """SELECT identity_key, usage_alias, auth_fingerprint,
                          account_id_hash, account_id_tail
                     FROM usage_events"""
            )
        }
        self.assertEqual(set(rows), set(resolved_keys.values()))
        row_a = rows[resolved_keys["a"]]
        row_b = rows[resolved_keys["b"]]
        self.assertEqual(row_a["identity_key"], expected_a.identity_key)
        self.assertEqual(row_b["identity_key"], expected_b.identity_key)
        self.assertNotEqual(row_a["identity_key"], row_b["identity_key"])
        for row in (row_a, row_b):
            self.assertIsNone(row["usage_alias"])
            self.assertIsNone(row["auth_fingerprint"])
            self.assertIsNone(row["account_id_hash"])
            self.assertIsNone(row["account_id_tail"])
        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(resolver), 0)

    def test_rotated_legacy_queue_fingerprint_does_not_guess_shared_workspace_principal(self) -> None:
        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        shared_account = str(fixture["shared_account"])
        assert isinstance(resolver, meter.AccountResolver)
        rotated_fingerprint = "deadbeefcafefeed"
        legacy_hash = meter.short_hash(shared_account)
        legacy_key = f"account:{legacy_hash}"
        self.assertIsNone(
            resolver.resolve(None, rotated_fingerprint).subscription_id_hash
        )
        self.assertIsNotNone(
            resolver.resolve("codex-2", None).subscription_id_hash
        )

        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, usage_alias, auth_fingerprint,
                    account_id_hash, account_id_tail, model, status_code, ok,
                    call_count, usage_missing, source)
                   VALUES ('2026-08-14T00:00:00.000000Z', ?, 'codex-2', ?, ?, ?,
                           'fixture-model', 200, 1, 1, 0, 'usage_queue')""",
                (
                    legacy_key,
                    rotated_fingerprint,
                    legacy_hash,
                    shared_account[-8:],
                ),
            )

        self.assertEqual(self.sidecar.repo.reconcile_auth_identities(resolver), 0)
        row = self.rows(
            """SELECT identity_key, usage_alias, auth_fingerprint, account_id_hash
                 FROM usage_events"""
        )[0]
        self.assertEqual(row["identity_key"], legacy_key)
        self.assertIsNone(row["usage_alias"])
        self.assertIsNone(row["auth_fingerprint"])
        self.assertIsNone(row["account_id_hash"])

        target_identity = resolver.resolve("codex-2", None)
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-15T00:05:00Z",
                "identity_key": meter.resolved_identity_key(target_identity),
                "account_id_hash": legacy_hash,
                "account_id_tail": shared_account[-8:],
                "usage_alias": "codex-2",
                "plan_type": "team",
                "window_kind": "weekly",
                "used_percent": 16,
                "remaining_percent": 84,
                "source": "fixture",
            }
        )
        legacy = next(
            item
            for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == legacy_key
        )
        # The rotated token is not evidence for selecting either principal.
        # Reconciliation keeps the opaque legacy key but removes every
        # persisted clue that could relink it to the shared workspace.
        self.assertFalse(legacy["legacy_ambiguous"])
        self.assertEqual(meter.display_identity(legacy), "unknown")
        self.assertEqual(meter.identity_badge(legacy)[0], "A")
        serialized = "\n".join(self._sqlite_dump())
        self.assertNotIn(rotated_fingerprint, serialized)
        self.assertNotIn(shared_account[-8:], serialized)

    def test_duplicate_auth_files_for_one_principal_poll_quota_once(self) -> None:
        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        principals = fixture["principals"]
        shared_account = str(fixture["shared_account"])
        assert isinstance(resolver, meter.AccountResolver)
        assert isinstance(principals, dict)
        principal = principals["a"]
        duplicate = {
            **principal,
            "file": "member-alpha@example.cpa.2026-08-15_00-01-02.json",
            "token": "fixture-access-alpha-rotated",
        }
        proxy_home = Path(fixture["home"]) / ".cli-proxy-api"
        (proxy_home / str(duplicate["file"])).write_text(
            json.dumps(
                {
                    "type": "codex",
                    "account_id": shared_account,
                    "access_token": duplicate["token"],
                    "id_token": duplicate["id_token"],
                    "email": duplicate["email"],
                    "plan_type": "team",
                }
            ),
            encoding="utf-8",
        )

        original_identity = resolver.resolve_auth_file(str(principal["file"]))
        duplicate_identity = resolver.resolve_auth_file(str(duplicate["file"]))
        self.assertEqual(
            duplicate_identity.subscription_id_hash,
            original_identity.subscription_id_hash,
        )
        self.assertEqual(
            meter.resolved_identity_key(duplicate_identity),
            meter.resolved_identity_key(original_identity),
        )
        self.assertEqual(duplicate_identity.usage_alias, "codex-2")
        rebuilt = meter.AccountResolver(home=fixture["home"], refresh_seconds=0)
        self.assertEqual(
            rebuilt.resolve_auth_file(str(duplicate["file"])).subscription_id_hash,
            original_identity.subscription_id_hash,
        )

        auth_files = [
            {
                "provider": "codex",
                "auth_index": auth_index,
                "name": auth["file"],
                "account": auth["email"],
                "id_token": {"chatgpt_account_id": shared_account, "plan_type": "team"},
            }
            for auth_index, auth in (
                ("opaque-duplicate", duplicate),
                ("opaque-original", principal),
            )
        ]
        poller = meter.CodexQuotaPoller(
            self.sidecar.repo,
            resolver,
            meter.urlsplit("http://127.0.0.1:8317"),
            key_loader=lambda: "fixture-management",
        )
        with mock.patch.object(
            poller,
            "_management_request",
            side_effect=[
                (200, {"files": auth_files}),
                (
                    200,
                    {
                        "status_code": 200,
                        "body": json.dumps(
                            {
                                "plan_type": "team",
                                "rate_limit": {
                                    "secondary_window": {
                                        "used_percent": 16,
                                        "limit_window_seconds": 604800,
                                        "reset_at": "2026-08-21T14:32:00Z",
                                    }
                                },
                            }
                        ),
                    },
                ),
            ],
        ) as management_mock, mock.patch.object(
            meter, "utc_now", return_value="2026-08-15T00:00:00Z"
        ):
            self.assertEqual(poller.poll_once("fixture-management"), (1, 1))

        self.assertEqual(management_mock.call_count, 2)
        api_call = management_mock.call_args_list[1].args[3]
        self.assertEqual(api_call["auth_index"], "opaque-duplicate")
        quotas = self.sidecar.repo.latest_subscription_quotas()
        self.assertEqual(len(quotas), 1)
        self.assertEqual(
            quotas[0]["identity_key"],
            meter.resolved_identity_key(original_identity),
        )
        self.assertEqual(quotas[0]["remaining_percent"], 84)

    def test_stale_401_rotation_falls_through_to_same_email_file(self) -> None:
        """A stale RT must not hide a newer file for the same mailbox."""

        fixture = self.shared_workspace_fixture()
        resolver = fixture["resolver"]
        principals = fixture["principals"]
        shared_account = str(fixture["shared_account"])
        assert isinstance(resolver, meter.AccountResolver)
        assert isinstance(principals, dict)
        principal = principals["a"]
        rotated = {
            **principal,
            "file": "member-alpha@example.cpa.2026-08-15_00-02-03.json",
            "token": "fixture-access-alpha-current",
        }
        proxy_home = Path(fixture["home"]) / ".cli-proxy-api"
        (proxy_home / str(rotated["file"])).write_text(
            json.dumps(
                {
                    "type": "codex",
                    "account_id": shared_account,
                    "access_token": rotated["token"],
                    "id_token": rotated["id_token"],
                    "email": rotated["email"],
                    "plan_type": "team",
                }
            ),
            encoding="utf-8",
        )

        identity = resolver.resolve_auth_file(str(principal["file"]))
        rotated_identity = resolver.resolve_auth_file(str(rotated["file"]))
        self.assertEqual(
            meter.resolved_identity_key(identity),
            meter.resolved_identity_key(rotated_identity),
        )
        auth_files = [
            {
                "provider": "codex",
                "auth_index": auth_index,
                "name": auth["file"],
                "account": auth["email"],
                "id_token": {"chatgpt_account_id": shared_account, "plan_type": "team"},
            }
            for auth_index, auth in (
                ("opaque-stale", principal),
                ("opaque-current", rotated),
            )
        ]
        calls: list[str] = []

        def management(
            _key: str,
            method: str,
            _path: str,
            payload: dict[str, object] | None = None,
        ) -> tuple[int, object]:
            if method == "GET":
                return 200, {"files": auth_files}
            assert payload is not None
            auth_index = str(payload["auth_index"])
            calls.append(auth_index)
            if auth_index == "opaque-stale":
                return 200, {"status_code": 401, "body": "{}"}
            return 200, {
                "status_code": 200,
                "body": json.dumps(
                    {
                        "plan_type": "team",
                        "rate_limit": {
                            "secondary_window": {
                                "used_percent": 22,
                                "limit_window_seconds": 604800,
                                "reset_at": "2026-08-22T14:32:00Z",
                            }
                        },
                    }
                ),
            }

        poller = meter.CodexQuotaPoller(
            self.sidecar.repo,
            resolver,
            meter.urlsplit("http://127.0.0.1:8317"),
            key_loader=lambda: "fixture-management",
        )
        with mock.patch.object(poller, "_management_request", side_effect=management), mock.patch.object(
            meter, "utc_now", return_value="2026-08-15T00:00:00Z"
        ):
            self.assertEqual(poller.poll_once("fixture-management"), (1, 1))

        self.assertEqual(calls, ["opaque-stale", "opaque-current"])
        self.assertEqual(len(self.sidecar.repo.subscription_dashboard_rows()), 1)
        card = self.sidecar.repo.subscription_dashboard_rows()[0]
        self.assertEqual(card["windows"]["weekly"]["remaining_percent"], 78)

    def test_chrome_management_session_decoder_keeps_key_in_memory(self) -> None:
        host = "localhost:8317"
        user_agent = "fixture-user-agent"
        expected = {"state": {"managementKey": "fixture-management", "apiBase": "http://localhost:8317"}}
        plain = json.dumps(expected, separators=(",", ":")).encode()
        mask = f"{chrome_key.STORAGE_PREFIX}|{host}|{user_agent}".encode()
        cipher = bytes(byte ^ mask[index % len(mask)] for index, byte in enumerate(plain))
        decoded = list(chrome_key._decoded_objects(cipher, [host], [user_agent]))
        self.assertEqual(decoded, [expected])

    def test_usage_queue_poller_records_direct_8317_event_without_api_key(self) -> None:
        full_digest = hashlib.sha256(AUTH_SECRET.encode()).hexdigest()
        FakeUpstreamHandler.queue_payload = [
            {
                "timestamp": "2026-08-12T00:00:00Z",
                "latency_ms": 321,
                "auth_index": "codex-eyrie-monody-9p@icloud.com-plus.json",
                "access_token_sha256": full_digest,
                "api_key": "queue-api-secret-must-not-be-stored",
                "model": "fake-responses",
                "alias": "fake-responses",
                "endpoint": "/v1/responses",
                "tokens": {
                    "input_tokens": 12,
                    "output_tokens": 8,
                    "cached_tokens": 2,
                    "reasoning_tokens": 3,
                    "total_tokens": 20,
                },
                "failed": False,
                "fail": {"status_code": 200, "body": ""},
                "request_id": "queue-request-1",
            }
        ]
        key_file = self.temp_path / "management.key"
        key_file.write_text("management-fixture\n", encoding="utf-8")
        key_file.chmod(0o600)
        poller = meter.UsageQueuePoller(
            self.sidecar.repo,
            self.sidecar.resolver,
            meter.urlsplit(f"http://127.0.0.1:{self.fake.server_address[1]}"),
            key_file=str(key_file),
            poll_seconds=0.5,
        )
        poller.start()
        try:
            self.wait_events(1)
        finally:
            poller.stop()
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertEqual(row["source"], "usage_queue")
        self.assertIsNone(row["request_id"])
        self.assertIsNone(row["usage_alias"])
        self.assertTrue(str(row["identity_key"]).startswith("subscription:"))
        self.assertEqual((row["input_tokens"], row["output_tokens"], row["total_tokens"]), (12, 8, 20))
        self.assertIsNone(row["session_id"])
        self.assertEqual(row["duration_ms"], 321)
        self.assertTrue(FakeUpstreamHandler.management_auth_forwarded)
        self.assertNotIn("queue-api-secret-must-not-be-stored", "\n".join(self._sqlite_dump()))

    def test_usage_queue_passes_raw_429_to_quota_guard_only_in_memory(self) -> None:
        observed: list[tuple[object, object, str]] = []

        class FakeGuard:
            def observe_record(self, record, event, key):
                observed.append((record, event, key))
                return True

            def reconcile(self, key):
                return 0

            def status(self):
                return {"enabled": True, "active_locks": 0}

        record = {
            "timestamp": "2026-08-12T00:00:00Z",
            "latency_ms": 10,
            "auth_index": "fixture-auth-index",
            "model": "fake-responses",
            "alias": "fake-responses",
            "tokens": {},
            "failed": True,
            "fail": {
                "status_code": 429,
                "body": '{"error":{"type":"usage_limit_reached","fixture_secret":"must-not-persist"}}',
            },
        }
        poller = meter.UsageQueuePoller(
            self.sidecar.repo,
            self.sidecar.resolver,
            meter.urlsplit("http://127.0.0.1:8317"),
            quota_guard=FakeGuard(),
        )
        poller._consume(json.dumps([record]).encode(), "fixture-management-key")
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0][2], "fixture-management-key")
        self.assertEqual(observed[0][0]["auth_index"], "fixture-auth-index")
        self.assertNotIn("must-not-persist", "\n".join(self._sqlite_dump()))

    def test_usage_queue_403_uses_long_backoff_without_retry_storm(self) -> None:
        FakeUpstreamHandler.management_status = 403
        key_file = self.temp_path / "management-backoff.key"
        key_file.write_text("management-fixture\n", encoding="utf-8")
        key_file.chmod(0o600)
        poller = meter.UsageQueuePoller(
            self.sidecar.repo,
            self.sidecar.resolver,
            meter.urlsplit(f"http://127.0.0.1:{self.fake.server_address[1]}"),
            key_file=str(key_file),
            poll_seconds=0.5,
        )
        poller.start()
        try:
            deadline = time.monotonic() + 2
            while FakeUpstreamHandler.management_calls < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(FakeUpstreamHandler.management_calls, 1)
            time.sleep(0.65)
            self.assertEqual(FakeUpstreamHandler.management_calls, 1)
            status = poller.status()
            self.assertEqual(status["last_status"], 403)
            self.assertEqual(status["backoff_seconds"], meter.MAX_MANAGEMENT_BACKOFF_SECONDS)
        finally:
            poller.stop()

    def _sqlite_dump(self) -> list[str]:
        with sqlite3.connect(self.db) as conn:
            return list(conn.iterdump())

    def test_missing_usage_and_other_v1_path_are_recorded(self) -> None:
        status, _, _ = self.request("POST", "/v1/no-usage", {"model": "fake-missing"})
        self.assertEqual(status, 200)
        status, _, body = self.request("GET", "/v1/models?source=fake", None)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["requested_path"], "/v1/models?source=fake")
        self.wait_events(2)
        rows = self.rows("SELECT endpoint, usage_missing, call_count FROM usage_events ORDER BY id")
        self.assertEqual(
            [(row["endpoint"], row["usage_missing"], row["call_count"]) for row in rows],
            [(None, 1, 1), (None, 1, 1)],
        )

    def test_malformed_v1_request_is_still_counted(self) -> None:
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as client:
            client.sendall(
                b"POST /v1/responses HTTP/1.1\r\n"
                b"Host: 127.0.0.1\r\n"
                + f"Authorization: Bearer {AUTH_SECRET}\r\n".encode()
                + b"Content-Length: invalid\r\nConnection: close\r\n\r\n"
            )
            response = client.recv(4096)
        self.assertIn(b"400", response.split(b"\r\n", 1)[0])
        self.wait_events(1)
        row = self.rows("SELECT status_code, ok, usage_missing, call_count FROM usage_events")[0]
        self.assertEqual(tuple(row), (400, 0, 1, 1))

    def test_failure_is_counted_redacted_and_closes_quota_cycle(self) -> None:
        status, _, _ = self.request("POST", "/v1/fail", {"model": "fake-error"})
        self.assertEqual(status, 429)
        self.wait_events(1)
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertEqual((row["ok"], row["call_count"], row["usage_missing"]), (0, 1, 1))
        self.assertIsNone(row["error_type"])
        self.assertIsNone(row["error_message_redacted"])
        deadline = time.monotonic() + 2
        quota: list[sqlite3.Row] = []
        cycles: list[sqlite3.Row] = []
        while time.monotonic() < deadline:
            quota = self.rows("SELECT event_type, raw_message_redacted FROM quota_events")
            cycles = self.rows("SELECT is_complete_cycle FROM account_quota_cycles")
            if quota and cycles:
                break
            time.sleep(0.01)
        self.assertTrue(quota, "quota event was not persisted")
        self.assertTrue(cycles, "quota cycle was not persisted")
        self.assertEqual(quota[0]["event_type"], "usage_limit_hit")
        self.assertEqual(cycles[0]["is_complete_cycle"], 1)
        card = next(
            item
            for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == row["identity_key"]
        )
        self.assertEqual(card["execution_availability"], "confirmed_exhausted")

        with sqlite3.connect(self.db) as conn:
            dump = "\n".join(conn.iterdump())
        if AUTH_SECRET in dump or ERROR_SECRET in dump:
            self.fail("a raw credential leaked into SQLite")

    def test_arbitrary_identity_text_is_not_persisted_as_a_model(self) -> None:
        sensitive_model = "member-fixture@example.test"
        historical_key = subscription_key("fixture-historical-text")
        status, _, _ = self.request(
            "POST", "/v1/responses", {"model": sensitive_model}
        )
        self.assertEqual(status, 200)
        self.wait_events(1)
        row = self.rows("SELECT model, total_tokens FROM usage_events")[0]
        self.assertIsNone(row["model"])
        self.assertEqual(row["total_tokens"], 150)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events (
                     ts, identity_key, model, status_code, ok, call_count,
                     usage_missing, error_type, source
                   ) VALUES (
                     '2026-08-13T00:00:00.000000Z', ?, ?, 500, 0, 1,
                     1, ?, 'fixture'
                   )""",
                (historical_key, sensitive_model, sensitive_model),
            )
        self.sidecar.repo.apply_privacy_minimization(self.sidecar.resolver)
        historical = self.rows(
            "SELECT model, error_type FROM usage_events "
            "WHERE identity_key=?",
            (historical_key,),
        )[0]
        self.assertIsNone(historical["model"])
        self.assertIsNone(historical["error_type"])
        self.assertNotIn(sensitive_model, "\n".join(self._sqlite_dump()))

    def test_success_after_quota_records_automatic_reset(self) -> None:
        self.request("POST", "/v1/fail", {"model": "fake-error"})
        self.wait_events(1)
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(2)
        events = [row["event_type"] for row in self.rows("SELECT event_type FROM quota_events ORDER BY id")]
        self.assertEqual(events, ["usage_limit_hit", "reset_detected"])
        summary = self.sidecar.repo.quota_summary("30d")[0]
        self.assertEqual(summary["currently_quota_hit"], 0)

    def test_success_after_quota_does_not_reset_while_provider_window_is_full(self) -> None:
        self.request("POST", "/v1/fail", {"model": "fake-error"})
        self.wait_events(1)
        identity = self.rows("SELECT identity_key, account_id_hash, account_id_tail FROM usage_events LIMIT 1")[0]
        snapshot_at = datetime.now(timezone.utc)
        reset_at = snapshot_at + timedelta(days=1)
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": snapshot_at,
                "identity_key": identity["identity_key"],
                "account_id_hash": identity["account_id_hash"],
                "account_id_tail": identity["account_id_tail"],
                "usage_alias": "codex-1",
                "window_kind": "weekly",
                "used_percent": 100,
                "remaining_percent": 0,
                "window_seconds": 604800,
                "reset_at": reset_at,
                "source": "fixture",
            }
        )
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(2)
        events = [row["event_type"] for row in self.rows("SELECT event_type FROM quota_events ORDER BY id")]
        self.assertEqual(events, ["usage_limit_hit"])
        card = next(
            item
            for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == identity["identity_key"]
        )
        self.assertEqual(card["execution_availability"], "recent_success")
        self.assertIn("上游 0% · 实测可用", meter.dashboard_html(self.sidecar.repo))

    def test_zero_quota_snapshot_after_success_is_not_called_live_available(self) -> None:
        identity = self.sidecar.resolver.resolve("codex-1", None)
        identity_key = meter.resolved_identity_key(identity)
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-17T20:19:11Z",
                "identity_key": identity_key,
                "window_kind": "weekly",
                "window_seconds": 604800,
                "used_percent": 100,
                "remaining_percent": 0,
                "source": "fixture",
            }
        )
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events (
                     ts, identity_key, model, status_code, ok, call_count,
                     account_attempt, usage_missing
                   ) VALUES (
                     '2026-08-17T20:19:09Z', ?, 'fixture-model', 200, 1, 1,
                     1, 0
                   )""",
                (identity_key,),
            )
        card = next(
            row
            for row in self.sidecar.repo.subscription_dashboard_rows()
            if row["identity_key"] == identity_key
        )
        self.assertEqual(card["execution_availability"], "success_before_zero_snapshot")
        page = meter.dashboard_html(self.sidecar.repo)
        self.assertIn("上游报告 0%", page)
        self.assertNotIn("上游 0% · 实测可用", page)

    def test_sse_is_byte_transparent_and_usage_is_recorded(self) -> None:
        expected = (
            b'data: {"type":"response.created","response":{"model":"fake-stream"}}\n\n'
            b'data: {"type":"response.completed","response":{"model":"fake-stream","usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}}}}\n\n'
            b"data: [DONE]\n\n"
        )
        status, headers, body = self.request("POST", "/v1/stream", {"model": "fake-stream", "stream": True})
        self.assertEqual(status, 200)
        self.assertEqual(body, expected)
        self.assertIn("text/event-stream", dict((key.lower(), value) for key, value in headers)["content-type"])
        self.wait_events(1)
        row = self.rows("SELECT * FROM usage_events")[0]
        self.assertEqual((row["stream"], row["usage_missing"], row["total_tokens"]), (1, 0, 18))
        self.assertEqual((row["cached_tokens"], row["reasoning_tokens"]), (3, 2))

    def test_sse_without_usage_is_marked_missing(self) -> None:
        status, _, body = self.request("POST", "/v1/stream-no-usage", {"model": "fake-stream", "stream": True})
        self.assertEqual(status, 200)
        self.assertTrue(body.endswith(b"data: [DONE]\n\n"))
        self.wait_events(1)
        row = self.rows("SELECT stream, usage_missing FROM usage_events")[0]
        self.assertEqual(tuple(row), (1, 1))

    def test_sse_first_event_is_forwarded_before_slow_stream_finishes(self) -> None:
        body = json.dumps({"model": "fake-stream", "stream": True}).encode()
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        started = time.monotonic()
        connection.request(
            "POST",
            "/v1/stream-slow",
            body=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {AUTH_SECRET}"},
        )
        response = connection.getresponse()
        first_line = response.readline()
        first_elapsed = time.monotonic() - started
        rest = response.read()
        connection.close()
        self.assertEqual(first_line, b'data: {"type":"response.output_text.delta","delta":"first"}\n')
        self.assertLess(first_elapsed, 0.25, "the sidecar buffered the streaming response")
        self.assertIn(b"response.completed", rest)
        self.wait_events(1)
        row = self.rows("SELECT stream, total_tokens FROM usage_events")[0]
        self.assertEqual(tuple(row), (1, 3))

    def test_response_timeline_is_dense_exact_and_all_api_only(self) -> None:
        now = datetime(2026, 8, 17, 12, 32, 59, tzinfo=timezone.utc)
        with self.sidecar.repo.connect() as conn:
            conn.executemany(
                """INSERT INTO api_response_observations
                   (observation_key, minute_ts, status_code, call_count, source)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    ("fixture-1", "2026-08-17T12:30:00Z", 200, 1, "sidecar"),
                    ("fixture-2", "2026-08-17T12:31:00Z", 200, 3, "usage_queue"),
                    ("fixture-3", "2026-08-17T12:32:00Z", 201, 2, "sidecar"),
                    ("fixture-4", "2026-08-17T12:32:00Z", 502, 4, "cockpit_tools"),
                    ("fixture-5", "2026-08-17T12:32:00Z", 200, 5, "manual_codex_app"),
                    ("fixture-6", "2026-08-17T12:29:00Z", 500, 7, "sidecar"),
                ),
            )

        timeline = self.sidecar.repo.response_timeline(3, now=now)
        self.assertEqual(
            [point["ts"] for point in timeline["points"]],
            [
                "2026-08-17T12:30:00Z",
                "2026-08-17T12:31:00Z",
                "2026-08-17T12:32:00Z",
            ],
        )
        self.assertEqual(
            [
                (point["status_200"], point["status_non_200"])
                for point in timeline["points"]
            ],
            [(1, 0), (3, 0), (0, 6)],
        )
        self.assertEqual(
            timeline["totals"],
            {
                "status_200": 4,
                "status_non_200": 6,
                "total": 10,
            },
        )
        self.assertEqual(
            timeline["sources"],
            ["sidecar", "usage_queue", "cockpit_tools", "sub2api"],
        )
        self.assertEqual(timeline["source"], "api")
        with self.assertRaises(ValueError):
            self.sidecar.repo.response_timeline(0, now=now)

    def test_dashboard_and_all_required_cli_queries(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.request("POST", "/v1/chat/completions", {"model": "fake-chat", "messages": []})
        self.wait_events(2)
        status, headers, page = self.request("GET", "/usage", None, alias=None)
        self.assertEqual(status, 200)
        self.assertEqual(dict((key.lower(), value) for key, value in headers)["cache-control"], "no-store")
        self.assertIn(b"Codex Usage Observatory", page)
        self.assertIn("Token 消费总览".encode(), page)
        self.assertIn("近 7 天趋势".encode(), page)
        self.assertIn("订阅额度雷达".encode(), page)
        self.assertIn("非缓存输入 Tokens".encode(), page)
        self.assertIn("输出 Tokens".encode(), page)
        self.assertIn("缓存命中 Tokens".encode(), page)
        self.assertIn("输入成本".encode(), page)
        self.assertIn("输出成本".encode(), page)
        self.assertIn("缓存成本".encode(), page)
        self.assertIn("API 原始处理量".encode(), page)
        self.assertIn("账号尝试".encode(), page)
        self.assertIn("推理（输出子集）".encode(), page)
        self.assertIn("API 响应时间轴".encode(), page)
        self.assertIn("HTTP 200 与非 200 双折线".encode(), page)
        self.assertIn(b'data-role="timeline-line-200"', page)
        self.assertIn(b'data-role="timeline-line-non-200"', page)
        self.assertIn(b'/usage/timeline?minutes=1440', page)
        self.assertNotIn(b'data-role="timeline-source"', page)
        self.assertIn(b'--paper:#fff4dd', page)
        self.assertIn(b'box-shadow:var(--shadow)', page)
        self.assertIn(b'data-role="theme-toggle"', page)
        self.assertIn(b"cliproxy-usage-theme", page)
        self.assertIn(b"fixture@example.com", page)
        self.assertNotIn(b"codex-1", page)
        timeline_status, timeline_headers, timeline_body = self.request(
            "GET", "/usage/timeline?minutes=60", None, alias=None
        )
        self.assertEqual(timeline_status, 200)
        normalized_timeline_headers = dict(
            (key.lower(), value) for key, value in timeline_headers
        )
        self.assertEqual(normalized_timeline_headers["cache-control"], "no-store")
        self.assertIn("application/json", normalized_timeline_headers["content-type"])
        timeline = json.loads(timeline_body)
        self.assertEqual(timeline["interval"], "1m")
        self.assertEqual(timeline["window_minutes"], 60)
        self.assertEqual(len(timeline["points"]), 60)
        self.assertGreaterEqual(timeline["totals"]["status_200"], 2)
        self.assertEqual(timeline["totals"]["status_non_200"], 0)
        invalid_status, _, invalid_body = self.request(
            "GET", "/usage/timeline?minutes=0", None, alias=None
        )
        self.assertEqual(invalid_status, 400)
        self.assertEqual(json.loads(invalid_body)["error"], "invalid timeline query")
        scoped_status, _, scoped_body = self.request(
            "GET", "/usage/timeline?minutes=60&source=cockpit_tools", None, alias=None
        )
        self.assertEqual(scoped_status, 400)
        self.assertEqual(json.loads(scoped_body)["error"], "invalid timeline query")
        health_status, health_headers, health_body = self.request(
            "GET", "/healthz", None, alias=None
        )
        self.assertEqual(health_status, 200)
        self.assertEqual(
            dict((key.lower(), value) for key, value in health_headers)["cache-control"],
            "no-store",
        )
        self.assertIn("cockpit_tools", json.loads(health_body))

        commands = [
            ("--summary", "today"),
            ("--summary", "all"),
            ("--recent", "20"),
            ("--by-account", "7d"),
            ("--by-model", "7d"),
            ("--by-session", "7d"),
            ("--by-date", "7d"),
            ("--quota-summary", "30d"),
            ("--quota-summary-by-account",),
            ("--price-sync-status",),
        ]
        for action in commands:
            completed = subprocess.run(
                [sys.executable, str(SCRIPT), "--db", str(self.db), "--json", *action],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, msg=f"CLI failed for {action}: {completed.stderr}")
            parsed = json.loads(completed.stdout)
            self.assertTrue(parsed)

    def test_codex_app_local_import_is_safe_idempotent_and_priced(self) -> None:
        app_home = self.temp_path / "app-codex"
        alias_home = self.temp_path / ".codex-c"
        sessions = app_home / "sessions" / "2026" / "08" / "13"
        sessions.mkdir(parents=True)
        alias_home.mkdir(parents=True)
        account_id = "acct-local-fa79c563"
        auth = {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account_id,
                "id_token": fixture_jwt({"email": "local-import@example.test"}),
            },
        }
        (app_home / "auth.json").write_text(json.dumps(auth), encoding="utf-8")
        (alias_home / "auth.json").write_text(json.dumps(auth), encoding="utf-8")
        (self.temp_path / ".zshrc").write_text(
            'alias codex-13=\'__codex_switch "$HOME/.codex-c" codex-13\'\n',
            encoding="utf-8",
        )
        session = sessions / "rollout-test.jsonl"
        records = [
            {
                "timestamp": "2026-08-13T07:00:00Z",
                "type": "session_meta",
                "payload": {
                    "id": "session-local",
                    "session_id": "session-local",
                    "originator": "Codex Desktop",
                    "model_provider": "openai",
                },
            },
            {
                "timestamp": "2026-08-13T07:00:01Z",
                "type": "turn_context",
                "payload": {"model": "fake-responses", "turn_id": "turn-local"},
            },
            {
                "timestamp": "2026-08-13T07:00:02Z",
                "type": "response_item",
                "payload": {"type": "message", "content": "must never be imported"},
            },
            {
                "timestamp": "2026-08-13T07:00:03Z",
                "ordinal": 4,
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "last_token_usage": {
                            "input_tokens": 100,
                            "cached_input_tokens": 20,
                            "cache_write_input_tokens": 0,
                            "output_tokens": 50,
                            "reasoning_output_tokens": 10,
                            "total_tokens": 150,
                        }
                    },
                    "rate_limits": {
                        "plan_type": "pro",
                        "primary": {
                            "used_percent": 12.5,
                            "window_minutes": 300,
                            "resets_at": 1786608000,
                        },
                    },
                },
            },
        ]
        session.write_text(
            "\n".join(json.dumps(item) for item in records) + "\n", encoding="utf-8"
        )
        resolver = meter.AccountResolver(home=self.temp_path, refresh_seconds=0)
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo, resolver, app_home, "codex-13"
        )
        first = importer.import_once()
        second = importer.import_once()
        self.assertEqual(first["imported"], 1)
        self.assertEqual(second["imported"], 0)
        self.assertEqual(second["quota_rows"], 0)
        appended = {
            **records[-1],
            "timestamp": "2026-08-13T07:00:04Z",
            "ordinal": 5,
        }
        with session.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(appended) + "\n")
        third = importer.import_once()
        self.assertEqual(third["imported"], 1)
        self.assertEqual(third["quota_rows"], 1)
        row = self.rows("SELECT * FROM usage_events WHERE source='codex_app_local'")[0]
        self.assertIsNone(row["usage_alias"])
        self.assertIsNone(row["session_id"])
        self.assertIsNone(row["thread_id"])
        self.assertIsNone(row["turn_id"])
        self.assertIsNone(row["request_id"])
        self.assertIsNone(row["usage_project"])
        self.assertEqual(row["model"], "fake-responses")
        self.assertEqual((row["input_tokens"], row["cached_tokens"], row["output_tokens"]), (100, 20, 50))
        self.assertAlmostEqual(row["estimated_api_cost_usd"], 0.00019, places=10)
        self.assertNotIn("must never be imported", json.dumps(dict(row)))
        quota_rows = self.rows(
            "SELECT * FROM subscription_quota_snapshots WHERE source='codex_app_local'"
        )
        self.assertEqual(len(quota_rows), 2)
        quota = quota_rows[-1]
        self.assertEqual(quota["window_kind"], "five_hour")
        self.assertEqual(quota["used_percent"], 12.5)
        self.assertEqual(quota["plan_type"], "pro")

    def test_sparse_local_auth_imports_tokens_anonymously_without_quota_identity(self) -> None:
        app_home = self.temp_path / "sparse-app-codex"
        alias_home = self.temp_path / ".codex-sparse"
        sessions = app_home / "sessions"
        sessions.mkdir(parents=True)
        alias_home.mkdir(parents=True)
        account_id = "acct-sparse-local-fixture"
        auth = {"auth_mode": "chatgpt", "tokens": {"account_id": account_id}}
        (app_home / "auth.json").write_text(json.dumps(auth), encoding="utf-8")
        (alias_home / "auth.json").write_text(json.dumps(auth), encoding="utf-8")
        (self.temp_path / ".zshrc").write_text(
            'alias codex-13=\'__codex_switch "$HOME/.codex-sparse" codex-13\'\n',
            encoding="utf-8",
        )
        (sessions / "rollout-sparse.jsonl").write_text(
            "\n".join(
                json.dumps(item)
                for item in (
                    {
                        "timestamp": "2026-08-13T08:00:00Z",
                        "type": "session_meta",
                        "payload": {"model_provider": "openai"},
                    },
                    {
                        "timestamp": "2026-08-13T08:00:01Z",
                        "ordinal": 1,
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "last_token_usage": {
                                    "input_tokens": 12,
                                    "cached_input_tokens": 2,
                                    "output_tokens": 3,
                                    "total_tokens": 15,
                                }
                            },
                            "rate_limits": {
                                "plan_type": "team",
                                "primary": {
                                    "used_percent": 25,
                                    "window_minutes": 300,
                                },
                            },
                        },
                    },
                )
            )
            + "\n",
            encoding="utf-8",
        )
        resolver = meter.AccountResolver(home=self.temp_path, refresh_seconds=0)
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo, resolver, app_home, "codex-13"
        )

        result = importer.import_once()

        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["quota_rows"], 0)
        row = self.rows(
            "SELECT identity_key, input_tokens, cached_tokens, output_tokens "
            "FROM usage_events WHERE source='codex_app_local'"
        )[0]
        self.assertEqual(row["identity_key"], "unknown")
        self.assertEqual(
            (row["input_tokens"], row["cached_tokens"], row["output_tokens"]),
            (12, 2, 3),
        )
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS count FROM subscription_quota_snapshots "
                "WHERE source='codex_app_local'"
            )[0]["count"],
            0,
        )

    def test_principal_only_app_auth_matches_richer_alias_member(self) -> None:
        app_home = self.temp_path / "principal-app-codex"
        alias_home = self.temp_path / ".codex-principal-alias"
        sessions = app_home / "sessions"
        sessions.mkdir(parents=True)
        alias_home.mkdir(parents=True)
        account_id = "acct-principal-upgrade-fixture"
        principal_id = "principal-upgrade-fixture"
        app_auth = {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account_id,
                "id_token": fixture_jwt({"sub": principal_id}),
            },
        }
        alias_auth = {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account_id,
                "id_token": fixture_jwt(
                    {
                        "email": "principal-upgrade@example.test",
                        "sub": principal_id,
                    }
                ),
            },
        }
        (app_home / "auth.json").write_text(json.dumps(app_auth), encoding="utf-8")
        (alias_home / "auth.json").write_text(json.dumps(alias_auth), encoding="utf-8")
        (self.temp_path / ".zshrc").write_text(
            'alias codex-13=\'__codex_switch "$HOME/.codex-principal-alias" codex-13\'\n',
            encoding="utf-8",
        )
        (sessions / "rollout-principal.jsonl").write_text(
            "\n".join(
                json.dumps(item)
                for item in (
                    {
                        "timestamp": "2026-08-13T09:00:00Z",
                        "type": "session_meta",
                        "payload": {"model_provider": "openai"},
                    },
                    {
                        "timestamp": "2026-08-13T09:00:01Z",
                        "ordinal": 1,
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "last_token_usage": {
                                    "input_tokens": 9,
                                    "cached_input_tokens": 1,
                                    "output_tokens": 2,
                                    "total_tokens": 11,
                                }
                            },
                            "rate_limits": {
                                "plan_type": "team",
                                "primary": {
                                    "used_percent": 15,
                                    "window_minutes": 300,
                                },
                            },
                        },
                    },
                )
            )
            + "\n",
            encoding="utf-8",
        )
        resolver = meter.AccountResolver(home=self.temp_path, refresh_seconds=0)
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo, resolver, app_home, "codex-13"
        )

        result = importer.import_once()

        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["quota_rows"], 1)
        self.assertTrue(importer.status()["account_match"])
        self.assertTrue(importer.status()["member_match"])
        expected_key = meter.resolved_identity_key(resolver.resolve("codex-13", None))
        imported = self.rows(
            "SELECT identity_key FROM usage_events WHERE source='codex_app_local'"
        )
        self.assertEqual(imported[0]["identity_key"], expected_key)

    def test_local_import_rejects_different_team_member_in_same_workspace(self) -> None:
        app_home = self.temp_path / "team-app-codex"
        alias_home = self.temp_path / ".codex-team-alias"
        (app_home / "sessions").mkdir(parents=True)
        alias_home.mkdir(parents=True)
        account_id = "acct-shared-local-team-fixture"
        app_auth = {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account_id,
                "id_token": fixture_jwt(
                    {
                        "email": "member-app@example.test",
                        "sub": "fixture-app-member",
                    }
                ),
            },
        }
        alias_auth = {
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": account_id,
                "id_token": fixture_jwt(
                    {
                        "email": "member-alias@example.test",
                        "sub": "fixture-alias-member",
                    }
                ),
            },
        }
        (app_home / "auth.json").write_text(json.dumps(app_auth), encoding="utf-8")
        (alias_home / "auth.json").write_text(json.dumps(alias_auth), encoding="utf-8")
        (self.temp_path / ".zshrc").write_text(
            'alias codex-13=\'__codex_switch "$HOME/.codex-team-alias" codex-13\'\n',
            encoding="utf-8",
        )
        resolver = meter.AccountResolver(home=self.temp_path, refresh_seconds=0)
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo, resolver, app_home, "codex-13"
        )

        with self.assertRaisesRegex(ValueError, "member does not match"):
            importer.import_once()

        self.assertTrue(importer.status()["account_match"])
        self.assertFalse(importer.status()["member_match"])
        self.assertEqual(
            self.rows("SELECT COUNT(*) AS count FROM usage_events")[0]["count"],
            0,
        )

    def test_dynamic_local_import_baselines_and_follows_account_switches(self) -> None:
        managed_key = subscription_key("managed-pool-fixture")
        self.sidecar.repo.reconcile_subscription_inventory({managed_key})
        app_home = self.temp_path / "dynamic-codex"
        sessions = app_home / "sessions"
        sessions.mkdir(parents=True)
        auth_a = self.codex_auth(
            "acct-dynamic-alpha-fixture",
            "dynamic-alpha@example.test",
            "dynamic-alpha-principal",
        )
        auth_b = self.codex_auth(
            "acct-dynamic-beta-fixture",
            "dynamic-beta@example.test",
            "dynamic-beta-principal",
        )
        (app_home / "auth.json").write_text(json.dumps(auth_a), encoding="utf-8")
        session = sessions / "rollout-dynamic.jsonl"
        records = [
            {
                "timestamp": "2026-08-16T01:00:00Z",
                "type": "session_meta",
                "payload": {"model_provider": "openai"},
            },
            {
                "timestamp": "2026-08-16T01:00:01Z",
                "type": "turn_context",
                "payload": {"model": "fake-responses"},
            },
            self.codex_token_record("2026-08-16T01:00:02Z", 1, 10),
        ]
        session.write_text(
            "\n".join(json.dumps(item) for item in records) + "\n",
            encoding="utf-8",
        )
        resolver = meter.AccountResolver(home=self.temp_path, refresh_seconds=0)
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo,
            resolver,
            app_home,
            alias=None,
        )

        baseline = importer.import_once()

        self.assertEqual(baseline["imported"], 0)
        self.assertEqual(baseline["baselined_files"], 1)
        self.assertEqual(baseline["matched_homes"], 1)
        with session.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    self.codex_token_record("2026-08-16T01:01:00Z", 2, 20)
                )
                + "\n"
            )
        # A provider can finish a stale inventory snapshot during startup.
        # Keep the unread event until the next complete union admits it.
        local_keys = resolver.active_subscription_keys(force_refresh=True)
        self.assertEqual(len(local_keys), 1)
        with self.sidecar.repo.connect() as conn:
            conn.execute(
                "UPDATE active_subscription_registry SET state='suspect_missing' WHERE identity_key=?",
                (next(iter(local_keys)),),
            )
        deferred = importer.import_once()
        self.assertEqual(deferred["imported"], 0)
        self.assertEqual(deferred["failed_homes"], 1)
        self.assertEqual(self.rows("SELECT * FROM usage_events"), [])
        self.sidecar.repo.reconcile_subscription_inventory(local_keys | {managed_key})
        alpha = importer.import_once()
        self.assertEqual(alpha["imported"], 1)
        self.assertEqual(alpha["baselined_files"], 0)
        alpha_key = self.rows("SELECT identity_key FROM usage_events")[0]["identity_key"]
        self.assertRegex(alpha_key, r"^subscription:[0-9a-f]{32}$")
        active = resolver.active_subscription_keys(force_refresh=True)
        self.assertIn(alpha_key, active)
        self.assertEqual(
            resolver.resolve_identity_key(alpha_key).account_email,
            "dynamic-alpha@example.test",
        )
        # A complete provider refresh must retain an independently signed-in
        # local member, and local discovery must not retire the provider pool.
        self.sidecar.repo.reconcile_subscription_inventory(active | {managed_key})
        self.assertEqual(
            {row["state"] for row in self.rows("SELECT state FROM active_subscription_registry")},
            {"active"},
        )
        self.assertTrue(self.rows("SELECT * FROM subscription_quota_snapshots"))

        (app_home / "auth.json").write_text(json.dumps(auth_b), encoding="utf-8")
        with session.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    self.codex_token_record("2026-08-16T01:02:00Z", 3, 30)
                )
                + "\n"
            )
        switched = importer.import_once()
        self.assertEqual(switched["imported"], 0)
        self.assertEqual(switched["baselined_files"], 1)

        with session.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    self.codex_token_record("2026-08-16T01:03:00Z", 4, 40)
                )
                + "\n"
            )
        beta = importer.import_once()
        self.assertEqual(beta["imported"], 1)
        self.assertEqual(beta["baselined_files"], 0)
        active = resolver.active_subscription_keys(force_refresh=True)
        self.assertNotIn(alpha_key, active)
        self.assertEqual(len(active), 1)
        self.assertEqual(
            resolver.resolve_identity_key(next(iter(active))).account_email,
            "dynamic-beta@example.test",
        )

        new_session = sessions / "rollout-dynamic-new.jsonl"
        new_session.write_text(
            "\n".join(
                json.dumps(item)
                for item in (
                    {
                        "timestamp": "2026-08-16T01:04:00Z",
                        "type": "session_meta",
                        "payload": {"model_provider": "openai"},
                    },
                    {
                        "timestamp": "2026-08-16T01:04:01Z",
                        "type": "turn_context",
                        "payload": {"model": "fake-responses"},
                    },
                    self.codex_token_record("2026-08-16T01:04:02Z", 1, 50),
                )
            )
            + "\n",
            encoding="utf-8",
        )
        new_file_baseline = importer.import_once()
        self.assertEqual(new_file_baseline["imported"], 0)
        self.assertEqual(new_file_baseline["baselined_files"], 1)
        with new_session.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    self.codex_token_record("2026-08-16T01:05:00Z", 2, 60)
                )
                + "\n"
            )
        new_file_delta = importer.import_once()
        self.assertEqual(new_file_delta["imported"], 1)
        self.assertEqual(new_file_delta["baselined_files"], 0)

        rows = self.rows(
            "SELECT identity_key, input_tokens FROM usage_events "
            "WHERE source='codex_app_local' ORDER BY ts"
        )
        self.assertEqual([row["input_tokens"] for row in rows], [20, 40, 60])
        self.assertEqual(len({row["identity_key"] for row in rows}), 2)
        for row in rows:
            self.assertRegex(row["identity_key"], r"^subscription:[0-9a-f]{32}$")
        bindings = self.rows(
            "SELECT home_key, binding_key FROM local_import_bindings"
        )
        self.assertEqual(len(bindings), 1)
        self.assertRegex(bindings[0]["home_key"], r"^home:[0-9a-f]{32}$")
        self.assertRegex(bindings[0]["binding_key"], r"^binding:[0-9a-f]{32}$")
        persisted = self.db.read_bytes()
        wal = Path(f"{self.db}-wal")
        if wal.exists():
            persisted += wal.read_bytes()
        for marker in (
            str(app_home),
            "acct-dynamic-alpha-fixture",
            "acct-dynamic-beta-fixture",
            "dynamic-alpha@example.test",
            "dynamic-beta@example.test",
        ):
            self.assertNotIn(marker.encode(), persisted)

    def test_dynamic_local_import_discovers_cockpit_instances_independently(self) -> None:
        managed_key = subscription_key("managed-instances-fixture")
        self.sidecar.repo.reconcile_subscription_inventory({managed_key})
        cockpit_dir = self.temp_path / ".antigravity_cockpit"
        cockpit_dir.mkdir()
        primary = self.temp_path / ".codex"
        instance = self.temp_path / "cockpit-instances" / "secondary"
        broken = self.temp_path / "cockpit-instances" / "broken"
        outside = self.temp_path.parent / "outside-codex-fixture"
        sessions_by_home: dict[Path, Path] = {}
        for index, home in enumerate((primary, instance), start=1):
            sessions = home / "sessions"
            sessions.mkdir(parents=True)
            (home / "auth.json").write_text(
                json.dumps(
                    self.codex_auth(
                        f"acct-instance-{index}-fixture",
                        f"instance-{index}@example.test",
                        f"instance-{index}-principal",
                    )
                ),
                encoding="utf-8",
            )
            session = sessions / f"rollout-instance-{index}.jsonl"
            session.write_text(
                "\n".join(
                    json.dumps(item)
                    for item in (
                        {
                            "timestamp": f"2026-08-16T02:0{index}:00Z",
                            "type": "session_meta",
                            "payload": {"model_provider": "openai"},
                        },
                        {
                            "timestamp": f"2026-08-16T02:0{index}:01Z",
                            "type": "turn_context",
                            "payload": {"model": "fake-responses"},
                        },
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            sessions_by_home[home] = session
        (broken / "sessions").mkdir(parents=True)
        (cockpit_dir / "codex_instances.json").write_text(
            json.dumps(
                {
                    "instances": [
                        {"id": "secondary", "userDataDir": str(instance)},
                        {"id": "duplicate", "userDataDir": str(primary)},
                        {"id": "broken", "userDataDir": str(broken)},
                        {"id": "unsafe", "userDataDir": str(outside)},
                    ]
                }
            ),
            encoding="utf-8",
        )
        resolver = meter.AccountResolver(
            home=self.temp_path,
            refresh_seconds=0,
            cockpit_tools_data_dir=cockpit_dir,
        )
        importer = meter.CodexAppLocalImporter(
            self.sidecar.repo,
            resolver,
            primary,
            alias=None,
        )

        baseline = importer.import_once()

        self.assertEqual(baseline["discovered_homes"], 3)
        self.assertEqual(baseline["matched_homes"], 2)
        self.assertEqual(baseline["failed_homes"], 1)
        self.assertEqual(baseline["rejected_homes"], 1)
        self.assertEqual(baseline["baselined_files"], 2)
        for index, home in enumerate((primary, instance), start=1):
            with sessions_by_home[home].open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        self.codex_token_record(
                            f"2026-08-16T02:1{index}:00Z",
                            index,
                            index * 10,
                        )
                    )
                    + "\n"
                )

        imported = importer.import_once()

        self.assertEqual(imported["imported"], 2)
        self.assertEqual(imported["matched_homes"], 2)
        self.assertEqual(imported["failed_homes"], 1)
        self.assertEqual(imported["rejected_homes"], 1)
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*) AS count FROM usage_events "
                "WHERE source='codex_app_local'"
            )[0]["count"],
            2,
        )
        status = importer.status()
        self.assertEqual(status["account_mode"], "dynamic")
        self.assertEqual(status["discovered_homes"], 3)
        self.assertEqual(status["matched_homes"], 2)
        self.assertNotIn(str(primary), json.dumps(status))
        self.assertNotIn(str(instance), json.dumps(status))
        active = resolver.active_subscription_keys(force_refresh=True)
        self.assertEqual(len(active), 2)
        self.assertEqual(
            {row["identity_key"] for row in self.rows("SELECT identity_key FROM usage_events")},
            active,
        )
        self.assertEqual(
            {resolver.resolve_identity_key(key).account_email for key in active},
            {"instance-1@example.test", "instance-2@example.test"},
        )

        # Removing one configured home removes only its contribution.
        (cockpit_dir / "codex_instances.json").write_text(
            json.dumps({"instances": []}), encoding="utf-8"
        )
        importer.import_once()
        active = resolver.active_subscription_keys(force_refresh=True)
        self.assertEqual(len(active), 1)
        self.assertEqual(
            resolver.resolve_identity_key(next(iter(active))).account_email,
            "instance-1@example.test",
        )

    def test_local_member_observation_preserves_existing_deletion_state(self) -> None:
        key = subscription_key("deleted-local-member-fixture")
        self.sidecar.repo.reconcile_subscription_inventory({key})
        with self.sidecar.repo.connect() as conn:
            conn.execute(
                "UPDATE active_subscription_registry SET state='suspect_missing' WHERE identity_key=?",
                (key,),
            )
        self.sidecar.repo.register_codex_app_subscription(key)
        self.assertEqual(
            self.rows("SELECT state FROM active_subscription_registry")[0]["state"],
            "suspect_missing",
        )
        with self.sidecar.repo.connect() as conn:
            conn.execute("DELETE FROM active_subscription_registry WHERE identity_key=?", (key,))
            conn.execute(
                "INSERT INTO retired_subscription_tombstones VALUES (?, ?, 1)",
                (key, meter.utc_now()),
            )
        self.sidecar.repo.register_codex_app_subscription(key)
        self.assertEqual(self.rows("SELECT * FROM active_subscription_registry"), [])
        self.assertEqual(len(self.rows("SELECT * FROM retired_subscription_tombstones")), 1)

    def test_invalid_structured_email_does_not_mask_valid_jwt_email(self) -> None:
        account, _tokens, email, principal, _provider_id = (
            meter.AccountResolver._account_and_tokens(
                {
                    "type": "codex",
                    "account_id": "acct-email-fallback-fixture",
                    "email": "not-an-email",
                    "id_token": fixture_jwt(
                        {"email": "jwt-fallback+team@example.test", "sub": "fixture-subject"}
                    ),
                }
            )
        )
        self.assertEqual(account, "acct-email-fallback-fixture")
        self.assertEqual(email, "jwt-fallback+team@example.test")
        self.assertEqual(principal, "fixture-subject")

    def test_conflicting_structured_emails_reject_complete_auth_record(self) -> None:
        parsed = meter.AccountResolver._account_and_tokens(
            {
                "type": "codex",
                "account_id": "acct-email-conflict-fixture",
                "email": "first-member@example.test",
                "access_token": "fixture-conflicting-email-token",
                "id_token": fixture_jwt(
                    {
                        "email": "second-member@example.test",
                        "sub": "fixture-conflicting-email-subject",
                    }
                ),
            }
        )

        self.assertEqual(parsed, (None, [], None, None, None))
        nested_jwt_conflict = meter.AccountResolver._account_and_tokens(
            {
                "type": "codex",
                "account_id": "acct-nested-email-conflict-fixture",
                "id_token": fixture_jwt(
                    {"email": "top-token-member@example.test"}
                ),
                "tokens": {
                    "id_token": fixture_jwt(
                        {"email": "nested-token-member@example.test"}
                    )
                },
            }
        )
        self.assertEqual(nested_jwt_conflict, (None, [], None, None, None))

    def test_all_time_query_keeps_history_outside_seven_day_view(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(2)
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE usage_events SET ts='2020-01-01T00:00:00.000000Z' WHERE id=1")
        week = self.sidecar.repo.summary("7d")
        all_time = self.sidecar.repo.summary("all")
        self.assertEqual(week["calls"], 1)
        self.assertEqual(week["total_tokens"], 150)
        self.assertEqual(all_time["calls"], 2)
        self.assertEqual(all_time["total_tokens"], 300)
        self.assertEqual(all_time["codex_status_tokens"], 260)
        self.assertEqual(all_time["api_processed_tokens"], 300)
        self.assertEqual(all_time["cached_tokens"], 40)
        self.assertIsNone(all_time["since"])
        coverage = self.sidecar.repo.coverage()
        self.assertEqual(coverage["first_event_ts"], "2020-01-01T00:00:00.000000Z")
        dates = self.sidecar.repo.grouped("all", "date")
        self.assertEqual(sum(row["calls"] for row in dates), 2)

    def test_codex_quota_parser_and_subscription_dashboard(self) -> None:
        fetched_at = "2026-08-12T00:00:00Z"
        fixture_key = subscription_key("quota-parser-fixture")
        windows = meter.parse_codex_quota_windows(
            {
                "plan_type": "plus",
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 22,
                        "limit_window_seconds": 18000,
                        "reset_at": 1786500000,
                    },
                    "secondary_window": {
                        "used_percent": 61.5,
                        "limit_window_seconds": 604800,
                        "reset_after_seconds": 3600,
                    },
                },
            },
            fetched_at,
        )
        self.assertEqual([row["window_kind"] for row in windows], ["five_hour", "weekly"])
        self.assertEqual(windows[0]["remaining_percent"], 78.0)
        self.assertEqual(windows[1]["remaining_percent"], 38.5)
        app_windows = meter.parse_codex_app_rate_windows(
            {
                "primary": {
                    "used_percent": 100,
                    "window_minutes": 10_080,
                }
            },
            fetched_at,
        )
        self.assertEqual(app_windows[0]["window_kind"], "weekly")
        self.assertEqual(app_windows[0]["window_seconds"], 604_800)
        gated_windows = meter.parse_codex_quota_windows(
            {
                "rate_limit": {
                    "allowed": False,
                    "limit_reached": True,
                    "primary_window": {
                        "used_percent": 0,
                        "limit_window_seconds": 604_800,
                    },
                }
            },
            fetched_at,
        )
        self.assertEqual(
            (gated_windows[0]["provider_allowed"], gated_windows[0]["provider_limit_reached"]),
            (False, True),
        )
        for window in windows:
            self.sidecar.repo.insert_subscription_quota_snapshot(
                {
                    **window,
                    "identity_key": fixture_key,
                    "account_id_hash": "fixturehash",
                    "account_id_tail": "ABCDEFGH",
                    "usage_alias": "codex-1",
                    "plan_type": "plus",
                    "source": "fixture",
                }
            )
        subscriptions = self.sidecar.repo.subscription_dashboard_rows()
        fixture = next(row for row in subscriptions if row["identity_key"] == fixture_key)
        self.assertEqual(fixture["plan_type"], "plus")
        self.assertEqual(fixture["windows"]["five_hour"]["remaining_percent"], 78.0)
        page = meter.dashboard_html(
            self.sidecar.repo,
            {
                "key_loaded": True,
                "last_status": 200,
                "quota_routing_guard": {"enabled": True, "active_locks": 2},
            },
            {"last_success_at": fetched_at, "account_count": 1},
        )
        self.assertIn("78%", page)
        self.assertIn("38.5%", page)
        self.assertIn("已用 22% · 剩余 78%", page)
        self.assertIn("1 个账号已刷新", page)
        self.assertIn("Quota guard <b>开启 · 2 锁", page)
        page_after_restart_timeout = meter.dashboard_html(
            self.sidecar.repo,
            {"key_loaded": True, "last_status": 200},
            {"last_success_at": None, "account_count": 0, "last_error_type": "TimeoutError"},
        )
        self.assertIn("1 个账号已刷新", page_after_restart_timeout)
        self.assertIn("Quota snapshot <b>正常", page_after_restart_timeout)
        self.assertIn("自动跟随当前账号", page)
        self.assertNotIn('action="/usage/manual-import"', page)

    def test_latest_subscription_quota_uses_fetched_time_not_backfill_id(self) -> None:
        fixture_key = subscription_key("latest-quota-fixture")
        common = {
            "identity_key": fixture_key,
            "account_id_hash": "fixturehash",
            "account_id_tail": "ABCDEFGH",
            "usage_alias": "codex-1",
            "plan_type": "plus",
            "window_kind": "weekly",
            "window_seconds": 604800,
            "source": "fixture",
        }
        # Insert the current reset first, then simulate an older JSONL file
        # being backfilled later with a larger SQLite id.
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                **common,
                "fetched_at": "2026-08-13T04:00:00Z",
                "used_percent": 0,
                "remaining_percent": 100,
                "reset_at": "2026-08-20T04:00:00Z",
            }
        )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                **common,
                "fetched_at": "2026-08-12T16:00:00Z",
                "used_percent": 43,
                "remaining_percent": 57,
                "reset_at": "2026-08-19T12:00:00Z",
            }
        )
        row = next(
            item for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == fixture_key
        )
        self.assertEqual(row["windows"]["weekly"]["used_percent"], 0)
        self.assertEqual(row["windows"]["weekly"]["remaining_percent"], 100)
        self.assertEqual(row["windows"]["weekly"]["reset_at"], "2026-08-20T04:00:00Z")

    def test_principal_snapshot_supersedes_legacy_workspace_across_window_kinds(self) -> None:
        principal_key = subscription_key("principal-fixture")
        common = {
            "account_id_hash": "sharedfixturehash",
            "account_id_tail": "ABCDEFGH",
            "usage_alias": "codex-2",
            "plan_type": "team",
            "used_percent": 25,
            "remaining_percent": 75,
            "source": "fixture",
        }
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO subscription_quota_snapshots (
                     fetched_at, identity_key, plan_type, window_kind,
                     used_percent, remaining_percent, source
                   ) VALUES (
                     '2026-08-15T00:00:00Z', 'account:sharedfixturehash',
                     'team', 'monthly', 25, 75, 'fixture'
                   )"""
            )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                **common,
                "fetched_at": "2026-08-15T00:01:00Z",
                "identity_key": principal_key,
                "window_kind": "weekly",
            }
        )
        rows = self.sidecar.repo.latest_subscription_quotas()
        # Privacy-minimized snapshots deliberately discard the workspace hash,
        # so different historical keys cannot be correlated heuristically.
        # Proven lineage is migrated before persistence; otherwise both safe
        # opaque keys remain independent.
        self.assertEqual(
            [(row["identity_key"], row["window_kind"]) for row in rows],
            [
                ("account:sharedfixturehash", "monthly"),
                (principal_key, "weekly"),
            ],
        )

    def test_dashboard_displays_local_account_email_without_persisting_it(self) -> None:
        identity = self.sidecar.resolver.resolve("codex-1", None)
        self.assertEqual(identity.account_email, "fixture@example.com")
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(1)
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-13T04:00:00Z",
                "identity_key": meter.resolved_identity_key(identity),
                "account_id_hash": identity.account_id_hash,
                "account_id_tail": identity.account_id_tail,
                "usage_alias": "codex-1",
                "plan_type": "plus",
                "window_kind": "weekly",
                "used_percent": 0,
                "remaining_percent": 100,
                "window_seconds": 604800,
                "reset_at": "2026-08-20T04:00:00Z",
                "source": "fixture",
            }
        )
        page = meter.dashboard_html(
            self.sidecar.repo,
            account_resolver=self.sidecar.resolver,
        )
        # The subscription card, account totals, and recent-attempt table each
        # use the email exactly once.  The local routing alias is never used as
        # a dashboard identity label.
        self.assertEqual(page.count("fixture@example.com"), 3)
        self.assertNotIn("codex-1", page)
        unresolved_page = meter.dashboard_html(self.sidecar.repo)
        self.assertGreaterEqual(unresolved_page.count("邮箱未获取"), 3)
        self.assertNotIn("codex-1", unresolved_page)
        with sqlite3.connect(self.db) as conn:
            serialized = "\n".join(
                str(value)
                for row in conn.execute("SELECT * FROM subscription_quota_snapshots")
                for value in row
            )
        self.assertNotIn("fixture@example.com", serialized)

    def test_subscription_estimate_uses_provider_window_and_meaningful_identity_badge(self) -> None:
        # A near-cap provider snapshot must use the observed spend, not the
        # last fragmented 429 cycle.  A low-use freshly reset account stays
        # explicitly unprojected.
        identity = self.sidecar.resolver.resolve("codex-1", None)
        canonical_key = meter.resolved_identity_key(identity)
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-12T12:00:00Z",
                "identity_key": canonical_key,
                "plan_type": "plus",
                "window_kind": "weekly",
                "used_percent": 100,
                "remaining_percent": 0,
                "window_seconds": 604800,
                "reset_at": "2026-08-19T12:00:00Z",
                "source": "fixture",
            }
        )
        rows = self.sidecar.repo.subscription_dashboard_rows()
        fixture = next(row for row in rows if row["identity_key"] == canonical_key)
        self.assertIsNone(fixture["current_window_full_quota_usd"])
        self.assertEqual(fixture["quota_estimate_method"], "observed_floor_only")
        self.assertIsNone(fixture["usage_alias"])
        self.assertEqual(meter.identity_badge(fixture)[0], "A")
        rendered = dict(fixture)
        rendered["usage_alias"] = self.sidecar.resolver.resolve_identity_key(
            canonical_key
        ).usage_alias
        self.assertEqual(meter.identity_badge(rendered)[0], "C")
        anonymous = {"usage_alias": "auth:abcd", "identity_key": "account:x"}
        self.assertEqual(meter.identity_badge(anonymous)[0], "A")

    def test_low_use_current_window_can_transfer_measured_previous_window(self) -> None:
        fixture_key = subscription_key("low-use-window-fixture")
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                    model, ok, status_code, input_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count, usage_missing)
                   VALUES ('2026-08-10T12:00:00.000000Z', ?,
                           'fixturehash', 'ABCDEFGH', 'codex-1', 'fake-responses',
                           1, 200, 1000000, 0, 1000000, 20.0, 1, 0)""",
                (fixture_key,),
            )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-11T11:00:00Z",
                "identity_key": fixture_key,
                "account_id_hash": "fixturehash",
                "account_id_tail": "ABCDEFGH",
                "usage_alias": "codex-1",
                "window_kind": "weekly",
                "used_percent": 100,
                "remaining_percent": 0,
                "window_seconds": 604800,
                "reset_at": "2026-08-12T12:00:00Z",
                "source": "fixture",
            }
        )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-12T13:00:00Z",
                "identity_key": fixture_key,
                "account_id_hash": "fixturehash",
                "account_id_tail": "ABCDEFGH",
                "usage_alias": "codex-1",
                "window_kind": "weekly",
                "used_percent": 5,
                "remaining_percent": 95,
                "window_seconds": 604800,
                "reset_at": "2026-08-19T12:00:00Z",
                "source": "fixture",
            }
        )
        row = next(
            item for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == fixture_key
        )
        self.assertAlmostEqual(row["current_window_full_quota_usd"], 20.0, places=8)
        self.assertEqual(row["quota_estimate_method"], "previous_window_transfer")

    def test_high_used_current_window_projects_remaining_five_percent(self) -> None:
        fixture_key = subscription_key("high-use-window-fixture")
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                    model, ok, status_code, input_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count, usage_missing)
                   VALUES ('2026-08-12T12:00:00.000000Z', ?,
                           'fixturehash', 'ABCDEFGH', 'codex-1', 'fake-responses',
                           1, 200, 9500000, 0, 9500000, 95.0, 1, 0)""",
                (fixture_key,),
            )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-12T13:00:00Z",
                "identity_key": fixture_key,
                "account_id_hash": "fixturehash",
                "account_id_tail": "ABCDEFGH",
                "usage_alias": "codex-1",
                "window_kind": "weekly",
                "used_percent": 95,
                "remaining_percent": 5,
                "window_seconds": 604800,
                "reset_at": "2026-08-19T12:00:00Z",
                "source": "fixture",
            }
        )
        row = next(
            item for item in self.sidecar.repo.subscription_dashboard_rows()
            if item["identity_key"] == fixture_key
        )
        self.assertAlmostEqual(row["current_window_full_quota_usd"], 100.0, places=8)
        self.assertEqual(row["quota_estimate_method"], "current_window_percent_projection")

    def test_five_percent_used_gives_fast_initial_estimate(self) -> None:
        fixture_key = subscription_key("initial-estimate-fixture")
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, identity_key, account_id_hash, account_id_tail, usage_alias,
                    model, ok, status_code, input_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count, usage_missing)
                   VALUES ('2026-08-12T12:00:00.000000Z', ?,
                           'fixturehash', 'ABCDEFGH', 'codex-1', 'fake-responses',
                           1, 200, 500000, 0, 500000, 5.0, 1, 0)""",
                (fixture_key,),
            )
        self.sidecar.repo.insert_subscription_quota_snapshot(
            {
                "fetched_at": "2026-08-12T13:00:00Z",
                "identity_key": fixture_key,
                "account_id_hash": "fixturehash",
                "account_id_tail": "ABCDEFGH",
                "usage_alias": "codex-1",
                "window_kind": "weekly",
                "used_percent": 5,
                "remaining_percent": 95,
                "window_seconds": 604800,
                "reset_at": "2026-08-19T12:00:00Z",
                "source": "fixture",
            }
        )
        row = next(item for item in self.sidecar.repo.subscription_dashboard_rows() if item["identity_key"] == fixture_key)
        self.assertAlmostEqual(row["current_window_full_quota_usd"], 100.0, places=8)
        self.assertEqual(row["quota_estimate_confidence"], "initial")

    def test_official_pricing_parser_reads_standard_short_context_rates(self) -> None:
        fake_html = """
        <div data-content-switcher-pane="true" data-value="standard">
          <astro-island component-export="TextTokenPricingTables">
            <table>
              <thead><tr><th>Model</th><th>Input</th><th>Cached input</th><th>Cache writes</th><th>Output</th></tr></thead>
              <tbody>
                <tr><td>gpt-5.6-sol</td><td>$5.00</td><td>$0.50</td><td>$6.25</td><td>$30.00</td></tr>
                <tr><td>gpt-5.5 (&lt;272K context length)</td><td>$5.00</td><td>$0.50</td><td>-</td><td>$30.00</td></tr>
              </tbody>
            </table>
          </astro-island>
        </div>
        """
        rows = meter.parse_official_pricing_html(fake_html)
        self.assertEqual([row["model_pattern"] for row in rows], ["gpt-5.6-sol", "gpt-5.5"])
        for row in rows:
            self.assertEqual(row["input_per_million"], 5.0)
            self.assertEqual(row["cached_input_per_million"], 0.5)
            self.assertEqual(row["output_per_million"], 30.0)
            self.assertEqual(row["long_context_threshold_tokens"], 272000)
            self.assertEqual(row["long_input_per_million"], 10.0)
            self.assertEqual(row["long_cached_input_per_million"], 1.0)
            self.assertEqual(row["long_output_per_million"], 45.0)

    def test_astra_official_rates_include_cache_and_context_threshold(self) -> None:
        document = """
        <div data-content-switcher-pane="true" data-value="standard">
          <astro-island component-export="TextTokenPricingTables">
            <table><tr><th>Model</th><th>Input</th><th>Cached input</th><th>Output</th></tr>
            <tr><td>gpt-6-astra</td><td>$10</td><td>$1</td><td>$50</td></tr>
            <tr><td>gpt-6-unpriced-fixture</td><td>$2</td><td>$0.2</td><td>$12</td></tr></table>
          </astro-island>
        </div>
        """
        rows = meter.parse_official_pricing_html(document)
        astra = rows[0]
        self.assertEqual(astra["cache_write_per_million"], 12.5)
        self.assertEqual(astra["long_context_threshold_tokens"], 272000)
        self.assertEqual(astra["long_input_per_million"], 20)
        self.assertEqual(astra["long_cached_input_per_million"], 2)
        self.assertEqual(astra["long_cache_write_per_million"], 25)
        self.assertEqual(astra["long_output_per_million"], 75)
        self.assertIsNone(rows[1].get("long_context_threshold_tokens"))
        components = self.sidecar.repo._components_for_price(
            meter.NormalizedUsage(input_tokens=112075, cached_tokens=110720, output_tokens=2049),
            astra,
        )
        self.assertAlmostEqual(components.non_cached_input_cost_usd, 0.01355)
        self.assertAlmostEqual(components.cached_input_cost_usd, 0.11072)
        self.assertAlmostEqual(components.output_cost_usd, 0.10245)
        self.assertAlmostEqual(components.total_cost_usd, 0.22672)
        for input_tokens, is_long in ((272000, False), (272001, True)):
            components = self.sidecar.repo._components_for_price(
                meter.NormalizedUsage(input_tokens=input_tokens, cached_tokens=270000, output_tokens=100),
                astra,
            )
            self.assertEqual(components.long_context_pricing_applied, is_long)

    def test_official_pricing_parser_uses_complete_ssr_props_not_collapsed_rows(self) -> None:
        fake_html = """
        <div data-content-switcher-pane="true" data-value="standard">
          <astro-island component-export="TextTokenPricingTables"
            props="{&quot;rows&quot;:[1,[[1,[[0,&quot;gpt-5.6-sol&quot;],[0,5],[0,0.5],[0,6.25],[0,30]]],[1,[[0,&quot;gpt-5.6-terra&quot;],[0,2],[0,0.2],[0,2.5],[0,12]]]]]}">
            <table><thead><tr><th>Model</th><th>Input</th><th>Cached input</th><th>Cache writes</th><th>Output</th></tr></thead>
            <tbody><tr><td>gpt-5.6-sol</td><td>$5.00</td><td>$0.50</td><td>$6.25</td><td>$30.00</td></tr></tbody></table>
          </astro-island>
        </div>
        """
        rows = meter.parse_official_pricing_html(fake_html)
        self.assertEqual([row["model_pattern"] for row in rows], ["gpt-5.6-sol", "gpt-5.6-terra"])
        self.assertEqual(rows[1]["cached_input_per_million"], 0.2)
        self.assertEqual(rows[1]["output_per_million"], 12.0)

    def test_official_pricing_parser_reads_grouped_long_context_rates(self) -> None:
        fake_html = """
        <div data-content-switcher-pane="true" data-value="standard">
          <astro-island component-export="GroupedPricingTable"
            props="{&quot;headings&quot;:[1,[[0,&quot;Model&quot;],[0,&quot;Short context input&quot;],[0,&quot;Long context input&quot;]]],&quot;groups&quot;:[1,[[0,{&quot;model&quot;:[0,&quot;gpt-5.6-sol&quot;],&quot;rows&quot;:[1,[[1,[[0,5],[0,0.5],[0,6.25],[0,30],[0,10],[0,1],[0,12.5],[0,45]]]]]}],[0,{&quot;model&quot;:[0,&quot;gpt-no-long&quot;],&quot;rows&quot;:[1,[[1,[[0,1],[0,0.1],[0,&quot;-&quot;],[0,2],[0,&quot;-&quot;],[0,&quot;-&quot;],[0,&quot;-&quot;],[0,&quot;-&quot;]]]]]}]]}"></astro-island>
          <astro-island component-export="TextTokenPricingTables">
            <table><thead><tr><th>Model</th><th>Input</th><th>Cached input</th><th>Output</th></tr></thead>
            <tbody><tr><td>gpt-5.6-sol</td><td>$5</td><td>$0.5</td><td>$30</td></tr></tbody></table>
          </astro-island>
        </div>
        """
        rows = meter.parse_official_pricing_html(fake_html)
        sol = next(row for row in rows if row["model_pattern"] == "gpt-5.6-sol")
        self.assertEqual(sol["long_context_threshold_tokens"], 272000)
        self.assertEqual(sol["long_input_per_million"], 10.0)
        self.assertEqual(sol["long_cached_input_per_million"], 1.0)
        self.assertEqual(sol["long_cache_write_per_million"], 12.5)
        self.assertEqual(sol["long_output_per_million"], 45.0)
        no_long = next(row for row in rows if row["model_pattern"] == "gpt-no-long")
        self.assertIsNone(no_long["long_context_threshold_tokens"])

    def test_long_context_pricing_counts_cached_input_and_uses_strict_threshold(self) -> None:
        price = {
            "input_per_million": 5.0,
            "cached_input_per_million": 0.5,
            "cache_write_per_million": 6.25,
            "output_per_million": 30.0,
            "long_context_threshold_tokens": 272000,
            "long_input_per_million": 10.0,
            "long_cached_input_per_million": 1.0,
            "long_cache_write_per_million": 12.5,
            "long_output_per_million": 45.0,
        }
        exact = meter.UsageRepository._components_for_price(
            meter.NormalizedUsage(input_tokens=272000, cached_tokens=270000, output_tokens=1000),
            price,
        )
        assert exact is not None
        self.assertFalse(exact.long_context_pricing_applied)
        self.assertAlmostEqual(exact.total_cost_usd, 0.175, places=12)

        over = meter.UsageRepository._components_for_price(
            meter.NormalizedUsage(input_tokens=272001, cached_tokens=270001, output_tokens=1000),
            price,
        )
        assert over is not None
        self.assertTrue(over.long_context_pricing_applied)
        self.assertAlmostEqual(over.non_cached_input_cost_usd, 0.02, places=12)
        self.assertAlmostEqual(over.cached_input_cost_usd, 0.270001, places=12)
        self.assertAlmostEqual(over.output_cost_usd, 0.045, places=12)

    def test_long_context_cost_upgrade_is_idempotent_and_provenance_safe(self) -> None:
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """UPDATE model_prices
                      SET long_context_threshold_tokens=272000,
                          long_input_per_million=2.0,
                          long_cached_input_per_million=1.0,
                          long_output_per_million=3.0
                    WHERE model_pattern='fake-*'"""
            )
            conn.execute(
                """INSERT INTO usage_events
                   (ts, model, input_tokens, cached_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, non_cached_input_cost_usd,
                    cached_input_cost_usd, output_cost_usd, call_count)
                   VALUES ('2026-08-13T00:00:00Z', 'fake-responses', 300000, 200000,
                           10000, 310000, 0.22, 0.1, 0.1, 0.02, 1),
                          ('2026-08-13T00:00:01Z', 'fake-responses', 300000, 200000,
                           10000, 310000, 99, 33, 33, 33, 1)"""
            )
        self.assertEqual(self.sidecar.repo.upgrade_long_context_costs(), 1)
        rows = self.rows(
            """SELECT estimated_api_cost_usd, non_cached_input_cost_usd,
                      cached_input_cost_usd, output_cost_usd,
                      long_context_pricing_applied
                 FROM usage_events ORDER BY id"""
        )
        self.assertAlmostEqual(rows[0]["estimated_api_cost_usd"], 0.43, places=12)
        self.assertEqual(rows[0]["long_context_pricing_applied"], 1)
        self.assertEqual(rows[1]["estimated_api_cost_usd"], 99)
        self.assertEqual(rows[1]["long_context_pricing_applied"], 0)
        self.assertEqual(self.sidecar.repo.upgrade_long_context_costs(), 0)

    def test_official_price_sync_is_atomic_and_failure_keeps_old_prices(self) -> None:
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO usage_events
                   (ts, model, input_tokens, cached_tokens, output_tokens, total_tokens,
                    estimated_api_cost_usd, call_count)
                   VALUES ('2020-01-01T00:00:00.000000Z', 'gpt-5.6-sol',
                           1000000, 500000, 100000, 1100000, NULL, 1)"""
            )
        fake_html = b"""
        <div data-content-switcher-pane="true" data-value="standard">
          <astro-island component-export="GroupedPricingTable"
            props="{&quot;headings&quot;:[1,[[0,&quot;Model&quot;],[0,&quot;Short context input&quot;],[0,&quot;Long context input&quot;]]],&quot;groups&quot;:[1,[[0,{&quot;model&quot;:[0,&quot;gpt-5.6-sol&quot;],&quot;rows&quot;:[1,[[1,[[0,5],[0,0.5],[0,6.25],[0,30],[0,10],[0,1],[0,12.5],[0,45]]]]]}]]}"></astro-island>
          <astro-island component-export="TextTokenPricingTables">
            <table><thead><tr><th>Model</th><th>Input</th><th>Cached input</th><th>Output</th></tr></thead>
            <tbody><tr><td>gpt-5.6-sol</td><td>$5.00</td><td>$0.50</td><td>$30.00</td></tr></tbody></table>
          </astro-island>
        </div>
        """
        digest = hashlib.sha256(fake_html).hexdigest()
        with mock.patch.object(
            meter,
            "fetch_official_pricing_html",
            return_value=(meter.OFFICIAL_PRICING_URL, fake_html, digest),
        ):
            result = meter.sync_official_prices(self.sidecar.repo)
        self.assertEqual(result["model_count"], 1)
        self.assertEqual(result["repriced_events"], 1)
        repriced = self.rows(
            """SELECT estimated_api_cost_usd, non_cached_input_cost_usd,
                      cached_input_cost_usd, output_cost_usd
                 FROM usage_events WHERE model='gpt-5.6-sol'"""
        )[0]
        self.assertAlmostEqual(repriced["estimated_api_cost_usd"], 10.0, places=10)
        self.assertAlmostEqual(repriced["non_cached_input_cost_usd"], 5.0, places=10)
        self.assertAlmostEqual(repriced["cached_input_cost_usd"], 0.5, places=10)
        self.assertAlmostEqual(repriced["output_cost_usd"], 4.5, places=10)
        price = next(row for row in self.sidecar.repo.list_prices() if row["model_pattern"] == "gpt-5.6-sol")
        self.assertEqual(price["source_kind"], "official")
        self.assertEqual(price["output_per_million"], 30.0)
        self.assertEqual(self.sidecar.repo.price_sync_status()["status"], "ok")

        with mock.patch.object(
            meter,
            "fetch_official_pricing_html",
            side_effect=meter.OfficialPriceSyncError("fixture parse failure"),
        ):
            with self.assertRaises(meter.OfficialPriceSyncError):
                meter.sync_official_prices(self.sidecar.repo)
        price_after = next(
            row for row in self.sidecar.repo.list_prices() if row["model_pattern"] == "gpt-5.6-sol"
        )
        self.assertEqual(price_after["output_per_million"], 30.0)
        status = self.sidecar.repo.price_sync_status()
        self.assertEqual(status["status"], "error")
        self.assertEqual(status["error_type"], "OfficialPriceSyncError")

    def test_summary_counts_success_failure_streaming_and_complete_quota(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.request("POST", "/v1/fail", {"model": "fake-error"})
        self.request("POST", "/v1/stream-no-usage", {"model": "fake-stream", "stream": True})
        self.wait_events(3)
        summary = self.sidecar.repo.summary("today")
        self.assertEqual(summary["calls"], 3)
        self.assertEqual(summary["successful_calls"], 2)
        self.assertEqual(summary["failed_calls"], 1)
        self.assertEqual(summary["streaming_calls"], 1)
        self.assertEqual(summary["total_tokens"], 150)
        self.assertAlmostEqual(summary["api_equivalent_quota_usd"], 0.00019, places=10)

    def test_codex_status_token_breakdown_and_logical_request_attempts(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.request("POST", "/v1/chat/completions", {"model": "fake-chat", "messages": []})
        self.wait_events(2)
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            # The HTTP server handles requests concurrently, so SQLite insertion
            # order is not a stable proxy for endpoint/model order. Select the
            # rows by their fixture model before assigning logical request IDs.
            source = conn.execute(
                "SELECT * FROM usage_events WHERE model=? ORDER BY id LIMIT 1",
                ("fake-responses",),
            ).fetchone()
            chat = conn.execute(
                "SELECT id FROM usage_events WHERE model=? ORDER BY id LIMIT 1",
                ("fake-chat",),
            ).fetchone()
            self.assertIsNotNone(source)
            self.assertIsNotNone(chat)
            conn.execute(
                "UPDATE usage_events SET request_id=? WHERE id=?",
                (meter.short_hash("logical-a"), source["id"]),
            )
            conn.execute(
                "UPDATE usage_events SET request_id=? WHERE id=?",
                (meter.short_hash("logical-b"), chat["id"]),
            )
            columns = [row[1] for row in conn.execute("PRAGMA table_info(usage_events)")]
            values = {column: source[column] for column in columns}
            values.pop("id")
            # The retry belongs to the Responses logical request, even though
            # the row snapshot above predates the UPDATE statement.
            values["request_id"] = meter.short_hash("logical-a")
            values["usage_alias"] = "codex-retry"
            values["ok"] = 0
            values["status_code"] = 429
            values["error_type"] = "rate_limit_error"
            placeholders = ",".join("?" for _ in values)
            conn.execute(
                f"INSERT INTO usage_events ({','.join(values)}) VALUES ({placeholders})",
                tuple(values.values()),
            )

        summary = self.sidecar.repo.summary("all")
        self.assertEqual(summary["account_attempts"], 3)
        self.assertEqual(summary["logical_requests"], 2)
        self.assertEqual(summary["retry_attempts"], 1)
        self.assertEqual(summary["successful_calls"], 2)
        self.assertEqual(summary["failed_calls"], 1)
        self.assertEqual(summary["successful_logical_requests"], 2)
        self.assertEqual(summary["failed_logical_requests"], 0)
        self.assertEqual(summary["input_tokens"], 230)
        self.assertEqual(summary["cached_tokens"], 45)
        self.assertEqual(summary["non_cached_input_tokens"], 185)
        self.assertEqual(summary["output_tokens"], 110)
        self.assertEqual(summary["codex_status_tokens"], 295)
        self.assertEqual(summary["api_processed_tokens"], 340)
        self.assertEqual(summary["total_tokens"], 340)
        self.assertAlmostEqual(summary["cache_hit_rate_percent"], 45 / 230 * 100)
        self.assertAlmostEqual(summary["non_cached_input_cost_usd"], 0.000185, places=10)
        self.assertAlmostEqual(summary["cached_input_cost_usd"], 0.0000225, places=10)
        self.assertAlmostEqual(summary["output_cost_usd"], 0.00022, places=10)
        self.assertAlmostEqual(
            summary["split_cost_total_usd"], summary["estimated_api_cost_usd"], places=10
        )

        breakdown = self.sidecar.repo.token_breakdown("all")
        self.assertEqual(breakdown["codex_status_tokens"], 295)
        self.assertEqual(breakdown["reasoning_tokens"], 24)
        self.assertNotEqual(
            breakdown["codex_status_tokens"] + breakdown["reasoning_tokens"],
            breakdown["api_processed_tokens"],
            "reasoning is an output subset and must not be added again",
        )
        models = self.sidecar.repo.grouped("all", "model")
        responses = next(row for row in models if row["model"] == "fake-responses")
        self.assertEqual(responses["account_attempts"], 2)
        self.assertEqual(responses["logical_requests"], 1)
        self.assertEqual(responses["retry_attempts"], 1)
        self.assertEqual(responses["codex_status_tokens"], 260)

        page = meter.dashboard_html(self.sidecar.repo)
        self.assertIn("成功 2 · 失败 1 · 请求关联不落库", page)
        self.assertIn('总调用 <b>3</b>', page)
        self.assertIn('成功 <b>2</b>', page)
        self.assertIn('失败 <b>1</b>', page)
        self.assertIn('缓存输入 <b>45</b>', page)
        self.assertIn("会话关联已按隐私策略关闭", page)
        self.assertIn("实际消耗 = max(输入−缓存, 0)+输出", page)
        self.assertIn("非缓存输入 Tokens", page)
        self.assertIn("输出 Tokens", page)
        self.assertIn("输入成本 $0.000185", page)
        self.assertIn("输出成本 $0.000220", page)

    def test_manual_quota_and_reset_commands_preserve_cycle_history(self) -> None:
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(1)
        command_home = self.temp_path / "home"
        command_environment = os.environ.copy()
        command_environment["HOME"] = str(command_home)
        base_command = [sys.executable, str(SCRIPT), "--db", str(self.db)]
        quota_result = subprocess.run(
            [*base_command, "--mark-quota-hit", "codex-1"],
            cwd=ROOT,
            env=command_environment,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(quota_result.returncode, 0, quota_result.stderr)
        reset_result = subprocess.run(
            [*base_command, "--mark-reset", "codex-1"],
            cwd=ROOT,
            env=command_environment,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(reset_result.returncode, 0, reset_result.stderr)
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.request("POST", "/v1/responses", {"model": "fake-responses"})
        self.wait_events(3)
        quota_result = subprocess.run(
            [*base_command, "--mark-quota-hit", "codex-1"],
            cwd=ROOT,
            env=command_environment,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(quota_result.returncode, 0, quota_result.stderr)

        quotas = self.sidecar.repo.quota_summary("30d")
        self.assertEqual(quotas[0]["complete_cycles_in_period"], 2)
        self.assertAlmostEqual(quotas[0]["historical_min_usd"], 0.00019, places=10)
        self.assertAlmostEqual(quotas[0]["historical_p50_usd"], 0.000285, places=10)
        self.assertAlmostEqual(quotas[0]["historical_max_usd"], 0.00038, places=10)
        cycles = self.rows("SELECT is_complete_cycle FROM account_quota_cycles ORDER BY id")
        self.assertEqual([row["is_complete_cycle"] for row in cycles], [1, 1])
        quota_events = [row["event_type"] for row in self.rows("SELECT event_type FROM quota_events ORDER BY id")]
        self.assertEqual(quota_events, ["manual_quota_hit", "manual_reset", "manual_quota_hit"])

    def test_unmapped_manual_quota_alias_returns_friendly_error(self) -> None:
        isolated_home = self.temp_path / "unmapped-command-home"
        isolated_home.mkdir()
        environment = os.environ.copy()
        environment["HOME"] = str(isolated_home)

        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--db",
                str(self.db),
                "--mark-quota-hit",
                "codex-404",
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(completed.returncode, 1)
        self.assertIn("alias is not mapped", completed.stderr)
        self.assertNotIn("Traceback", completed.stderr)

    def test_start_script_uses_an_independent_port_and_fake_upstream(self) -> None:
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        sidecar_port = probe.getsockname()[1]
        probe.close()
        external_db = self.temp_path / "external.sqlite"
        environment = os.environ.copy()
        environment.pop("CLIPROXY_MANAGEMENT_KEY", None)
        environment.pop("CLIPROXY_MANAGEMENT_KEY_FILE", None)
        environment.update(
            {
                "PORT": str(sidecar_port),
                "UPSTREAM": f"http://127.0.0.1:{self.fake.server_address[1]}",
                "CLIPROXY_USAGE_DB": str(external_db),
                "CLIPROXY_USAGE_ACCOUNT_SCAN": "0",
            }
        )
        process = subprocess.Popen(
            [str(ROOT / "scripts" / "start_cliproxy_usage_meter.sh")],
            cwd=ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 5
            health_body = None
            while time.monotonic() < deadline:
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", sidecar_port, timeout=0.5)
                    connection.request("GET", "/healthz")
                    response = connection.getresponse()
                    health_body = response.read()
                    connection.close()
                    if response.status == 200:
                        break
                except OSError:
                    time.sleep(0.05)
            self.assertIsNotNone(health_body, "start script did not expose /healthz")
            health = json.loads(health_body)
            self.assertTrue(health["ok"])
            self.assertFalse(health["usage_queue"]["enabled"])
            self.assertFalse(health["subscription_quota"]["enabled"])
            self.assertTrue(external_db.exists())
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
