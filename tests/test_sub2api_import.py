from __future__ import annotations

import json
import re
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from scripts import cliproxy_usage_meter as meter


ADMIN_KEY = "admin-fixture-key-never-persist"
ACCOUNT_SELECTOR = 910_223_344_556_677
SECOND_ACCOUNT_SELECTOR = 910_223_344_556_688


class TrackingUsageRepository(meter.UsageRepository):
    def __init__(self, path: Path) -> None:
        self.traced_statements: list[str] | None = None
        self.watch_usage_updates = False
        self.usage_update_started = threading.Event()
        self.update_delay_seconds = 0.0
        super().__init__(path)

    def connect(self) -> sqlite3.Connection:
        connection = super().connect()
        if self.update_delay_seconds:
            connection.create_function(
                "fixture_update_delay",
                0,
                lambda: time.sleep(self.update_delay_seconds),
            )
        if self.traced_statements is not None or self.watch_usage_updates:
            def trace(statement: str) -> None:
                normalized = " ".join(statement.split())
                if self.traced_statements is not None:
                    self.traced_statements.append(normalized)
                if self.watch_usage_updates and normalized.upper().startswith(
                    "UPDATE USAGE_EVENTS SET"
                ):
                    self.usage_update_started.set()

            connection.set_trace_callback(trace)
        return connection

    def start_trace(self) -> None:
        self.traced_statements = []

    def stop_trace(self) -> list[str]:
        statements = self.traced_statements or []
        self.traced_statements = None
        return statements


def table_dml(statements: list[str], table: str) -> list[str]:
    pattern = re.compile(
        rf"^(?:INSERT(?: OR \w+)? INTO|UPDATE|DELETE FROM) {re.escape(table)}\b",
        re.IGNORECASE,
    )
    return [statement for statement in statements if pattern.match(statement)]


class FakeSub2APIServer(ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), FakeSub2APIHandler)
        self.admin_key = ADMIN_KEY
        self.force_status = 200
        self.requests: list[dict[str, object]] = []
        self.accounts: list[dict[str, object]] = []
        self.usage: list[dict[str, object]] = []


class FakeSub2APIHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def fake_server(self) -> FakeSub2APIServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        key_ok = self.headers.get("X-API-Key") == self.fake_server.admin_key
        admin_ui = self.headers.get("X-Admin-UI-Request") == "1"
        self.fake_server.requests.append(
            {
                "path": parsed.path,
                "query": query,
                "key_ok": key_ok,
                "admin_ui": admin_ui,
            }
        )
        if self.fake_server.force_status != 200:
            self._json(self.fake_server.force_status, {"code": self.fake_server.force_status})
            return
        if not key_ok or not admin_ui:
            self._json(401, {"code": 401, "message": "unauthorized"})
            return
        if parsed.path == "/api/v1/admin/accounts":
            records = self.fake_server.accounts
        elif parsed.path == "/api/v1/admin/usage":
            records = self.fake_server.usage
        else:
            self._json(404, {"code": 404, "message": "not found"})
            return

        page = int(query.get("page", ["1"])[0])
        page_size = int(query.get("page_size", ["20"])[0])
        ordered = sorted(records, key=lambda row: int(row["id"]))
        start = (page - 1) * page_size
        items = ordered[start : start + page_size]
        pages = max(1, (len(ordered) + page_size - 1) // page_size)
        self._json(
            200,
            {
                "code": 0,
                "message": "success",
                "data": {
                    "items": items,
                    "total": len(ordered),
                    "page": page,
                    "page_size": page_size,
                    "pages": pages,
                },
            },
        )

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def account_fixture(
    account_id: int,
    email: str,
    *,
    status: str = "active",
    schedulable: bool = True,
) -> dict[str, object]:
    return {
        "id": account_id,
        "name": email,
        "platform": "openai",
        "type": "oauth",
        "status": status,
        "schedulable": schedulable,
        "created_at": "2026-08-18T01:00:00Z",
        "updated_at": "2026-08-18T02:00:00Z",
        "expires_at": 1_830_297_600,
        "credentials": {
            "access_token": "fixture-access-token-never-persist",
            "refresh_token": "fixture-refresh-token-never-persist",
        },
        "extra": {
            "email": email,
            "plan_type": "pro",
            "codex_usage_updated_at": "2026-08-19T03:00:00Z",
            "codex_5h_used_percent": 25.0,
            "codex_5h_window_minutes": 300,
            "codex_5h_reset_at": "2026-08-19T07:00:00Z",
            "codex_7d_used_percent": 40.0,
            "codex_7d_window_minutes": 10_080,
            "codex_7d_reset_at": "2026-08-24T00:00:00Z",
            "private_note": "fixture-private-note-never-persist",
        },
    }


def usage_fixture(
    usage_id: int,
    account_id: int,
    *,
    created_at: str = "2026-08-19T03:15:00Z",
) -> dict[str, object]:
    return {
        "id": usage_id,
        "account_id": account_id,
        "request_id": f"sub2api-request-{usage_id}-never-persist",
        "model": "gpt-5.2-codex",
        "input_tokens": 700,
        "cache_creation_tokens": 100,
        "cache_read_tokens": 200,
        "output_tokens": 100,
        "input_cost": 0.0014,
        "cache_creation_cost": 0.0002,
        "cache_read_cost": 0.00004,
        "output_cost": 0.001,
        "image_input_cost": 0.0,
        "image_output_cost": 0.0,
        "total_cost": 0.00264,
        "long_context_billing_applied": False,
        "request_type": "stream",
        "stream": True,
        "duration_ms": 345,
        "created_at": created_at,
        "user": {"email": "consumer@example.test"},
    }


class Sub2APIImporterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "meter.sqlite"
        self.key_file = self.root / "sub2api-admin.key"
        self.key_file.write_text(ADMIN_KEY + "\n", encoding="utf-8")
        self.key_file.chmod(0o600)
        self.fake = FakeSub2APIServer()
        self.fake.accounts = [
            account_fixture(ACCOUNT_SELECTOR, "alpha@example.test"),
            account_fixture(
                SECOND_ACCOUNT_SELECTOR,
                "new-account@example.test",
                status="error",
                schedulable=False,
            ),
        ]
        self.fake.usage = [usage_fixture(70_001, ACCOUNT_SELECTOR)]
        self.fake_thread = threading.Thread(target=self.fake.serve_forever, daemon=True)
        self.fake_thread.start()
        self.repo = TrackingUsageRepository(self.db)
        self.resolver = meter.AccountResolver(
            home=self.root / "home",
            enabled=False,
            identity_key_file=self.root / "identity.key",
        )
        self.importer = meter.Sub2APIImporter(
            self.repo,
            self.resolver,
            base_url=f"http://127.0.0.1:{self.fake.server_address[1]}",
            key_file=self.key_file,
            poll_seconds=1,
            timeout=2,
            page_size=1,
            backfill_days=30,
        )

    def importer_with_page_size(self, page_size: int) -> meter.Sub2APIImporter:
        return meter.Sub2APIImporter(
            self.repo,
            self.resolver,
            base_url=f"http://127.0.0.1:{self.fake.server_address[1]}",
            key_file=self.key_file,
            poll_seconds=1,
            timeout=5,
            page_size=page_size,
            backfill_days=30,
        )

    def tearDown(self) -> None:
        self.importer.stop()
        self.fake.shutdown()
        self.fake.server_close()
        self.fake_thread.join(timeout=2)
        self.temp.cleanup()

    def test_imports_paginated_usage_quota_and_accounts_idempotently(self) -> None:
        first = self.importer.import_once()
        self.assertEqual(
            first,
            {
                "imported": 1,
                "scanned": 1,
                "accounts": 2,
                "quota_rows": 6,
                "new": 1,
                "changed": 0,
                "unchanged": 0,
                "retired": 0,
                "source_conflict": 0,
                "write_transactions": 1,
                "usage_updates": 0,
                "observation_updates": 1,
            },
        )
        second = self.importer.import_once()
        self.assertEqual(second["imported"], 0)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(second["write_transactions"], 0)
        self.assertEqual(second["usage_updates"], 0)
        self.assertEqual(second["observation_updates"], 0)

        with sqlite3.connect(self.db) as connection:
            connection.row_factory = sqlite3.Row
            event = connection.execute(
                "SELECT * FROM usage_events WHERE source='sub2api'"
            ).fetchone()
            self.assertIsNotNone(event)
            assert event is not None
            self.assertEqual(event["input_tokens"], 1000)
            self.assertEqual(event["cached_tokens"], 200)
            self.assertEqual(event["cache_write_tokens"], 100)
            self.assertEqual(event["output_tokens"], 100)
            self.assertAlmostEqual(event["non_cached_input_cost_usd"], 0.0016)
            self.assertAlmostEqual(event["cached_input_cost_usd"], 0.00004)
            self.assertAlmostEqual(event["output_cost_usd"], 0.001)
            self.assertAlmostEqual(event["estimated_api_cost_usd"], 0.00264)
            self.assertIsNone(event["request_id"])
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM usage_events WHERE source='sub2api'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subscription_quota_snapshots "
                    "WHERE source='sub2api_account'"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM subscription_quota_snapshots "
                    "WHERE source='sub2api_quota'"
                ).fetchone()[0],
                4,
            )

        rows = self.repo.subscription_dashboard_rows()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["identity_key"].startswith("subscription:") for row in rows))
        self.assertEqual(
            {frozenset(row["windows"]) for row in rows},
            {frozenset({"account_status", "five_hour", "weekly"})},
        )
        requests = self.fake.requests
        self.assertTrue(all(request["key_ok"] for request in requests))
        self.assertTrue(all(request["admin_ui"] for request in requests))
        usage_queries = [
            request["query"]
            for request in requests
            if request["path"] == "/api/v1/admin/usage"
        ]
        self.assertTrue(usage_queries)
        self.assertTrue(all(query.get("exact_total") == ["true"] for query in usage_queries))

    def test_new_usage_and_account_state_changes_are_incremental(self) -> None:
        self.importer.import_once()
        self.fake.usage.append(
            usage_fixture(
                70_002,
                SECOND_ACCOUNT_SELECTOR,
                created_at="2026-08-19T03:20:00Z",
            )
        )
        self.fake.accounts[0]["status"] = "error"
        self.fake.accounts[0]["schedulable"] = False
        result = self.importer.import_once()
        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["scanned"], 2)

        with sqlite3.connect(self.db) as connection:
            connection.row_factory = sqlite3.Row
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM usage_events WHERE source='sub2api'"
                ).fetchone()[0],
                2,
            )
            state = connection.execute(
                """SELECT estimate_method, provider_allowed
                     FROM subscription_quota_snapshots
                    WHERE source='sub2api_account'
                      AND identity_key=?
                    ORDER BY fetched_at DESC, id DESC LIMIT 1""",
                (
                    f"subscription:{self.resolver.resolve_sub2api_account(ACCOUNT_SELECTOR).subscription_id_hash}",
                ),
            ).fetchone()
            self.assertEqual(state["estimate_method"], "sub2api_error")
            self.assertEqual(state["provider_allowed"], 0)

    def test_identical_14k_overlap_has_no_usage_or_observation_dml(self) -> None:
        count = 14_000
        self.fake.usage = [
            usage_fixture(100_000 + index, ACCOUNT_SELECTOR)
            for index in range(count)
        ]
        importer = self.importer_with_page_size(1000)

        first = importer.import_once()
        self.assertEqual(first["new"], count)
        self.assertEqual(first["write_transactions"], 28)
        self.assertEqual(first["usage_updates"], 0)
        self.assertEqual(first["observation_updates"], count)

        self.repo.start_trace()
        second = importer.import_once()
        statements = self.repo.stop_trace()

        self.assertEqual(second["scanned"], count)
        self.assertEqual(second["new"], 0)
        self.assertEqual(second["changed"], 0)
        self.assertEqual(second["unchanged"], count)
        self.assertEqual(second["retired"], 0)
        self.assertEqual(second["source_conflict"], 0)
        self.assertEqual(second["write_transactions"], 0)
        self.assertEqual(second["usage_updates"], 0)
        self.assertEqual(second["observation_updates"], 0)
        self.assertEqual(table_dml(statements, "usage_events"), [])
        self.assertEqual(table_dml(statements, "api_response_observations"), [])

    def test_price_revision_updates_only_the_changed_usage_row(self) -> None:
        self.fake.usage.append(usage_fixture(70_002, ACCOUNT_SELECTOR))
        self.importer.import_once()
        self.fake.usage[0]["output_cost"] = 0.002
        self.fake.usage[0]["total_cost"] = 0.00364

        result = self.importer.import_once()

        self.assertEqual(result["new"], 0)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["usage_updates"], 1)
        self.assertEqual(result["observation_updates"], 0)
        with sqlite3.connect(self.db) as connection:
            costs = sorted(
                row[0]
                for row in connection.execute(
                    """SELECT estimated_api_cost_usd FROM usage_events
                         WHERE source='sub2api'"""
                )
            )
        self.assertEqual(costs, [0.00264, 0.00364])
        self.assertAlmostEqual(
            self.repo.summary("all")["estimated_api_cost_usd"],
            0.00628,
        )

    def test_observation_drift_updates_only_the_observation(self) -> None:
        self.fake.usage.append(usage_fixture(70_002, ACCOUNT_SELECTOR))
        self.importer.import_once()
        converted = self.importer._event_from_record(self.fake.usage[0])
        self.assertIsNotNone(converted)
        assert converted is not None
        with self.repo.connect() as connection:
            connection.execute(
                """UPDATE api_response_observations SET status_code=503
                    WHERE observation_key=?""",
                (f"import:{converted[1]}",),
            )

        result = self.importer.import_once()

        self.assertEqual(result["changed"], 1)
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["usage_updates"], 0)
        self.assertEqual(result["observation_updates"], 1)
        with sqlite3.connect(self.db) as connection:
            statuses = {
                row[0]
                for row in connection.execute(
                    "SELECT status_code FROM api_response_observations WHERE source=?",
                    (meter.SUB2API_REQUEST_SOURCE,),
                )
            }
        self.assertEqual(statuses, {200})

    def test_source_conflict_fails_closed_without_observation(self) -> None:
        self.resolver.register_sub2api_accounts(
            self.importer.instance_scope,
            self.fake.accounts,
        )
        converted = self.importer._event_from_record(self.fake.usage[0])
        self.assertIsNotNone(converted)
        assert converted is not None
        event, import_key = converted
        self.assertTrue(
            self.repo.record_imported_event(event, import_key, "fixture_conflict")
        )

        result = self.importer.import_once()

        self.assertEqual(result["new"], 0)
        self.assertEqual(result["changed"], 0)
        self.assertEqual(result["source_conflict"], 1)
        self.assertEqual(result["write_transactions"], 0)
        with sqlite3.connect(self.db) as connection:
            source = connection.execute(
                "SELECT source FROM local_import_records WHERE import_key=?",
                (import_key,),
            ).fetchone()[0]
            observation = connection.execute(
                "SELECT 1 FROM api_response_observations WHERE observation_key=?",
                (f"import:{import_key}",),
            ).fetchone()
        self.assertEqual(source, "fixture_conflict")
        self.assertIsNone(observation)

    def test_privacy_retired_record_is_not_recreated(self) -> None:
        self.importer.import_once()
        with self.repo.connect() as connection:
            imported = connection.execute(
                """SELECT import_key, usage_event_id FROM local_import_records
                     WHERE source=?""",
                (meter.SUB2API_REQUEST_SOURCE,),
            ).fetchone()
            connection.execute(
                "UPDATE local_import_records SET usage_event_id=NULL WHERE import_key=?",
                (imported["import_key"],),
            )
            connection.execute(
                "DELETE FROM usage_events WHERE id=?",
                (imported["usage_event_id"],),
            )
            connection.execute(
                "DELETE FROM api_response_observations WHERE observation_key=?",
                (f"import:{imported['import_key']}",),
            )

        self.repo.start_trace()
        result = self.importer.import_once()
        statements = self.repo.stop_trace()

        self.assertEqual(result["retired"], 1)
        self.assertEqual(result["new"], 0)
        self.assertEqual(result["changed"], 0)
        self.assertEqual(result["write_transactions"], 0)
        self.assertEqual(table_dml(statements, "usage_events"), [])
        self.assertEqual(table_dml(statements, "api_response_observations"), [])
        with sqlite3.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM usage_events WHERE source=?",
                    (meter.SUB2API_REQUEST_SOURCE,),
                ).fetchone()[0],
                0,
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM api_response_observations WHERE observation_key=?",
                    (f"import:{imported['import_key']}",),
                ).fetchone()
            )

    def test_suspect_identity_does_not_downgrade_live_history(self) -> None:
        self.importer.import_once()
        with sqlite3.connect(self.db) as connection:
            original_identity = connection.execute(
                "SELECT identity_key FROM usage_events WHERE source=?",
                (meter.SUB2API_REQUEST_SOURCE,),
            ).fetchone()[0]
        self.fake.accounts = []

        result = self.importer.import_once()

        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["usage_updates"], 0)
        with sqlite3.connect(self.db) as connection:
            current_identity = connection.execute(
                "SELECT identity_key FROM usage_events WHERE source=?",
                (meter.SUB2API_REQUEST_SOURCE,),
            ).fetchone()[0]
            registry_state = connection.execute(
                "SELECT state FROM active_subscription_registry WHERE identity_key=?",
                (original_identity,),
            ).fetchone()[0]
        self.assertEqual(current_identity, original_identity)
        self.assertEqual(registry_state, "suspect_missing")

    def test_mid_batch_failure_rolls_back_every_usage_update(self) -> None:
        self.fake.usage.append(usage_fixture(70_002, ACCOUNT_SELECTOR))
        self.importer.import_once()
        self.resolver.register_sub2api_accounts(
            self.importer.instance_scope,
            self.fake.accounts,
        )
        second = self.importer._event_from_record(self.fake.usage[1])
        self.assertIsNotNone(second)
        assert second is not None
        with self.repo.connect() as connection:
            second_event_id = connection.execute(
                "SELECT usage_event_id FROM local_import_records WHERE import_key=?",
                (second[1],),
            ).fetchone()[0]
            connection.execute(
                f"""CREATE TRIGGER fixture_fail_second_usage_update
                    BEFORE UPDATE OF estimated_api_cost_usd ON usage_events
                    WHEN OLD.id={int(second_event_id)}
                    BEGIN
                      SELECT RAISE(ABORT, 'fixture batch failure');
                    END"""
            )
        for record in self.fake.usage:
            record["output_cost"] = 0.002
            record["total_cost"] = 0.00364

        with self.assertRaisesRegex(sqlite3.IntegrityError, "fixture batch failure"):
            self.importer.import_once()

        with sqlite3.connect(self.db) as connection:
            costs = [
                row[0]
                for row in connection.execute(
                    """SELECT estimated_api_cost_usd FROM usage_events
                         WHERE source=? ORDER BY id""",
                    (meter.SUB2API_REQUEST_SOURCE,),
                )
            ]
        self.assertEqual(len(costs), 2)
        self.assertTrue(all(abs(cost - 0.00264) < 1e-12 for cost in costs))

    def test_bounded_batch_does_not_long_block_realtime_event_write(self) -> None:
        count = meter.IMPORTED_EVENT_SYNC_BATCH_SIZE
        self.fake.usage = [
            usage_fixture(200_000 + index, ACCOUNT_SELECTOR)
            for index in range(count)
        ]
        importer = self.importer_with_page_size(1000)
        importer.import_once()
        for record in self.fake.usage:
            record["output_cost"] = 0.002
            record["total_cost"] = 0.00364

        self.repo.update_delay_seconds = 0.001
        with self.repo.connect() as connection:
            connection.execute(
                """CREATE TRIGGER fixture_slow_sub2api_updates
                   BEFORE UPDATE OF estimated_api_cost_usd ON usage_events
                   WHEN OLD.source='sub2api'
                   BEGIN
                     SELECT fixture_update_delay();
                   END"""
            )
        self.repo.watch_usage_updates = True
        result: dict[str, object] = {}
        errors: list[BaseException] = []

        def run_import() -> None:
            try:
                result.update(importer.import_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        import_thread = threading.Thread(target=run_import)
        import_thread.start()
        self.assertTrue(self.repo.usage_update_started.wait(timeout=3))
        realtime = importer._event_from_record(usage_fixture(999_001, ACCOUNT_SELECTOR))
        self.assertIsNotNone(realtime)
        assert realtime is not None
        realtime_event = realtime[0]
        realtime_event.source = "sidecar"
        started = time.monotonic()
        self.repo.insert_event(realtime_event)
        elapsed = time.monotonic() - started
        import_thread.join(timeout=5)

        self.assertFalse(import_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result.get("usage_updates"), count)
        self.assertLess(elapsed, 3.0)

    def test_key_file_must_be_owner_only_and_auth_failure_is_sanitized(self) -> None:
        self.key_file.chmod(0o644)
        self.assertIsNone(self.importer._load_key())
        self.key_file.chmod(0o600)
        self.assertEqual(self.importer._load_key(), ADMIN_KEY)

        self.fake.force_status = 401
        self.importer.start()
        deadline = time.monotonic() + 2
        status: dict[str, object] = {}
        while time.monotonic() < deadline:
            status = self.importer.status()
            if status.get("last_error_type"):
                break
            time.sleep(0.02)
        self.assertEqual(status.get("last_error_type"), "Sub2APIAuthenticationError")
        self.assertEqual(status.get("last_status"), 401)
        serialized = json.dumps(status, sort_keys=True)
        self.assertNotIn(ADMIN_KEY, serialized)
        self.assertNotIn(str(self.key_file), serialized)

    def test_dashboard_and_health_expose_sub2api_without_secrets(self) -> None:
        server = meter.MeterHTTPServer(
            ("127.0.0.1", 0),
            self.repo,
            "http://127.0.0.1:9",
            self.resolver,
            1,
            codex_app_import_enabled=False,
            cockpit_tools_import_enabled=False,
            sub2api_base_url=f"http://127.0.0.1:{self.fake.server_address[1]}",
            sub2api_admin_key_file=self.key_file,
            sub2api_poll_seconds=1,
            sub2api_timeout=2,
            sub2api_page_size=1,
        )
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            server.start_local_importer()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if server.sub2api_status().get("last_success_at"):
                    break
                time.sleep(0.02)
            import http.client

            connection = http.client.HTTPConnection(
                "127.0.0.1", server.server_address[1], timeout=3
            )
            connection.request("GET", "/healthz")
            response = connection.getresponse()
            health_body = response.read()
            self.assertEqual(response.status, 200)
            health = json.loads(health_body)
            self.assertIn("sub2api", health)
            self.assertTrue(health["sub2api"]["key_loaded"])
            self.assertEqual(health["sub2api"]["last_new"], 1)
            self.assertEqual(health["sub2api"]["last_changed"], 0)
            self.assertEqual(health["sub2api"]["last_usage_updates"], 0)
            self.assertEqual(health["sub2api"]["last_observation_updates"], 1)
            connection.close()

            connection = http.client.HTTPConnection(
                "127.0.0.1", server.server_address[1], timeout=3
            )
            connection.request("GET", "/usage")
            response = connection.getresponse()
            page = response.read().decode("utf-8")
            self.assertEqual(response.status, 200)
            self.assertIn("Sub2API 只读同步正常", page)
            self.assertIn("S = Sub2API account", page)
            self.assertIn("Sub2API 可调度", page)
            self.assertIn("Sub2API 账号异常", page)
            self.assertIn("时间轴固定合并 Sub2API", page)
            self.assertNotIn(ADMIN_KEY, page)
            self.assertNotIn("sub2api-request-70001-never-persist", page)
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2)

    def test_database_contains_no_remote_identity_or_credentials(self) -> None:
        self.importer.import_once()
        forbidden = (
            ADMIN_KEY,
            "alpha@example.test",
            "new-account@example.test",
            "fixture-access-token-never-persist",
            "fixture-refresh-token-never-persist",
            "fixture-private-note-never-persist",
            "sub2api-request-70001-never-persist",
            str(ACCOUNT_SELECTOR),
            str(SECOND_ACCOUNT_SELECTOR),
        )
        database_bytes = b"".join(
            path.read_bytes()
            for path in self.root.glob("meter.sqlite*")
            if path.is_file()
        )
        for value in forbidden:
            self.assertNotIn(value.encode("utf-8"), database_bytes)

    def test_rejects_non_loopback_management_origin(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            meter.Sub2APIImporter(
                self.repo,
                self.resolver,
                base_url="https://sub2api.example.test",
                key_file=self.key_file,
            )


if __name__ == "__main__":
    unittest.main()
