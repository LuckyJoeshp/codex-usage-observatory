from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import cliproxy_usage_meter as meter


class CodexAppLargeSessionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / "codex"
        self.sessions = self.home / "sessions"
        self.sessions.mkdir(parents=True)
        self.write_auth("alpha")
        self.repo = meter.UsageRepository(self.root / "usage.sqlite")
        self.repo.set_price("fixture-model", 1.0, 2.0, 0.5, "fixture")
        self.resolver = meter.AccountResolver(
            home=self.root, refresh_seconds=0, cockpit_tools_enabled=False
        )
        self.importer = meter.CodexAppLocalImporter(
            self.repo, self.resolver, self.home
        )

    def write_auth(self, member: str) -> None:
        (self.home / "auth.json").write_text(json.dumps({
            "auth_mode": "chatgpt",
            "tokens": {
                "account_id": "fixture-workspace",
                "id_token": {"email": f"{member}@example.test"},
            },
        }))

    def session(self, name: str = "rollout-fixture.jsonl") -> Path:
        path = self.sessions / name
        self.append(path, {
            "type": "session_meta", "payload": {"model_provider": "openai"}
        })
        self.append(path, {
            "type": "turn_context",
            "payload": {"model": "fixture-model", "service_tier": "default"},
        })
        return path

    @staticmethod
    def token(ordinal: int) -> dict:
        return {
            "type": "event_msg",
            "ordinal": ordinal,
            "timestamp": f"2026-10-06T07:00:{ordinal:02d}Z",
            "payload": {
                "type": "token_count",
                "info": {"last_token_usage": {
                    "input_tokens": 100, "cached_input_tokens": 20,
                    "output_tokens": 50, "total_tokens": 150,
                }},
            },
        }

    @staticmethod
    def append(path: Path, record: dict) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def rows(self) -> list[dict]:
        with self.repo.connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM usage_events WHERE source='codex_app_local' ORDER BY ts"
            )]

    def test_growing_past_128_mib_resumes_usage_without_replaying_history(self) -> None:
        path = self.session()
        self.assertEqual(self.importer.import_once()["baselined_files"], 1)
        self.append(path, self.token(1))
        self.assertEqual(self.importer.import_once()["imported"], 1)

        # A long tool/image response must not make the whole session disappear.
        with path.open("ab") as handle:
            handle.write(b'{"type":"response_item","payload":{"output":"')
            chunk = b"x" * (1024 * 1024)
            for _ in range(129):
                handle.write(chunk)
            handle.write(b'"}}\n')
        self.append(path, self.token(2))

        result = self.importer.import_once()
        self.assertEqual(result["scanned_files"], 1)
        self.assertEqual(result["baselined_files"], 0)
        self.assertEqual(result["imported"], 1)
        self.assertEqual(self.repo.local_import_file_state(path)["offset"], path.stat().st_size)
        restarted = meter.CodexAppLocalImporter(self.repo, self.resolver, self.home)
        self.assertEqual(restarted.import_once()["imported"], 0)
        self.append(path, self.token(3))
        self.assertEqual(restarted.import_once()["imported"], 1)
        rows = self.rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(sum(row["total_tokens"] for row in rows), 450)
        self.assertAlmostEqual(sum(row["estimated_api_cost_usd"] for row in rows), 0.00057)
        self.assertEqual(len({row["identity_key"] for row in rows}), 1)
        self.assertEqual(self.repo.summary("all")["calls"], 3)

    def test_oversized_records_and_partial_utf8_tail_do_not_lose_next_usage(self) -> None:
        path = self.session()
        self.importer.import_once()
        original_loads = json.loads

        def bounded_loads(raw, *args, **kwargs):
            self.assertLessEqual(len(raw), 4096)
            return original_loads(raw, *args, **kwargs)

        self.append(path, {
            "type": "response_item", "payload": {"output": "private-fixture" * 10000}
        })
        prefix_end = path.stat().st_size
        record = self.token(1)
        record["ignored"] = "中文"
        raw = json.dumps(record, ensure_ascii=False).encode()
        split = raw.index("中文".encode()) + 1
        with path.open("ab") as handle:
            handle.write(raw[:split])
        with mock.patch.object(meter, "MAX_CODEX_APP_RECORD_BYTES", 4096), mock.patch.object(
            meter.json, "loads", side_effect=bounded_loads
        ):
            self.assertEqual(self.importer.import_once()["imported"], 0)
            self.assertEqual(self.repo.local_import_file_state(path)["offset"], prefix_end)
            with path.open("ab") as handle:
                handle.write(raw[split:])
            self.assertEqual(self.importer.import_once()["imported"], 0)
            with path.open("ab") as handle:
                handle.write(b"\n")
            self.assertEqual(self.importer.import_once()["imported"], 1)
            self.assertEqual(self.importer.import_once()["imported"], 0)
        self.assertNotIn("private-fixture", json.dumps(self.rows()))

    def test_returning_file_cannot_import_across_a_missed_account_boundary(self) -> None:
        older = self.session("older.jsonl")
        active = self.session("active.jsonl")
        self.importer.import_once()
        self.append(older, self.token(1))
        # Simulate a file omitted during a switch, as the old size cap did.
        os.utime(older, (1, 1))
        self.write_auth("beta")
        self.importer.max_files = 1
        switched = self.importer.import_once()
        self.assertEqual(switched["baselined_files"], 1)
        self.importer.max_files = 500
        returned = self.importer.import_once()
        self.assertEqual(returned["baselined_files"], 1)
        self.assertEqual(returned["imported"], 0)
        self.append(older, self.token(2))
        self.append(active, self.token(3))
        self.assertEqual(self.importer.import_once()["imported"], 2)
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(len({row["identity_key"] for row in self.rows()}), 1)

    def test_account_baseline_waits_for_a_complete_record(self) -> None:
        path = self.session()
        raw = json.dumps(self.token(1)).encode()
        with path.open("ab") as handle:
            handle.write(raw[:50])
        first = self.importer.import_once()
        self.assertEqual(first["imported"], 0)
        self.assertEqual(first["failed_homes"], 1)
        with path.open("ab") as handle:
            handle.write(raw[50:] + b"\n")
        self.assertEqual(self.importer.import_once()["imported"], 0)
        self.append(path, self.token(2))
        self.assertEqual(self.importer.import_once()["imported"], 1)


if __name__ == "__main__":
    unittest.main()
