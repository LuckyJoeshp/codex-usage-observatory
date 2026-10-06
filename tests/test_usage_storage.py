from __future__ import annotations

import gzip
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from scripts import cliproxy_usage_meter as meter


NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
ALPHA = "subscription:" + "a" * 32
BETA = "subscription:" + "b" * 32


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.astimezone().replace(tzinfo=None)


class UsageStorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = meter.UsageRepository(self.root / "usage.sqlite")
        self.backup = self.root / "usage.backup.sqlite.gz"

    def snapshot(self, **overrides) -> dict:
        return {
            "identity_key": ALPHA,
            "window_kind": "weekly",
            "window_seconds": 604800,
            "reset_at": "2026-10-12T00:00:00Z",
            "fetched_at": "2026-10-06T11:59:00Z",
            "used_percent": 0,
            "remaining_percent": 100,
            "source": "fixture",
            **overrides,
        }

    def seed_snapshot(self, **overrides) -> None:
        values = self.snapshot(**overrides)
        with self.repo.connect() as conn:
            conn.execute(
                "INSERT INTO subscription_quota_snapshots ("
                + ",".join(values) + ") VALUES ("
                + ",".join("?" for _ in values) + ")",
                tuple(values.values()),
            )

    def snapshots(self) -> list[dict]:
        with self.repo.connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM subscription_quota_snapshots ORDER BY id"
            )]

    def test_new_polls_keep_latest_status_and_each_reset_despite_backfill_order(self):
        for key in (ALPHA, BETA):
            for minute in range(20):
                self.repo.insert_subscription_quota_snapshot(self.snapshot(
                    identity_key=key, window_kind="account_status", reset_at=None,
                    fetched_at=NOW - timedelta(minutes=20 - minute),
                    estimate_method="sub2api_active", provider_allowed=True,
                ))
                self.repo.insert_subscription_quota_snapshot(self.snapshot(
                    identity_key=key,
                    fetched_at=NOW - timedelta(minutes=20 - minute),
                    used_percent=minute, remaining_percent=100 - minute,
                ))
            self.repo.insert_subscription_quota_snapshot(self.snapshot(
                identity_key=key, fetched_at="2026-10-06T11:30:00Z",
                used_percent=5, remaining_percent=95,
            ))
            self.repo.insert_subscription_quota_snapshot(self.snapshot(
                identity_key=key, reset_at="2026-10-05T00:00:00Z",
                fetched_at="2026-10-04T12:00:00Z", used_percent=80,
            ))
        self.assertEqual(len(self.snapshots()), 6)
        latest = self.repo.latest_subscription_quotas()
        self.assertEqual(len(latest), 4)
        self.assertEqual(
            [row["used_percent"] for row in latest if row["window_kind"] == "weekly"],
            [19, 19],
        )

    def test_compaction_preserves_rendered_dashboard_usage_timeline_and_backup(self):
        self.repo.insert_subscription_quota_snapshot(self.snapshot(
            reset_at="2026-10-05T00:00:00Z", fetched_at="2026-10-04T12:00:00Z",
            used_percent=100, remaining_percent=0,
        ))
        # The estimator uses the last observation of a reset, even if an
        # earlier observation had a higher percentage.
        self.seed_snapshot(
            reset_at="2026-10-05T00:00:00Z", fetched_at="2026-10-04T13:00:00Z",
            used_percent=80, remaining_percent=20,
        )
        self.seed_snapshot()
        with self.repo.connect() as conn:
            for key in (ALPHA, BETA):
                conn.executemany(
                    """INSERT INTO subscription_quota_snapshots (
                        identity_key, window_kind, fetched_at, source,
                        plan_type, estimate_method, provider_allowed)
                       VALUES (?, 'account_status', ?, 'fixture', 'pro',
                               'sub2api_active', 1)""",
                    [(key, (NOW - timedelta(seconds=1000 - i)).isoformat())
                     for i in range(1000)],
                )
                for stamp in ("2026-10-03T12:00:00.000000Z", "2026-10-06T11:58:00.000000Z"):
                    event_id = conn.execute(
                        """INSERT INTO usage_events (
                            ts, identity_key, model, input_tokens, cached_tokens,
                            output_tokens, total_tokens, estimated_api_cost_usd,
                            status_code, ok, usage_missing, source)
                           VALUES (?, ?, 'fixture-model', 100, 20, 50, 150,
                                   20, 200, 1, 0, 'codex_app_local')""",
                        (stamp, key),
                    ).lastrowid
                    import_key = f"fixture:{event_id}"
                    conn.execute(
                        "INSERT INTO local_import_records VALUES (?, 'codex_app_local', ?, ?)",
                        (import_key, event_id, stamp),
                    )
                    conn.execute(
                        "INSERT INTO api_response_observations VALUES (?, ?, 200, 1, 'codex_app_local')",
                        (f"import:{import_key}", stamp[:16] + ":00Z"),
                    )
        self.repo.save_local_import_file_state({
            "path": self.root / "session.jsonl", "size": 999, "offset": 999,
            "model_provider": "openai", "model": "fixture-model",
        })
        protected = (
            "usage_events", "anonymous_usage_daily", "api_response_observations",
            "local_import_records", "local_import_files", "local_import_bindings",
            "quota_events", "account_quota_cycles", "active_subscription_registry",
        )

        def protected_rows():
            with self.repo.connect() as conn:
                return {name: [tuple(row) for row in conn.execute(f"SELECT * FROM {name}")]
                        for name in protected}

        before = protected_rows()
        original_snapshots = len(self.snapshots())
        with mock.patch.object(meter, "datetime", FrozenDatetime):
            page = meter.dashboard_html(self.repo)
            cards = self.repo.subscription_dashboard_rows()
            self.assertEqual(cards[0]["current_window_full_quota_usd"], 25)
            timeline = self.repo.response_timeline(now=NOW)
            result = self.repo.compact_storage()
            self.assertEqual(meter.dashboard_html(self.repo), page)
            self.assertEqual(self.repo.subscription_dashboard_rows(), cards)
            self.assertEqual(self.repo.response_timeline(now=NOW), timeline)
        self.assertEqual(protected_rows(), before)
        self.assertEqual(result["quota_snapshots_after"], 4)
        self.assertEqual(result["quota_snapshots_before"], original_snapshots)
        self.assertLess(result["database_bytes_after"], result["database_bytes_before"])
        self.assertEqual(self.backup.stat().st_mode & 0o077, 0)
        restored = self.root / "restored.sqlite"
        with gzip.open(self.backup, "rb") as archive:
            restored.write_bytes(archive.read())
        with sqlite3.connect(restored) as conn:
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM subscription_quota_snapshots"
            ).fetchone()[0], original_snapshots)
        self.assertEqual(self.repo.compact_storage()["quota_snapshots_removed"], 0)
        self.assertEqual(list(self.root.glob("*.sqlite.gz")), [self.backup])

    def test_sparse_metadata_anchors_survive_compaction_for_future_polls(self):
        self.seed_snapshot(fetched_at="2026-10-06T11:50:00Z", plan_type="team")
        self.seed_snapshot(fetched_at="2026-10-06T11:51:00Z", subscription_active_until="2026-11-01T00:00:00Z")
        self.seed_snapshot(fetched_at="2026-10-06T11:52:00Z")
        self.seed_snapshot(fetched_at="2026-10-06T11:53:00Z")
        self.assertEqual(self.repo.compact_storage()["quota_snapshots_after"], 3)
        self.repo.insert_subscription_quota_snapshot(self.snapshot())
        rows = self.snapshots()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["plan_type"], "team")
        self.assertEqual(rows[0]["subscription_active_until"], "2026-11-01T00:00:00Z")

    def test_failed_backup_leaves_history_and_previous_backup_intact(self):
        self.seed_snapshot()
        self.backup.write_bytes(b"previous-backup-fixture")
        before = self.snapshots()
        with mock.patch.object(meter.gzip, "GzipFile", side_effect=OSError("fixture")):
            with self.assertRaises(OSError):
                self.repo.compact_storage()
        self.assertEqual(self.snapshots(), before)
        self.assertEqual(self.backup.read_bytes(), b"previous-backup-fixture")
        self.assertEqual(list(self.root.glob(".usage-backup-*")), [])


if __name__ == "__main__":
    unittest.main()
