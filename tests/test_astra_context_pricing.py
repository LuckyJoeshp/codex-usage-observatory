from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import cliproxy_usage_meter as meter


class AstraContextPricingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repo = meter.UsageRepository(Path(self.temp.name) / "usage.sqlite")
        with self.repo.connect() as conn:
            conn.execute(
                """INSERT INTO model_prices (
                     model_pattern, input_per_million, cached_input_per_million,
                     cache_write_per_million, output_per_million,
                     long_context_threshold_tokens, long_input_per_million,
                     long_cached_input_per_million, long_cache_write_per_million,
                     long_output_per_million, currency, source_kind
                   ) VALUES ('gpt-6-astra', 10, 1, 12.5, 50,
                             272000, 20, 2, 25, 75, 'USD', 'official')"""
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert_snapshot(
        self,
        source: str,
        costs: tuple[float | None, float | None, float | None, float],
        model: str = "gpt-6-astra",
    ) -> None:
        with self.repo.connect() as conn:
            conn.execute(
                """INSERT INTO usage_events (
                     ts, model, source, input_tokens, cached_tokens,
                     cache_write_tokens, output_tokens, total_tokens,
                     non_cached_input_cost_usd, cached_input_cost_usd,
                     output_cost_usd, estimated_api_cost_usd,
                     long_context_pricing_applied, call_count
                   ) VALUES ('2026-09-01T00:00:00Z', ?, ?, 300000, 200000,
                             1000, 10000, 310000, ?, ?, ?, ?, 1, 1)""",
                (model, source, *costs),
            )

    def snapshots(self) -> list[tuple]:
        with self.repo.connect() as conn:
            return [
                tuple(row)
                for row in conn.execute(
                    """SELECT non_cached_input_cost_usd, cached_input_cost_usd,
                              output_cost_usd, estimated_api_cost_usd,
                              long_context_pricing_applied
                         FROM usage_events ORDER BY id"""
                )
            ]

    def test_stale_astra_price_cannot_enable_a_surcharge(self) -> None:
        components = self.repo.price_components_for(
            "gpt-6-astra",
            meter.NormalizedUsage(
                input_tokens=300000, cached_tokens=200000,
                cache_write_tokens=1000, output_tokens=10000,
            ),
        )
        self.assertIsNotNone(components)
        self.assertAlmostEqual(components.total_cost_usd, 1.7025)
        self.assertFalse(components.long_context_pricing_applied)

    def test_grouped_astra_table_cannot_restore_inferred_long_rates(self) -> None:
        document = """
        <astro-island component-export="GroupedPricingTable"
          props="{&quot;headings&quot;:[1,[[0,&quot;Short context input&quot;],[0,&quot;Long context input&quot;]]],&quot;groups&quot;:[1,[[0,{&quot;model&quot;:[0,&quot;gpt-6-astra&quot;],&quot;rows&quot;:[1,[[1,[[0,10],[0,1],[0,12.5],[0,50],[0,20],[0,2],[0,25],[0,75]]]]]}]]}"></astro-island>
        """
        price = meter.parse_official_pricing_html(document)[0]
        self.assertEqual(price["model_pattern"], "gpt-6-astra")
        self.assertEqual(price["cache_write_per_million"], 12.5)
        self.assertIsNone(price["long_context_threshold_tokens"])
        self.assertIsNone(price["long_output_per_million"])

    def test_startup_repairs_only_verified_snapshots_and_is_idempotent(self) -> None:
        for source in ("codex_app_local", "sub2api", "cockpit_tools"):
            self.insert_snapshot(source, (2.005, 0.4, 0.75, 3.155))
        self.insert_snapshot("cockpit_tools", (1.0025, 0.2, 0.5, 1.7025))
        self.insert_snapshot("sub2api", (33, 33, 33, 99))
        self.insert_snapshot("sub2api", (None, None, None, 3.155))
        self.insert_snapshot("sidecar", (2.005, 0.4, 0.75, 3.155), "gpt-5.6-sol")
        original = self.snapshots()

        self.repo.initialize()
        repaired = self.snapshots()
        for row in repaired[:4]:
            self.assertEqual(row[4], 0)
            for actual, expected in zip(row[:4], (1.0025, 0.2, 0.5, 1.7025)):
                self.assertAlmostEqual(actual, expected)
        self.assertEqual(repaired[4:], original[4:])
        price = self.repo.list_prices()[0]
        self.assertIsNone(price["long_context_threshold_tokens"])
        self.assertIsNone(price["long_input_per_million"])
        self.repo.initialize()
        self.assertEqual(self.snapshots(), repaired)
        self.assertEqual(self.repo.upgrade_long_context_costs(), 0)

    def test_sync_corrects_using_previous_rates_before_replacing_prices(self) -> None:
        self.insert_snapshot("codex_app_local", (2.005, 0.4, 0.75, 3.155))
        rows = meter._complete_context_tiers([{
            "model_pattern": "gpt-6-astra", "input_per_million": 12,
            "cached_input_per_million": 1.2, "output_per_million": 60,
        }])
        kwargs = {
            "source_url": meter.OFFICIAL_PRICING_URL,
            "fetched_at": "2026-09-02T00:00:00Z",
            "content_sha256": "0" * 64,
            "parser_version": meter.OFFICIAL_PRICE_PARSER_VERSION,
        }
        self.assertEqual(self.repo.replace_official_prices(rows, **kwargs), 1)
        self.assertAlmostEqual(self.snapshots()[0][3], 1.7025)
        self.assertEqual(self.snapshots()[0][4], 0)
        self.assertEqual(self.repo.replace_official_prices(rows, **kwargs), 0)

    def test_legacy_upgrade_never_marks_astra_as_long_context(self) -> None:
        self.insert_snapshot("sidecar", (1.0025, 0.2, 0.5, 1.7025))
        with self.repo.connect() as conn:
            conn.execute("UPDATE model_prices SET source_kind='manual'")
            conn.execute("UPDATE usage_events SET long_context_pricing_applied=0")
        self.assertEqual(self.repo.upgrade_long_context_costs(), 0)
        self.assertEqual(self.snapshots()[0][4], 0)


if __name__ == "__main__":
    unittest.main()
