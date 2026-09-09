from __future__ import annotations

import json
import http.client
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from scripts import cliproxy_usage_meter as meter
from tests.test_sub2api_import import usage_fixture


def pricing_document() -> str:
    models = (
        ("gpt-6-astra", (10, 1, 12.5, 50), (20, 2, 25, 100)),
        ("gpt-5.6-sol", (4, 0.4, 5, 20), (8, 0.8, 10, 40)),
        ("gpt-5.5", (5, 0.5, None, 30), (12.5, 1.25, None, 75)),
    )
    panes = []
    for tier, index in (("fast", 2), ("standard", 1)):
        rows = "".join(
            "<tr><td>" + model[0] + "</td>"
            + "".join(f"<td>{rate if rate is not None else '-'}</td>" for rate in model[index])
            + "</tr>" for model in models
        )
        panes.append(
            f'<div data-value="{tier}" data-content-switcher-pane="true">'
            '<astro-island component-export="TextTokenPricingTables"><table>'
            '<tr><th>Model</th><th>Input</th><th>Cached input</th><th>Cache writes</th><th>Output</th></tr>'
            + rows + '</table></astro-island></div>'
        )
    return "".join(panes)


class ServiceTierPricingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.repo = meter.UsageRepository(self.root / "usage.sqlite")
        self.resolver = meter.AccountResolver(home=self.root, enabled=False, cockpit_tools_enabled=False)
        self.prices = meter.parse_official_pricing_html(pricing_document())
        self.sync_prices()
        self.usage = meter.NormalizedUsage(input_tokens=1000, cached_tokens=800, output_tokens=100)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def sync_prices(self) -> int:
        return self.repo.replace_official_prices(
            self.prices, source_url=meter.OFFICIAL_PRICING_URL,
            fetched_at=meter.utc_now(), content_sha256="0" * 64,
            parser_version=meter.OFFICIAL_PRICE_PARSER_VERSION,
        )

    def event(self, request_tier=None, response_tier=None, model="gpt-6-astra"):
        payload = {"model": model, "service_tier": request_tier}
        info = meter.request_info("/v1/responses", "POST", {}, json.dumps(payload).encode(), self.resolver)
        handler = object.__new__(meter.UsageMeterHandler)
        handler.server = SimpleNamespace(repo=self.repo)
        return handler._make_event(info, model, 200, self.usage, None, None, 0, 0, time.monotonic(), response_tier=response_tier)

    def test_official_panes_keep_rates_separate_even_when_fast_comes_first(self):
        price = self.repo.list_prices()[2]
        self.assertEqual(price["model_pattern"], "gpt-6-astra")
        self.assertEqual(price["input_per_million"], 10)
        self.assertEqual(price["fast_input_per_million"], 20)
        self.assertEqual(price["fast_cache_write_per_million"], 25)
        self.assertEqual(price["fast_output_per_million"], 100)
        self.assertAlmostEqual(self.repo.price_for("gpt-5.5", self.usage, "fast"), 0.011)

    def test_astra_fast_keeps_flat_context_policy_without_losing_fast_premium(self):
        for inputs in (1000, 272000, 272001, 1000000):
            usage = replace(self.usage, input_tokens=inputs, cache_write_tokens=100)
            standard = self.repo.price_components_for("gpt-6-astra", usage, "default")
            fast = self.repo.price_components_for("gpt-6-astra", usage, "priority")
            self.assertAlmostEqual(fast.total_cost_usd, standard.total_cost_usd * 2)
            self.assertFalse(fast.long_context_pricing_applied)

    def test_response_downgrade_wins_over_requested_fast_and_is_persisted(self):
        event = self.event("fast", "default")
        self.repo.insert_event(event)
        with self.repo.connect() as conn:
            row = dict(conn.execute("SELECT * FROM usage_events").fetchone())
        self.assertEqual(row["service_tier"], "default")
        self.assertEqual(row["requested_service_tier"], "fast")
        self.assertEqual(row["service_tier_source"], "response")
        self.assertAlmostEqual(row["estimated_api_cost_usd"], 0.0078)
        self.assertIn("Fast", meter.service_tier_label(row))

    def test_request_only_estimate_and_unknown_mode_remain_distinguishable(self):
        fast = self.event("priority")
        unknown = self.event()
        self.assertEqual(fast.service_tier, "fast")
        self.assertEqual(fast.service_tier_source, "request")
        self.assertAlmostEqual(fast.estimated_api_cost_usd, 0.0156)
        self.assertIsNone(unknown.service_tier)
        self.assertAlmostEqual(unknown.estimated_api_cost_usd, 0.0078)
        self.assertNotEqual(meter.service_tier_label(vars(fast)), meter.service_tier_label(vars(unknown)))

    def test_recent_attempts_preserve_actual_requested_and_unknown_tiers(self):
        events = [self.event("fast", "default"), self.event("priority"), self.event()]
        for event in events:
            self.repo.insert_event(event)
        recent = self.repo.recent_account_attempts(50)
        self.assertEqual(
            [meter.service_tier_label(row) for row in recent],
            [meter.service_tier_label(vars(event)) for event in reversed(events)],
        )
        page = meter.dashboard_html(self.repo)
        table = page.split("最近 50 次账号尝试", 1)[1].split("</article>", 1)[0]
        self.assertIn("Standard（Fast 已降级）", table)
        self.assertIn("Fast（请求估算）", table)
        self.assertIn("模式未知", table)

    def test_sse_final_response_tier_wins_and_nested_content_is_ignored(self):
        inspector = meter.SSEInspector()
        events = [
            {"type": "response.created", "response": {"service_tier": "priority"}},
            {"type": "response.completed", "response": {"service_tier": "default", "usage": {"input_tokens": 1000, "output_tokens": 100}}},
            {"type": "response.output_item.done", "item": {"service_tier": "fast"}},
        ]
        stream = b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in events)
        for offset in range(0, len(stream), 7):
            inspector.feed(stream[offset:offset + 7])
        inspector.finish()
        self.assertEqual(inspector.service_tier, "default")
        self.assertEqual(inspector.usage.output_tokens, 100)

    def test_proxy_forwards_fast_and_records_json_and_stream_response_tiers(self):
        forwarded = []

        class Upstream(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                forwarded.append(payload["service_tier"])
                response = {"model": "gpt-6-astra", "service_tier": "priority", "usage": {
                    "input_tokens": 1000, "output_tokens": 100,
                    "input_tokens_details": {"cached_tokens": 800},
                }}
                if payload.get("stream"):
                    response["service_tier"] = "default"
                    body = b"data: " + json.dumps({"type": "response.completed", "response": response}).encode() + b"\n\ndata: [DONE]\n\n"
                    content_type = "text/event-stream"
                else:
                    body = json.dumps(response).encode()
                    content_type = "application/json"
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        server = meter.create_server(
            "127.0.0.1", 0, f"http://127.0.0.1:{upstream.server_port}", self.repo.path,
            account_resolver=self.resolver, codex_app_import_enabled=False,
            cockpit_tools_import_enabled=False, sub2api_import_enabled=False,
        )
        threads = [threading.Thread(target=target.serve_forever, daemon=True) for target in (upstream, server)]
        for thread in threads:
            thread.start()
        try:
            for stream, expected_tier, expected_cost in ((False, "fast", 0.0156), (True, "default", 0.0078)):
                with self.subTest(stream=stream):
                    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                    try:
                        connection.request("POST", "/v1/responses", json.dumps({
                            "model": "gpt-6-astra", "service_tier": "fast", "stream": stream,
                        }), {"Content-Type": "application/json"})
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertIn(b'"service_tier"', response.read())
                    finally:
                        connection.close()
                    deadline = time.monotonic() + 3
                    row = None
                    while time.monotonic() < deadline:
                        with self.repo.connect() as conn:
                            row = conn.execute("SELECT * FROM usage_events WHERE stream=?", (int(stream),)).fetchone()
                        if row is not None:
                            break
                        time.sleep(0.01)
                    self.assertIsNotNone(row)
                    self.assertEqual(row["service_tier"], expected_tier)
                    self.assertEqual(row["service_tier_source"], "response")
                    self.assertAlmostEqual(row["estimated_api_cost_usd"], expected_cost)
            self.assertEqual(forwarded, ["fast", "fast"])
        finally:
            server.shutdown()
            upstream.shutdown()
            server.server_close()
            upstream.server_close()
            for thread in threads:
                thread.join(timeout=3)

    def test_missing_fast_price_stays_unpriced_and_sync_backfills_at_fast_rate(self):
        self.repo.set_price("gpt-6-astra", 10, 50, 1, "fixture")
        event = self.event("fast", "priority")
        self.assertIsNone(event.estimated_api_cost_usd)
        self.repo.insert_event(event)
        self.assertEqual(self.sync_prices(), 1)
        with self.repo.connect() as conn:
            row = conn.execute("SELECT estimated_api_cost_usd, service_tier FROM usage_events").fetchone()
        self.assertAlmostEqual(row[0], 0.0156)
        self.assertEqual(row[1], "fast")
        self.assertEqual(self.sync_prices(), 0)

    def test_long_context_fast_prices_are_model_specific(self):
        usage = replace(self.usage, input_tokens=300000)
        sol = self.repo.price_components_for("gpt-5.6-sol", usage, "fast")
        self.assertTrue(sol.long_context_pricing_applied)
        self.assertAlmostEqual(sol.output_cost_usd, 0.006)
        self.assertIsNone(self.repo.price_for("gpt-5.5", usage, "fast"))

    def test_sub2api_historical_tier_update_preserves_snapshot_without_double_charge(self):
        api = meter.Sub2APIImporter(self.repo, self.resolver)
        record = usage_fixture(1, 1, created_at=meter.utc_now())
        record.update(model="gpt-6-astra", input_tokens=200, cache_creation_tokens=0,
                      cache_read_tokens=800, output_tokens=100, input_cost=0.004,
                      cache_creation_cost=0, cache_read_cost=0.0016,
                      output_cost=0.01, total_cost=0.0156)
        event, key = api._event_from_record(record)
        self.repo.sync_imported_event(event, key, "sub2api")
        record["service_tier"] = "priority"
        event, key = api._event_from_record(record)
        self.assertEqual(self.repo.sync_imported_events([(event, key)], "sub2api")["changed"], 1)
        self.assertFalse(self.repo.sync_imported_event(event, key, "sub2api"))
        with self.repo.connect() as conn:
            row = conn.execute("SELECT COUNT(*), SUM(estimated_api_cost_usd), MAX(service_tier) FROM usage_events").fetchone()
        self.assertEqual(row[0], 1)
        self.assertAlmostEqual(row[1], 0.0156)
        self.assertEqual(row[2], "fast")

    def test_astra_context_repair_retains_fast_rate_and_is_idempotent(self):
        usage = replace(self.usage, input_tokens=300000)
        fast = self.repo.price_components_for("gpt-6-astra", usage, "fast")
        frozen = meter.PriceComponents(fast.non_cached_input_cost_usd * 2, fast.cached_input_cost_usd * 2,
                                       fast.output_cost_usd * 1.5, True)
        corrected = self.repo.correct_astra_context_components("gpt-6-astra", usage, frozen, "priority")
        self.assertAlmostEqual(corrected.total_cost_usd, fast.total_cost_usd)
        self.assertFalse(corrected.long_context_pricing_applied)
        event = replace(self.event("fast", "priority"), input_tokens=300000,
                        non_cached_input_cost_usd=frozen.non_cached_input_cost_usd,
                        cached_input_cost_usd=frozen.cached_input_cost_usd,
                        output_cost_usd=frozen.output_cost_usd, estimated_api_cost_usd=frozen.total_cost_usd,
                        long_context_pricing_applied=1)
        self.repo.insert_event(event)
        for _ in range(2):
            self.repo.initialize()
            with self.repo.connect() as conn:
                row = conn.execute("SELECT estimated_api_cost_usd, long_context_pricing_applied FROM usage_events").fetchone()
            self.assertAlmostEqual(row[0], fast.total_cost_usd)
            self.assertEqual(row[1], 0)

    def test_sub2api_one_time_backfill_uses_full_configured_window(self):
        api = meter.Sub2APIImporter(self.repo, self.resolver, backfill_days=30)
        now = datetime.now(timezone.utc)
        self.repo.save_remote_import_state("sub2api", now)
        self.assertEqual(api._usage_range(now)[0], (now - timedelta(days=30)).date().isoformat())
        ranges = api._usage_ranges(now)
        self.assertEqual(ranges[0][1], (now - timedelta(days=1)).date().isoformat())
        self.assertEqual(ranges[1], (now.date().isoformat(), now.date().isoformat()))
        self.repo.mark_service_tier_backfill("sub2api")
        self.assertEqual(api._usage_range(now)[0], (now - timedelta(days=1)).date().isoformat())
        self.assertEqual(api._usage_ranges(now), [api._usage_range(now)])

    def test_cockpit_preserves_fast_snapshot_and_accepts_missing_tier(self):
        importer = meter.CockpitToolsImporter(self.repo, self.resolver, data_dir=self.root)
        row = {"event_key": "fixture-event", "timestamp": int(time.time()),
               "account_id": "fixture-selector", "model_id": "gpt-6-astra", "success": 1,
               "input_tokens": 1000, "cached_tokens": 800, "output_tokens": 100,
               "input_usd_per_million": 20, "cached_input_usd_per_million": 2,
               "output_usd_per_million": 100, "service_tier": "priority"}
        event, _ = importer._event_from_row(row)
        self.assertEqual(event.service_tier, "fast")
        self.assertAlmostEqual(event.estimated_api_cost_usd, 0.0156)
        del row["service_tier"]
        unknown, _ = importer._event_from_row(row)
        self.assertIsNone(unknown.service_tier)
        self.assertEqual(unknown.estimated_api_cost_usd, event.estimated_api_cost_usd)

    def test_local_turn_tier_survives_resume_and_missing_next_turn_is_unknown(self):
        importer = meter.CodexAppLocalImporter(self.repo, self.resolver, codex_home=self.root)
        path = self.root / "session.jsonl"
        token = {"type": "event_msg", "timestamp": meter.utc_now(), "payload": {
            "type": "token_count", "info": {"last_token_usage": {
                "input_tokens": 1000, "cached_input_tokens": 800, "output_tokens": 100,
            }},
        }}
        records = [
            {"type": "session_meta", "payload": {"model_provider": "openai"}},
            {"type": "turn_context", "payload": {"model": "gpt-6-astra", "service_tier": "fast"}}, token,
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in records))
        identity = meter.AccountIdentity(None, None, None)
        scan = importer._scan_file(path, identity, None, None, None, None)
        self.assertEqual(scan.events[0][0].service_tier, "fast")
        self.assertAlmostEqual(scan.events[0][0].estimated_api_cost_usd, 0.0156)
        self.repo.save_local_import_file_state(scan.state)
        with path.open("a") as handle:
            handle.write(json.dumps(token) + "\n")
            handle.write(json.dumps({"type": "turn_context", "payload": {"model": "gpt-6-astra"}}) + "\n")
            handle.write(json.dumps(token) + "\n")
        scan = importer._scan_file(path, identity, None, None, None, self.repo.local_import_file_state(path))
        self.assertEqual([event.service_tier for event, _ in scan.events], ["fast", None])

    def test_dashboard_separates_fast_standard_and_unknown_costs(self):
        for request, response in (("fast", "priority"), ("fast", "default"), (None, None)):
            self.repo.insert_event(self.event(request, response))
        rows = self.repo.service_tier_breakdown()
        self.assertEqual({row["service_tier"] for row in rows}, {"fast", "default", None})
        self.assertEqual(sum(row["calls"] for row in rows), 3)
        self.assertAlmostEqual(sum(row["estimated_api_cost_usd"] for row in rows), 0.0312)
        page = meter.dashboard_html(self.repo)
        self.assertIn('data-role="service-tier-costs"', page)
        self.assertIn("$20.00/M", page)
        self.assertIn("$100.00/M", page)
        self.assertIn("Standard", page)


if __name__ == "__main__":
    unittest.main()
