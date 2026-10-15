import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from access_strategy import AccessScanner
from access_http import HttpScanError
from main import create_app
from search_config import RADIUS_URL
from service import Store


class AccessComparisonTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.scanner = AccessScanner(self.root / "access-probes.json")
        reader = patch("access_strategy.scan_reader", side_effect=RuntimeError("reader unavailable"))
        reader.start()
        self.addCleanup(reader.stop)

    async def test_complete_browser_method_is_reused_without_repeating_matrix(self):
        rows = [{"id": 1, "rent_eur": 1600}]
        outcomes = [
            {"variant": "direct_tab", "status": "blocked", "reason": "datadome_captcha"},
            {"variant": "home_then_search", "status": "ok", "listings": rows, "total": 1},
            {"variant": "home_then_search", "status": "ok", "listings": rows, "total": 1},
        ]
        with patch("access_strategy.probe_http", return_value={"results": []}) as http, patch(
            "access_strategy.probe_browser", AsyncMock(side_effect=outcomes)
        ) as browser:
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), rows)
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), rows)
        http.assert_called_once()
        self.assertEqual(browser.await_args_list[-1].args[-1], "home_then_search")
        report = json.loads(self.scanner.report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["source_url"], RADIUS_URL)
        self.assertEqual(report["last_scan"]["ids"], [1])
        self.assertEqual(report["state"], "working")
        self.assertNotIn("listings", report["experiments"][-1])

    async def test_first_page_candidate_does_not_count_as_complete_search(self):
        entry = {"profile": "chrome", "url": RADIUS_URL, "recognized_results": True}
        with patch("access_strategy.probe_http", return_value={"results": [entry]}), patch(
            "access_strategy.scan_http", side_effect=RuntimeError("Incomplete search")
        ), patch("access_strategy.probe_browser", AsyncMock(return_value={"status": "blocked"})):
            with self.assertRaisesRegex(RuntimeError, "no complete search"):
                await self.scanner("chromium", self.root / "profile")
        self.assertIsNone(self.scanner.selected)
        self.assertEqual(self.scanner.report["state"], "no_working_method")
        self.assertEqual(entry["complete_scan"]["status"], "failed")

    async def test_empty_valid_search_can_be_selected_and_report_read_only(self):
        entry = {"profile": "chrome", "url": RADIUS_URL, "recognized_results": True}
        with patch("access_strategy.probe_http", return_value={"results": [entry]}), patch(
            "access_strategy.scan_http", return_value=[]
        ):
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), [])
        store = Store(self.root / "listings.sqlite3")
        client = create_app(store, None, 180).test_client()
        self.assertEqual(client.get("/diagnostics").json["last_scan"]["total"], 0)
        self.assertEqual(client.post("/diagnostics").status_code, 405)

    async def test_all_failed_methods_are_not_retried_as_matrix_each_interval(self):
        with patch("access_strategy.probe_http", return_value={"results": []}) as http, patch(
            "access_strategy.probe_browser", AsyncMock(return_value={"status": "blocked", "reason": "challenge"})
        ) as browser:
            with self.assertRaises(RuntimeError):
                await self.scanner("chromium", self.root / "profile")
            initial_calls = browser.await_count
            with self.assertRaises(RuntimeError):
                await self.scanner("chromium", self.root / "profile")
            self.assertEqual(browser.await_count, initial_calls)
            http.assert_called_once()
        self.assertEqual(self.scanner.report["last_scan"]["status"], "failed")

    async def test_reader_is_used_when_direct_access_fails_and_reused_next_scan(self):
        rows = [{"id": 1, "rent_eur": 1600}]
        with patch("access_strategy.probe_http", return_value={"results": []}) as http, patch(
            "access_strategy.scan_reader", return_value=rows
        ) as reader, patch("access_strategy.probe_browser", AsyncMock()) as browser:
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), rows)
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), rows)
        http.assert_called_once()
        self.assertEqual(reader.call_count, 2)
        browser.assert_not_called()
        self.assertEqual(self.scanner.selected, {"method": "reader", "transport": "curl"})

    async def test_selected_http_can_recover_through_reader_without_full_matrix(self):
        self.scanner.started = True
        self.scanner.selected = {"method": "http", "profile": "chrome", "url": RADIUS_URL}
        rows = [{"id": 1}]
        with patch("access_strategy.scan_http", side_effect=RuntimeError("blocked")), patch(
            "access_strategy.scan_reader", return_value=rows
        ), patch("access_strategy.probe_http") as matrix:
            self.assertEqual(await self.scanner("chromium", self.root / "profile"), rows)
        matrix.assert_not_called()
        self.assertEqual(self.scanner.selected, {"method": "reader", "transport": "curl"})

    async def test_rate_limit_is_recorded_and_not_retried_by_changing_clients(self):
        error = HttpScanError("reader_http_status", status=429)
        error.details = {"name": "RateLimitTriggeredError", "status": 42901}
        with patch("access_strategy.probe_http", return_value={"results": []}), patch(
            "access_strategy.scan_reader", side_effect=error
        ) as reader, patch("access_strategy.probe_browser", AsyncMock(return_value={"status": "blocked"})):
            with self.assertRaises(RuntimeError):
                await self.scanner("chromium", self.root / "profile")
        reader.assert_called_once()
        entry = self.scanner.report["experiments"][0]
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(entry["http_status"], 429)
        self.assertEqual(entry["category"], "reader_http_status")
        self.assertEqual(entry["reader_error"]["name"], "RateLimitTriggeredError")


if __name__ == "__main__":
    unittest.main()
