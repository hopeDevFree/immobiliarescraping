import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from listing_parser import inspect_html
from browser_runtime import open_browser
from browser_runtime import start_browser
from main import create_app
from scraper import ProtocolException, collect_pages, page_url, read_page
from search_config import RADIUS_URL
from service import Store, run_once, run_schedule


def listing(ident, price=500):
    return {"id": ident, "title": f"Home {ident}", "url": f"https://www.immobiliare.it/annunci/{ident}/", "rent_eur": price}


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(__file__).resolve().parent / "data"
        scratch.mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "test.sqlite3"
        self.store = Store(self.path)

    def test_restart_changes_and_missing_only_after_success(self):
        self.assertEqual(self.store.save([listing(1), listing(2)])["added"], 2)
        restarted = Store(self.path)
        self.assertEqual(restarted.save([listing(1), listing(2)])["added"], 0)
        result = restarted.save([listing(1, 550), listing(3)])
        self.assertEqual(result, {"total": 2, "added": 1, "changed": 1, "removed": 1})
        self.assertEqual(restarted.status()["active_listings"], 2)

    def test_block_keeps_last_success_and_active_listings(self):
        self.store.save([listing(1)])
        last_success = self.store.status()["last_success"]

        async def blocked(*_):
            raise RuntimeError("DataDome")

        with self.assertLogs("service", level="ERROR"):
            self.assertFalse(run_once(self.store, "browser", "profile", scanner=blocked))
        state = self.store.status()
        self.assertEqual(state["active_listings"], 1)
        self.assertEqual(state["last_success"], last_success)
        self.assertEqual(state["last_scan"]["ok"], 0)

    def test_failed_write_rolls_back_the_whole_scan(self):
        self.store.save([listing(1)])
        with self.assertRaises(ValueError):
            self.store.save([listing(1, 600), {"id": 2, "rent_eur": float("nan")}])
        with self.store.connect() as db:
            row = db.execute("SELECT payload FROM listings WHERE id=1").fetchone()
            self.assertEqual(json.loads(row[0])["rent_eur"], 500)
        self.assertEqual(self.store.status()["last_scan"]["total"], 1)

    def test_health_exposes_worker_and_actual_scan_status(self):
        worker = Mock()
        worker.is_alive.return_value = True
        client = create_app(self.store, worker, 180).test_client()
        self.store.failure("blocked")
        response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["interval_seconds"], 180)
        self.assertEqual(response.json["last_scan"]["ok"], 0)
        worker.is_alive.return_value = False
        self.assertEqual(client.get("/health").status_code, 503)


class SchedulingTests(unittest.TestCase):
    def test_interval_is_start_to_start_and_slow_runs_do_not_catch_up(self):
        for elapsed, expected in ((20, 160), (190, 180)):
            with self.subTest(elapsed=elapsed):
                stop = Mock()
                stop.is_set.side_effect = [False, True]
                scanner = Mock()
                times = iter([0, elapsed])
                run_schedule(None, "browser", "profile", stop, clock=lambda: next(times), scan_once=scanner)
                scanner.assert_called_once()
                stop.wait.assert_called_once_with(expected)


class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_browser_is_attached_without_starting_a_second_process(self):
        browser = Mock()
        browser.start = AsyncMock(side_effect=RuntimeError("Failed to connect to browser"))
        browser._process.returncode = None
        browser._http.get = AsyncMock(side_effect=[
            ConnectionRefusedError(), {"webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/browser/test"},
        ])
        browser.attach = AsyncMock()
        browser.update_targets = AsyncMock()
        with patch("browser_runtime.asyncio.sleep", AsyncMock()):
            await start_browser(browser)
        browser.start.assert_awaited_once()
        browser.attach.assert_awaited_once()
        browser.update_targets.assert_awaited_once()
        self.assertEqual(browser.websocket_url, "ws://127.0.0.1:9222/devtools/browser/test")

    async def test_startup_error_reports_chromium_stderr_and_display(self):
        browser = Mock()
        browser.start = AsyncMock(side_effect=RuntimeError("Failed to connect to browser"))
        browser._process.returncode = 1
        browser._process.communicate = AsyncMock(return_value=(b"", b"Missing X server or DISPLAY"))
        with patch("browser_runtime.nd.Browser", return_value=browser), patch.dict(
            "os.environ", {"DISPLAY": ":99"}
        ), patch("browser_runtime.nd.util.get_registered_instances", return_value={browser}):
            with self.assertRaisesRegex(RuntimeError, "Missing X server or DISPLAY") as raised:
                async with open_browser("browser", "profile"):
                    self.fail("Unreachable")
        self.assertIn("DISPLAY=:99", str(raised.exception))

    async def test_navigation_session_replacement_is_reattached(self):
        tab = Mock()
        tab.get_content = AsyncMock(side_effect=[
            ProtocolException({"code": -32001, "message": "Session with given id not found"}),
            "html", "html", "html", "html",
        ])
        tab.close = AsyncMock()
        browser = Mock()
        browser.get = AsyncMock(return_value=tab)
        info = {"rendered_listings": [listing(1)], "displayed_total_ads": 1}
        with patch("scraper.asyncio.sleep", AsyncMock()), patch("scraper.inspect_html", return_value=info):
            rows, total, _ = await read_page(browser, RADIUS_URL)
        self.assertEqual(total, 1)
        self.assertEqual(rows, [listing(1)])
        self.assertIsNone(tab.session_id)
        tab.close.assert_awaited_once()

    async def test_failed_browser_start_always_stops_and_reaps_child(self):
        browser = Mock()
        browser.start = AsyncMock(side_effect=RuntimeError("start failed"))
        browser._process.communicate = AsyncMock()
        registry = {browser}
        with patch("browser_runtime.nd.Browser", return_value=browser), patch(
            "browser_runtime.nd.util.get_registered_instances", return_value=registry
        ):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                async with open_browser("browser", "profile"):
                    self.fail("Unreachable")
        browser.stop.assert_called_once()
        browser._process.communicate.assert_awaited_once()
        self.assertFalse(registry)

    async def test_all_pages_with_exact_url_and_no_price_filter(self):
        reader = AsyncMock(side_effect=[([listing(1, 1600)], 2, {2}), ([listing(2)], 2, {1})])
        with patch("scraper.read_page", reader):
            rows = await collect_pages(None)
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(reader.await_args_list[0].args[1], RADIUS_URL)
        self.assertEqual(reader.await_args_list[1].args[1], page_url(RADIUS_URL, 2))

    async def test_incomplete_or_changing_results_are_errors(self):
        cases = [
            [([listing(1)], 2, set())],
            [([listing(1)], 2, {2}), ([listing(2)], 3, set())],
        ]
        for pages in cases:
            with patch("scraper.read_page", AsyncMock(side_effect=pages)):
                with self.assertRaises(RuntimeError):
                    await collect_pages(None)

    async def test_explicit_empty_search_is_valid_but_block_page_is_not(self):
        self.assertEqual(inspect_html("<p>0 risultati per: case in affitto</p>", RADIUS_URL)["displayed_total_ads"], 0)
        self.assertNotIn("displayed_total_ads", inspect_html("Please enable JS", RADIUS_URL))
        with patch("scraper.read_page", AsyncMock(return_value=([], 0, set()))):
            self.assertEqual(await collect_pages(None), [])


if __name__ == "__main__":
    unittest.main()
