"""Run a finite access comparison once, then reuse a complete working method."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from access_browser import VARIANTS, probe_browser
from access_http import probe_http, scan_http
from access_reader import scan_reader
from search_config import RADIUS_URL

logger = logging.getLogger(__name__)


class AccessScanner:
    def __init__(self, report_path):
        self.report_path = Path(report_path)
        self.started = False
        self.selected = None
        self.report = {
            "source_url": RADIUS_URL, "filters_unchanged": True,
            "state": "pending", "selected": None, "experiments": [],
        }

    def publish(self):
        self.report["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.report["revision"] = os.environ.get("RENDER_GIT_COMMIT", "unknown")
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.report_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.report, ensure_ascii=False), encoding="utf-8")
        temporary.replace(self.report_path)

    def choose(self, method, rows):
        self.selected = method
        self.report.update(state="working", selected=method)
        self.report["last_scan"] = {"status": "ok", "total": len(rows), "ids": [r["id"] for r in rows]}
        self.publish()
        logger.info("ACCESS_METHOD_SELECTED %s total=%s", method["method"], len(rows))
        return rows

    async def __call__(self, executable, profile_dir):
        if self.started:
            return await self.repeat(executable, profile_dir)
        self.started = True
        self.report["state"] = "testing_http"
        self.publish()
        try:
            results = await asyncio.to_thread(probe_http)
            self.report["experiments"].extend(results["results"])
            self.publish()
            for entry in results["results"]:
                if not entry.get("recognized_results"):
                    continue
                method = {"method": "http", "profile": entry["profile"], "url": entry["url"]}
                try:
                    rows = await asyncio.to_thread(scan_http, entry["profile"], entry["url"])
                except Exception as exc:
                    entry["complete_scan"] = {"status": "failed", "error_type": type(exc).__name__}
                    self.publish()
                else:
                    entry["complete_scan"] = {"status": "ok", "total": len(rows)}
                    return self.choose(method, rows)
        except Exception as exc:
            self.report["experiments"].append({"method": "http", "error_type": type(exc).__name__})
            self.publish()

        self.report["state"] = "testing_reader"
        self.publish()
        try:
            rows = await asyncio.to_thread(scan_reader)
        except Exception as exc:
            self.report["experiments"].append({"method": "reader", "status": "failed", "error_type": type(exc).__name__})
            self.publish()
        else:
            self.report["experiments"].append({"method": "reader", "status": "ok", "total": len(rows)})
            return self.choose({"method": "reader"}, rows)

        for variant in VARIANTS:
            self.report["state"] = "testing_" + variant
            self.publish()
            result = await probe_browser(executable, Path(profile_dir).parent / "access-profiles", variant)
            rows = result.pop("listings", None)
            self.report["experiments"].append(result)
            self.publish()
            if result["status"] == "ok" and rows is not None:
                return self.choose({"method": "browser", "variant": variant}, rows)

        self.report.update(state="no_working_method", selected=None)
        self.publish()
        raise RuntimeError("SEARCH_BLOCKED: bounded free access comparison found no complete search; see /diagnostics")

    async def repeat(self, executable, profile_dir):
        # A failed matrix is never repeated every three minutes. Retry one browser
        # navigation only, retaining its session, so a temporary block can clear.
        method = self.selected or {"method": "browser", "variant": "direct_tab"}
        try:
            if method["method"] == "http":
                rows = await asyncio.to_thread(scan_http, method["profile"], method["url"])
            elif method["method"] == "reader":
                rows = await asyncio.to_thread(scan_reader)
            else:
                result = await probe_browser(executable, Path(profile_dir).parent / "access-profiles", method["variant"])
                rows = result.pop("listings", None)
                if result["status"] != "ok" or rows is None:
                    self.report["last_browser_result"] = result
                    raise RuntimeError("SEARCH_BLOCKED: " + result.get("reason", "search_unavailable"))
            return self.choose(method, rows)
        except Exception as exc:
            # One lighter fallback can recover an HTTP method that becomes
            # unavailable. The full startup comparison is never repeated.
            if method["method"] == "http":
                try:
                    rows = await asyncio.to_thread(scan_reader)
                except Exception:
                    pass
                else:
                    return self.choose({"method": "reader"}, rows)
            self.report["state"] = "unavailable"
            self.report["last_scan"] = {"status": "failed", "error_type": type(exc).__name__}
            self.publish()
            raise
