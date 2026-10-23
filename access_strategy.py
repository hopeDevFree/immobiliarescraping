"""Run a finite access comparison once, then reuse a complete working method."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from access_browser import VARIANTS, probe_browser
from access_http import HttpScanError, probe_http, scan_http
from access_reader import READER_TRANSPORTS, scan_reader
from search_config import RADIUS_URL

logger = logging.getLogger(__name__)


def error_evidence(exc):
    result = {"error_type": type(exc).__name__}
    if isinstance(exc, HttpScanError):
        result.update(category=exc.category, http_status=exc.status, transport_error_code=exc.transport_error_code)
        if getattr(exc, "details", None):
            result["reader_error"] = exc.details
    return result


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

        # Andiamo direttamente a testare il reader, evitando test HTTP rumorosi e Chromium
        self.report["state"] = "testing_reader"
        self.publish()

        for transport in READER_TRANSPORTS:
            try:
                rows = await asyncio.to_thread(scan_reader, transport=transport)
            except Exception as exc:
                self.report["experiments"].append({
                    "method": "reader",
                    "transport": transport,
                    "status": "failed",
                    **error_evidence(exc)
                })
                self.publish()
                if isinstance(exc, HttpScanError) and exc.status in (402, 429, 451):
                    break
            else:
                self.report["experiments"].append({
                    "method": "reader",
                    "transport": transport,
                    "status": "ok",
                    "total": len(rows)
                })
                return self.choose({"method": "reader", "transport": transport}, rows)

        # Se il reader fallisce, ci fermiamo qui senza lanciare Chromium
        self.report.update(state="no_working_method", selected=None)
        self.publish()
        raise RuntimeError("READER_FAILED: Impossibile recuperare gli annunci tramite reader.")

    async def repeat(self, executable, profile_dir):
        # Do not repeat the matrix or repeatedly launch browsers from a blocked
        # network. Retry one bounded Reader scan if no method has worked yet.
        method = self.selected or {"method": "reader", "transport": "curl"}
        try:
            if method["method"] == "http":
                rows = await asyncio.to_thread(scan_http, method["profile"], method["url"])
            elif method["method"] == "reader":
                rows = await asyncio.to_thread(scan_reader, transport=method.get("transport", "curl"))
            else:
                result = await probe_browser(executable, Path(profile_dir).parent / "access-profiles",
                                             method["variant"])
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
                    return self.choose({"method": "reader", "transport": "curl"}, rows)
            self.report["state"] = "unavailable"
            self.report["last_scan"] = {"status": "failed", **error_evidence(exc)}
            self.publish()
            raise
