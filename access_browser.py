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
        logger.info("ACCESS_METHOD_SELECTED %s total=%s", method.get("method") or method.get("variant"), len(rows))
        return rows

    async def __call__(self, executable, profile_dir):
        if self.started:
            return await self.repeat(executable, profile_dir)
        self.started = True

        # 1. Testiamo prima il reader (veloce)
        self.report["state"] = "testing_reader"
        self.publish()

        reader_blocked = False
        for transport in READER_TRANSPORTS:
            try:
                logger.info("Test reader con transport='%s'...", transport)
                rows = await asyncio.to_thread(scan_reader, transport=transport)
            except Exception as exc:
                logger.warning("Reader transport='%s' non riuscito (DataDome/Captcha rilevato): %s", transport, exc)
                self.report["experiments"].append({
                    "method": "reader",
                    "transport": transport,
                    "status": "failed",
                    **error_evidence(exc)
                })
                self.publish()
                # Se DataDome intercetta il reader, non ha senso provare gli altri transport
                reader_blocked = True
                break
            else:
                self.report["experiments"].append({
                    "method": "reader",
                    "transport": transport,
                    "status": "ok",
                    "total": len(rows)
                })
                return self.choose({"method": "reader", "transport": transport}, rows)

        # 2. Se il Reader è bloccato da DataDome, avviamo Chromium stealth (nodriver)
        logger.info("Reader bloccato da DataDome. Avvio scansione con Chromium anti-detect...")
        self.report["state"] = "testing_browser"
        self.publish()

        profiles_root = Path(profile_dir).parent / "access-profiles"
        for variant in VARIANTS:
            logger.info("Tentativo Browser con variante='%s'...", variant)
            try:
                result = await probe_browser(executable, profiles_root, variant)
                status = result.get("status")
                if status == "ok" and result.get("listings") is not None:
                    rows = result["listings"]
                    logger.info("Browser variante '%s' RIUSCITO! Trovati %d annunci", variant, len(rows))
                    self.report["experiments"].append({
                        "method": "browser",
                        "variant": variant,
                        "status": "ok",
                        "total": len(rows)
                    })
                    return self.choose({"method": "browser", "variant": variant}, rows)
                else:
                    logger.warning("Browser variante '%s' non riuscito: reason=%s (status=%s)", variant, result.get("reason"), status)
                    self.report["experiments"].append({
                        "method": "browser",
                        "variant": variant,
                        "status": "failed",
                        "reason": result.get("reason")
                    })
                    self.publish()
            except Exception as exc:
                logger.error("Errore durante test browser variante '%s': %r", variant, exc, exc_info=True)
                self.report["experiments"].append({
                    "method": "browser",
                    "variant": variant,
                    "status": "failed",
                    "reason": str(exc)
                })
                self.publish()

        self.report.update(state="no_working_method", selected=None)
        self.publish()
        raise RuntimeError("Tutti i metodi di accesso (Reader e Browser) sono stati bloccati da DataDome.")

    async def repeat(self, executable, profile_dir):
        method = self.selected or {"method": "browser", "variant": "home_then_search"}
        try:
            if method.get("method") == "reader":
                rows = await asyncio.to_thread(scan_reader, transport=method.get("transport", "curl"))
            elif method.get("method") == "http":
                rows = await asyncio.to_thread(scan_http, method["profile"], method["url"])
            else:
                variant = method.get("variant", "home_then_search")
                result = await probe_browser(executable, Path(profile_dir).parent / "access-profiles", variant)
                rows = result.pop("listings", None)
                if result.get("status") != "ok" or rows is None:
                    self.report["last_browser_result"] = result
                    raise RuntimeError("SEARCH_BLOCKED: " + result.get("reason", "search_unavailable"))
            return self.choose(method, rows)
        except Exception as exc:
            self.report["state"] = "unavailable"
            self.report["last_scan"] = {"status": "failed", **error_evidence(exc)}
            self.publish()
            raise