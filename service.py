"""Persistent scan history and a single sequential scheduler."""

import asyncio
import json
import logging
import os
import queue
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from scraper import scan

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class TelegramNotifier:
    """Invia notifiche Telegram in un thread separato usando la Bot API HTTP.

    Configurazione tramite variabili d'ambiente:
      TELEGRAM_BOT_TOKEN  - Token del bot (da @BotFather)
      TELEGRAM_CHAT_ID    - ID della chat/gruppo dove inviare i messaggi

    Se le variabili non sono configurate, le notifiche sono disabilitate
    e il notifier non interferisce con lo scraper.
    """

    def __init__(self):
        self._queue = queue.Queue()
        self._thread = None
        self._stop = threading.Event()
        self._enabled = False

        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

        if self.bot_token and self.chat_id:
            self._enabled = True
            logger.info("Telegram notifier enabled")
        else:
            logger.warning(
                "Telegram notifications disabled: "
                "set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable"
            )

    def start(self):
        """Avvia il thread di invio. Chiamare una volta all'avvio del servizio."""
        if not self._enabled:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="telegram-notifier", daemon=True
        )
        self._thread.start()

    def stop(self):
        """Ferma il thread di invio. Chiamare allo shutdown."""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def notify(self, message):
        """Accoda un messaggio per l'invio. Non blocca il chiamante."""
        if self._enabled:
            self._queue.put(message)

    def _run(self):
        """Loop del thread: legge dalla coda e invia via HTTP."""
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        while not self._stop.is_set():
            try:
                message = self._queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                data = urllib.parse.urlencode({
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                }).encode("utf-8")
                req = urllib.request.Request(url, data=data, method="POST")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
                urllib.request.urlopen(req, timeout=10)
                logger.info("Telegram notification sent")
            except Exception as exc:
                # Non propagare mai: una notifica fallita non deve fermare lo scraper
                logger.error("Failed to send Telegram notification: %s", exc)


# Istanza globale del notifier
notifier = TelegramNotifier()


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS listings (
                    id INTEGER PRIMARY KEY, payload TEXT NOT NULL,
                    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY, checked_at TEXT NOT NULL,
                    ok INTEGER NOT NULL, total INTEGER,
                    added INTEGER, changed INTEGER, removed INTEGER, error TEXT
                );
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def save(self, rows):
        identifiers = {row["id"] for row in rows}
        if len(identifiers) != len(rows):
            raise ValueError("Duplicate listing IDs in scan")
        now = utc_now()
        with self.connect() as db:
            previous = {r["id"]: r for r in db.execute("SELECT * FROM listings")}
            added = changed = 0
            for row in rows:
                payload = json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False)
                old = previous.get(row["id"])
                added += int(old is None)
                changed += int(old is not None and (old["payload"] != payload or not old["active"]))
                db.execute("""
                    INSERT INTO listings (id, payload, first_seen, last_seen, active)
                    VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,
                        last_seen=excluded.last_seen, active=1
                """, (row["id"], payload, now, now))
            missing = [i for i, r in previous.items() if r["active"] and i not in identifiers]
            db.executemany("UPDATE listings SET active=0 WHERE id=?", [(i,) for i in missing])
            db.execute("""
                INSERT INTO scans (checked_at, ok, total, added, changed, removed)
                VALUES (?, 1, ?, ?, ?, ?)
            """, (now, len(rows), added, changed, len(missing)))
        return {"total": len(rows), "added": added, "changed": changed, "removed": len(missing)}

    def failure(self, error):
        with self.connect() as db:
            db.execute(
                "INSERT INTO scans (checked_at, ok, error) VALUES (?, 0, ?)",
                (utc_now(), str(error)[:1000]),
            )

    def status(self):
        with self.connect() as db:
            last = db.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
            success = db.execute(
                "SELECT checked_at FROM scans WHERE ok=1 ORDER BY id DESC LIMIT 1"
            ).fetchone()
            count = db.execute(
                "SELECT COUNT(*) FROM listings WHERE active=1"
            ).fetchone()[0]
        return {
            "last_scan": dict(last) if last else None,
            "last_success": success[0] if success else None,
            "active_listings": count,
        }


def run_once(store, executable, profile_dir, scanner=scan):
    try:
        rows = asyncio.run(scanner(executable, profile_dir))
        result = store.save(rows)
        logger.info("SCAN_OK %s", json.dumps(result))

        # Notifica Telegram
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        if result["added"] or result["changed"] or result["removed"]:
            msg = (
                f"✅ <b>Scan OK</b> ({now})\n"
                f"📊 Totale: {result['total']}\n"
                f"➕ Nuovi: {result['added']}\n"
                f"🔄 Modificati: {result['changed']}\n"
                f"➖ Rimossi: {result['removed']}"
            )
        else:
            msg = f"✅ Scan OK ({now}): {result['total']} annunci, nessuna novità"
        notifier.notify(msg)

        return True
    except Exception as exc:
        logger.exception("SCAN_FAILED: existing listings preserved")
        store.failure(exc)

        # Notifica Telegram per il fallimento
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        error_short = str(exc)[:300]
        msg = f"❌ <b>Scan FALLITA</b> ({now})\n🚫 {error_short}"
        notifier.notify(msg)

        return False


def run_schedule(store, executable, profile_dir, stop, interval=180, clock=time.monotonic, scan_once=run_once):
    if interval < 180:
        raise ValueError("Minimum scan interval is 180 seconds")

    # Avvia il notifier Telegram
    notifier.start()

    # Notifica di avvio servizio
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    notifier.notify(
        f"🚀 <b>Servizio avviato</b> ({now})\n"
        f"⏰ Intervallo: {interval}s ({interval // 60} min)"
    )

    try:
        while not stop.is_set():
            started = clock()
            scan_once(store, executable, profile_dir)
            delay = interval - (clock() - started)
            stop.wait(delay if delay > 0 else interval)
    finally:
        notifier.stop()
