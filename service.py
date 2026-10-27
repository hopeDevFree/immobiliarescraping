"""Persistent scan history and a single sequential scheduler."""
from scraper_subito import fetch_subito_listings
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
from notifier import notify_new_listings
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sqlite3

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


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
    def __init__(self, path=None, database_url=None):
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        self.is_postgres = bool(self.database_url and HAS_PSYCOPG2)

        if not self.is_postgres:
            self.path = Path(path or "listings.sqlite3")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._init_sqlite()
        else:
            logger.info("Store: connessione a Supabase (PostgreSQL) attiva")

    def _init_sqlite(self):
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
        if self.is_postgres:
            conn = psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
            try:
                with conn:
                    with conn.cursor() as cur:
                        yield cur
            finally:
                conn.close()
        else:
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
        new_rows = []
        param = "%s" if self.is_postgres else "?"

        with self.connect() as db:
            # 1. Recupero annunci precedenti
            db.execute("SELECT id, payload, active FROM listings")
            previous_rows = db.fetchall()
            previous = {r["id"]: dict(r) for r in previous_rows}

            added = changed = 0

            # 2. Upsert degli annunci correnti
            upsert_query = f"""
                INSERT INTO listings (id, payload, first_seen, last_seen, active)
                VALUES ({param}, {param}, {param}, {param}, 1)
                ON CONFLICT(id) DO UPDATE SET 
                    payload = EXCLUDED.payload,
                    last_seen = EXCLUDED.last_seen, 
                    active = 1
            """

            for row in rows:
                payload = json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False)
                old = previous.get(row["id"])
                if old is None:
                    added += 1
                    new_rows.append(row)
                else:
                    changed += int(old["payload"] != payload or not old["active"])

                db.execute(upsert_query, (row["id"], payload, now, now))

            # 3. Disattiva annunci non più presenti
            missing = [i for i, r in previous.items() if r["active"] and i not in identifiers]
            if missing:
                update_query = f"UPDATE listings SET active=0 WHERE id={param}"
                db.executemany(update_query, [(i,) for i in missing])

            # 4. Registra scansione riuscita
            scan_query = f"""
                INSERT INTO scans (checked_at, ok, total, added, changed, removed)
                VALUES ({param}, 1, {param}, {param}, {param}, {param})
            """
            db.execute(scan_query, (now, len(rows), added, changed, len(missing)))

        return {
            "total": len(rows),
            "added": added,
            "changed": changed,
            "removed": len(missing),
            "new_rows": new_rows,
            "first_population": not previous,
        }

    def failure(self, error):
        param = "%s" if self.is_postgres else "?"
        query = f"INSERT INTO scans (checked_at, ok, error) VALUES ({param}, 0, {param})"
        with self.connect() as db:
            db.execute(query, (utc_now(), str(error)[:1000]))

    def status(self):
        with self.connect() as db:
            db.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1")
            last = db.fetchone()

            db.execute("SELECT checked_at FROM scans WHERE ok=1 ORDER BY id DESC LIMIT 1")
            success = db.fetchone()

            db.execute("SELECT COUNT(*) AS cnt FROM listings WHERE active=1")
            count = db.fetchone()

        last_success_val = None
        if success:
            last_success_val = success["checked_at"] if self.is_postgres else success[0]

        active_count = 0
        if count:
            active_count = count["cnt"] if self.is_postgres else count[0]

        return {
            "last_scan": dict(last) if last else None,
            "last_success": str(last_success_val) if last_success_val else None,
            "active_listings": active_count,
        }


def run_once(store, executable, profile_dir, scanner=scan):
    try:
        # Scansione Immobiliare + scansione Subito
        imm_rows = asyncio.run(scanner(executable, profile_dir))
        subito_rows = fetch_subito_listings()
        rows = imm_rows + subito_rows

        result = store.save(rows)
        new_rows = result.pop("new_rows", [])
        first = result.pop("first_population", False)
        logger.info("SCAN_OK %s", json.dumps(result))
        try:
            notify_new_listings(new_rows, first_population=first)
        except Exception:
            logger.exception("NOTIFY_FAILED (dati salvati comunque)")
        return True
    except Exception as exc:
        logger.exception("SCAN_FAILED: existing listings preserved")
        store.failure(exc)
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
