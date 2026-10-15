"""Persistent scan history and a single sequential scheduler."""

import asyncio
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from scraper import scan

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


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
            db.execute("INSERT INTO scans (checked_at, ok, error) VALUES (?, 0, ?)", (utc_now(), str(error)[:1000]))

    def status(self):
        with self.connect() as db:
            last = db.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
            success = db.execute("SELECT checked_at FROM scans WHERE ok=1 ORDER BY id DESC LIMIT 1").fetchone()
            count = db.execute("SELECT COUNT(*) FROM listings WHERE active=1").fetchone()[0]
        return {"last_scan": dict(last) if last else None, "last_success": success[0] if success else None, "active_listings": count}


def run_once(store, executable, profile_dir, scanner=scan):
    try:
        rows = asyncio.run(scanner(executable, profile_dir))
        result = store.save(rows)
        logger.info("SCAN_OK %s", json.dumps(result))
        return True
    except Exception as exc:
        logger.exception("SCAN_FAILED: existing listings preserved")
        store.failure(exc)
        return False


def run_schedule(store, executable, profile_dir, stop, interval=180, clock=time.monotonic, scan_once=run_once):
    if interval < 180:
        raise ValueError("Minimum scan interval is 180 seconds")
    while not stop.is_set():
        started = clock()
        scan_once(store, executable, profile_dir)
        delay = interval - (clock() - started)
        stop.wait(delay if delay > 0 else interval)
