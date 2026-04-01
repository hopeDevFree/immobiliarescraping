import os
import re
from pathlib import Path

import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname=os.getenv("db_name"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    host=os.getenv("db_host"),
    port=os.getenv("db_port")
)

def get_conn():
    return connection_pool.getconn()

def put_conn(conn):
    connection_pool.putconn(conn)

def ensure_column(cur, table_name, column_name, definition):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    if cur.fetchone() is None:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def normalize_image_gallery(image_urls):
    normalized_urls = []
    seen_keys = set()

    for image_url in image_urls or []:
        if not image_url:
            continue

        match = re.search(r"/image/(\d+)/", image_url)
        dedupe_key = match.group(1) if match else image_url

        if dedupe_key in seen_keys:
            continue

        seen_keys.add(dedupe_key)
        normalized_urls.append(image_url)

    return normalized_urls


def dedupe_annunci_galleries(cur):
    cur.execute("""
        SELECT id, url_immagini
        FROM annunci
        WHERE url_immagini IS NOT NULL
          AND cardinality(url_immagini) > 1
    """)

    for annuncio_id, image_urls in cur.fetchall():
        normalized_urls = normalize_image_gallery(image_urls)
        if normalized_urls and normalized_urls != image_urls:
            cur.execute(
                """
                UPDATE annunci
                SET url_immagini = %s::TEXT[],
                    url_immagine = %s
                WHERE id = %s
                """,
                (normalized_urls, normalized_urls[0], annuncio_id),
            )

def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS annunci (
                id BIGINT PRIMARY KEY,
                titolo TEXT,
                prezzo INTEGER,
                stanze INTEGER,
                superficie INTEGER,
                arredato BOOLEAN DEFAULT FALSE,
                ascensore BOOLEAN DEFAULT FALSE,
                terrazzo BOOLEAN DEFAULT FALSE,
                posto_auto BOOLEAN DEFAULT FALSE,
                url TEXT,
                url_immagine TEXT,
                url_immagini TEXT[] DEFAULT ARRAY[]::TEXT[],
                descrizione TEXT,
                zona TEXT,
                macrozona TEXT,
                microzona TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                tipo TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS utenti (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS preferiti (
                utente_id INTEGER REFERENCES utenti(id),
                annuncio_id BIGINT REFERENCES annunci(id),
                decision TEXT DEFAULT 'like',
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (utente_id, annuncio_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS visti (
                utente_id INTEGER REFERENCES utenti(id),
                annuncio_id BIGINT REFERENCES annunci(id),
                PRIMARY KEY (utente_id, annuncio_id)
            )
        """)
        ensure_column(cur, "annunci", "titolo", "TEXT")
        ensure_column(cur, "annunci", "prezzo", "INTEGER")
        ensure_column(cur, "annunci", "stanze", "INTEGER")
        ensure_column(cur, "annunci", "superficie", "INTEGER")
        ensure_column(cur, "annunci", "arredato", "BOOLEAN DEFAULT FALSE")
        ensure_column(cur, "annunci", "ascensore", "BOOLEAN DEFAULT FALSE")
        ensure_column(cur, "annunci", "terrazzo", "BOOLEAN DEFAULT FALSE")
        ensure_column(cur, "annunci", "posto_auto", "BOOLEAN DEFAULT FALSE")
        ensure_column(cur, "annunci", "url", "TEXT")
        ensure_column(cur, "annunci", "url_immagine", "TEXT")
        ensure_column(cur, "annunci", "url_immagini", "TEXT[] DEFAULT ARRAY[]::TEXT[]")
        ensure_column(cur, "annunci", "descrizione", "TEXT")
        ensure_column(cur, "annunci", "zona", "TEXT")
        ensure_column(cur, "annunci", "macrozona", "TEXT")
        ensure_column(cur, "annunci", "microzona", "TEXT")
        ensure_column(cur, "annunci", "latitude", "DOUBLE PRECISION")
        ensure_column(cur, "annunci", "longitude", "DOUBLE PRECISION")
        ensure_column(cur, "annunci", "tipo", "TEXT")
        ensure_column(cur, "annunci", "created_at", "TIMESTAMP DEFAULT NOW()")
        ensure_column(cur, "annunci", "updated_at", "TIMESTAMP DEFAULT NOW()")
        ensure_column(cur, "utenti", "created_at", "TIMESTAMP DEFAULT NOW()")
        ensure_column(cur, "preferiti", "decision", "TEXT DEFAULT 'like'")
        ensure_column(cur, "preferiti", "created_at", "TIMESTAMP DEFAULT NOW()")

        cur.execute("ALTER TABLE annunci ALTER COLUMN created_at SET DEFAULT NOW()")
        cur.execute("ALTER TABLE annunci ALTER COLUMN updated_at SET DEFAULT NOW()")
        cur.execute("ALTER TABLE annunci ALTER COLUMN url_immagini SET DEFAULT ARRAY[]::TEXT[]")
        cur.execute("ALTER TABLE annunci ALTER COLUMN arredato SET DEFAULT FALSE")
        cur.execute("ALTER TABLE annunci ALTER COLUMN ascensore SET DEFAULT FALSE")
        cur.execute("ALTER TABLE annunci ALTER COLUMN terrazzo SET DEFAULT FALSE")
        cur.execute("ALTER TABLE annunci ALTER COLUMN posto_auto SET DEFAULT FALSE")
        cur.execute("UPDATE annunci SET updated_at = COALESCE(updated_at, created_at)")
        cur.execute("ALTER TABLE utenti ALTER COLUMN created_at SET DEFAULT NOW()")
        cur.execute("ALTER TABLE preferiti ALTER COLUMN decision SET DEFAULT 'like'")
        cur.execute("ALTER TABLE preferiti ALTER COLUMN created_at SET DEFAULT NOW()")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_annunci_tipo_created_at_valid
            ON annunci (tipo, created_at DESC)
            WHERE titolo IS NOT NULL
              AND prezzo IS NOT NULL
              AND url IS NOT NULL
              AND url_immagine IS NOT NULL
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_annunci_tipo_prezzo_created_at_valid
            ON annunci (tipo, prezzo, created_at DESC)
            WHERE titolo IS NOT NULL
              AND prezzo IS NOT NULL
              AND url IS NOT NULL
              AND url_immagine IS NOT NULL
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_annunci_created_at_feed
            ON annunci (created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_annunci_location_valid
            ON annunci (latitude, longitude)
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
        """)
        dedupe_annunci_galleries(cur)

        conn.commit()
    finally:
        put_conn(conn)
