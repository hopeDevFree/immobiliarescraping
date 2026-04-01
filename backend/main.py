import asyncio
import hashlib
import logging
import re
import secrets
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

from database import get_conn, put_conn, init_db
from scraper import fetch_detail_description_for_listing, scrape, should_refresh_description

logger = logging.getLogger(__name__)
SUPPORTED_LISTING_TYPES = {"casa", "stanza"}


async def bootstrap_app(app: FastAPI):
    app.state.db_ready = False
    app.state.bootstrap_error = None

    try:
        await asyncio.to_thread(init_db)
        app.state.db_ready = True
        logger.info("Database inizializzato correttamente.")
    except Exception as exc:
        app.state.bootstrap_error = str(exc)
        logger.exception("Inizializzazione database fallita.")
        return

    try:
        scheduler = AsyncIOScheduler(timezone="Europe/Rome")
        scheduler.add_job(scrape, "interval", minutes=10, next_run_time=datetime.now() + timedelta(seconds=5))
        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("Scheduler avviato correttamente.")
    except Exception as exc:
        app.state.bootstrap_error = str(exc)
        logger.exception("Avvio scheduler fallito.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.scheduler = None
    app.state.bootstrap_task = asyncio.create_task(bootstrap_app(app))
    yield
    bootstrap_task = getattr(app.state, "bootstrap_task", None)
    if bootstrap_task and not bootstrap_task.done():
        bootstrap_task.cancel()
        with suppress(asyncio.CancelledError):
            await bootstrap_task

    scheduler = getattr(app.state, "scheduler", None)
    if scheduler and scheduler.running:
        scheduler.shutdown()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def block_requests_until_ready(request, call_next):
    if request.url.path == "/health":
        return await call_next(request)

    bootstrap_task = getattr(app.state, "bootstrap_task", None)
    db_ready = getattr(app.state, "db_ready", False)
    bootstrap_error = getattr(app.state, "bootstrap_error", None)

    if bootstrap_task and not bootstrap_task.done():
        return JSONResponse(
            status_code=503,
            content={"detail": "Backend in avvio. Riprova tra qualche secondo."},
        )

    if bootstrap_error or not db_ready:
        return JSONResponse(
            status_code=503,
            content={"detail": "Backend non pronto.", "error": bootstrap_error},
        )

    return await call_next(request)


@app.get("/health")
def health():
    bootstrap_task = getattr(app.state, "bootstrap_task", None)
    scheduler = getattr(app.state, "scheduler", None)
    return {
        "ok": True,
        "db_ready": getattr(app.state, "db_ready", False),
        "bootstrap_complete": bootstrap_task.done() if bootstrap_task else True,
        "scheduler_running": bool(scheduler and scheduler.running),
        "bootstrap_error": getattr(app.state, "bootstrap_error", None),
    }


def ensure_full_description(cur, annuncio_id, descrizione):
    normalized_description = descrizione or ""
    if not should_refresh_description(normalized_description):
        return normalized_description

    try:
        refreshed_description = asyncio.run(fetch_detail_description_for_listing(annuncio_id))
    except Exception as exc:
        logger.warning("Refresh descrizione non riuscito per annuncio %s: %s", annuncio_id, exc)
        return normalized_description

    if not refreshed_description:
        return normalized_description

    cur.execute(
        "UPDATE annunci SET descrizione = %s WHERE id = %s",
        (refreshed_description, annuncio_id),
    )
    return refreshed_description


def refresh_description_in_background(annuncio_id: int):
    try:
        refreshed_description = asyncio.run(fetch_detail_description_for_listing(annuncio_id))
    except Exception as exc:
        logger.warning("Refresh descrizione in background fallito per annuncio %s: %s", annuncio_id, exc)
        return

    if not refreshed_description:
        return

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE annunci SET descrizione = %s WHERE id = %s",
            (refreshed_description, annuncio_id),
        )
        conn.commit()
    finally:
        put_conn(conn)


def queue_description_refresh(background_tasks: BackgroundTasks, annuncio_id: int, descrizione: str | None):
    normalized_description = descrizione or ""
    if should_refresh_description(normalized_description):
        background_tasks.add_task(refresh_description_in_background, annuncio_id)

    return normalized_description


def parse_exclude_ids(exclude_ids: str | None):
    parsed_ids = []
    if not exclude_ids:
        return parsed_ids

    for raw_value in exclude_ids.split(","):
        raw_value = raw_value.strip()
        if not raw_value:
            continue
        try:
            parsed_ids.append(int(raw_value))
        except ValueError:
            continue

    return parsed_ids


DEFAULT_LOCATION_LABEL = "Tutta Napoli"
AUTH_ERROR_MESSAGE = "Sessione non valida. Rientra per continuare."
USERNAME_CONFLICT_MESSAGE = (
    "Questo username e gia associato a un'altra sessione locale. "
    "Usa lo stesso dispositivo oppure scegline un altro."
)


def build_distance_expression(table_alias: str):
    return f"""
        6371 * acos(
            LEAST(
                1.0,
                GREATEST(
                    -1.0,
                    cos(radians(%s)) * cos(radians({table_alias}.latitude))
                    * cos(radians({table_alias}.longitude) - radians(%s))
                    + sin(radians(%s)) * sin(radians({table_alias}.latitude))
                )
            )
        )
    """


def serialize_listing(row, descrizione):
    return {
        "id": row["id"],
        "titolo": row["titolo"],
        "prezzo": row["prezzo"],
        "stanze": row["stanze"],
        "superficie": row["superficie"],
        "arredato": bool(row["arredato"]),
        "ascensore": bool(row["ascensore"]),
        "terrazzo": bool(row["terrazzo"]),
        "posto_auto": bool(row["posto_auto"]),
        "url": row["url"],
        "url_immagine": row["url_immagine"],
        "url_immagini": row["url_immagini"] or [row["url_immagine"]],
        "descrizione": descrizione,
        "zona": row["zona"],
        "macrozona": row["macrozona"],
        "microzona": row["microzona"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "tipo": row["tipo"],
        "distance_km": row.get("distance_km"),
    }


def serialize_preferito(row):
    base_listing = serialize_listing(row, row.get("descrizione") or "")
    base_listing["decision"] = row.get("decision") or "like"
    return base_listing


def upsert_preferito(cur, utente_id: int, annuncio_id: int, decision: str):
    cur.execute(
        """
        INSERT INTO preferiti (utente_id, annuncio_id, decision)
        VALUES (%s, %s, %s)
        ON CONFLICT (utente_id, annuncio_id) DO UPDATE
        SET decision = EXCLUDED.decision,
            created_at = NOW()
        """,
        (utente_id, annuncio_id, decision),
    )


def slugify_label(value: str):
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "posizione"


def normalize_listing_type(value: str | None):
    normalized = str(value or "casa").strip().lower()
    return normalized if normalized in SUPPORTED_LISTING_TYPES else "casa"


def hash_auth_token(token: str):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def issue_auth_token():
    return secrets.token_urlsafe(32)


def extract_bearer_token(authorization: str | None):
    if not authorization:
        return ""

    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return ""

    return value.strip()


def get_current_user(authorization: str | None = Header(default=None)):
    auth_token = extract_bearer_token(authorization)
    if not auth_token:
        raise HTTPException(status_code=401, detail=AUTH_ERROR_MESSAGE)

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT id, username
            FROM utenti
            WHERE auth_token_hash = %s
            """,
            (hash_auth_token(auth_token),),
        )
        user = cur.fetchone()
    finally:
        put_conn(conn)

    if user is None:
        raise HTTPException(status_code=401, detail=AUTH_ERROR_MESSAGE)

    return {"id": user["id"], "username": user["username"]}


# --- Models ---

class UsernameBody(BaseModel):
    username: str
    auth_token: str | None = None
    legacy_user_id: int | None = None


# --- Utenti ---

@app.post("/utenti/login")
def login(body: UsernameBody):
    normalized_username = body.username.strip()
    if not normalized_username:
        raise HTTPException(status_code=400, detail="Username obbligatorio.")

    provided_auth_token = (body.auth_token or "").strip()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT id, username, auth_token_hash
            FROM utenti
            WHERE username = %s
            """,
            (normalized_username,),
        )
        utente = cur.fetchone()

        if utente is None:
            issued_auth_token = issue_auth_token()
            cur.execute(
                """
                INSERT INTO utenti (username, auth_token_hash)
                VALUES (%s, %s)
                RETURNING id, username
                """,
                (normalized_username, hash_auth_token(issued_auth_token)),
            )
            created_user = cur.fetchone()
            conn.commit()
            return {
                "id": created_user["id"],
                "username": created_user["username"],
                "auth_token": issued_auth_token,
            }

        stored_auth_token_hash = utente["auth_token_hash"] or ""
        if provided_auth_token and hash_auth_token(provided_auth_token) == stored_auth_token_hash:
            return {
                "id": utente["id"],
                "username": utente["username"],
                "auth_token": provided_auth_token,
            }

        if not stored_auth_token_hash and body.legacy_user_id == utente["id"]:
            issued_auth_token = issue_auth_token()
            cur.execute(
                """
                UPDATE utenti
                SET auth_token_hash = %s
                WHERE id = %s
                """,
                (hash_auth_token(issued_auth_token), utente["id"]),
            )
            conn.commit()
            return {
                "id": utente["id"],
                "username": utente["username"],
                "auth_token": issued_auth_token,
            }

        raise HTTPException(status_code=409, detail=USERNAME_CONFLICT_MESSAGE)
    finally:
        put_conn(conn)

# --- Annunci ---

@app.get("/posizioni")
def get_posizioni(tipo: str = "casa"):
    normalized_tipo = normalize_listing_type(tipo)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            WITH valid_locations AS (
                SELECT
                    COALESCE(
                        NULLIF(BTRIM(microzona), ''),
                        NULLIF(BTRIM(macrozona), ''),
                        NULLIF(BTRIM(zona), '')
                    ) AS label,
                    latitude,
                    longitude
                FROM annunci
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND titolo IS NOT NULL
                  AND prezzo IS NOT NULL
                  AND url IS NOT NULL
                  AND url_immagine IS NOT NULL
                  AND tipo = %s
            ),
            grouped_locations AS (
                SELECT
                    label,
                    AVG(latitude) AS latitude,
                    AVG(longitude) AS longitude,
                    COUNT(*) AS listing_count
                FROM valid_locations
                WHERE label IS NOT NULL
                GROUP BY label
            )
            SELECT
                label,
                latitude,
                longitude,
                listing_count
            FROM grouped_locations
            ORDER BY listing_count DESC, label ASC
            LIMIT 10
            """
            ,
            (normalized_tipo,),
        )
        zone_rows = cur.fetchall()

        cur.execute(
            """
            SELECT
                AVG(latitude) AS latitude,
                AVG(longitude) AS longitude,
                COUNT(*) AS listing_count
            FROM annunci
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND titolo IS NOT NULL
              AND prezzo IS NOT NULL
              AND url IS NOT NULL
              AND url_immagine IS NOT NULL
              AND tipo = %s
            """,
            (normalized_tipo,),
        )
        summary_row = cur.fetchone()

        positions = []
        if summary_row and summary_row["listing_count"]:
            positions.append(
                {
                    "id": "tutta-napoli",
                    "kind": "all",
                    "label": DEFAULT_LOCATION_LABEL,
                    "latitude": float(summary_row["latitude"]),
                    "longitude": float(summary_row["longitude"]),
                    "listing_count": int(summary_row["listing_count"]),
                }
            )

        for row in zone_rows:
            positions.append(
                {
                    "id": slugify_label(row["label"]),
                    "kind": "zone",
                    "label": row["label"],
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "listing_count": int(row["listing_count"]),
                }
            )

        return positions
    finally:
        put_conn(conn)


@app.get("/annunci/prossimo")
def prossimo_annuncio(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    tipo: str = "casa",
    prezzo_max: int | None = None,
    stanze_min: int | None = None,
    superficie_min: int | None = None,
    zona_query: str | None = None,
    solo_arredato: bool = False,
    solo_ascensore: bool = False,
    solo_terrazzo: bool = False,
    solo_posto_auto: bool = False,
    center_lat: float | None = None,
    center_lng: float | None = None,
    radius_km: float | None = None,
    exclude_ids: str | None = None,
):
    normalized_tipo = normalize_listing_type(tipo)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        where_clauses = [
            "a.tipo = %s",
            "a.titolo IS NOT NULL",
            "a.prezzo IS NOT NULL",
            "a.url IS NOT NULL",
            "a.url_immagine IS NOT NULL",
            "NOT EXISTS (SELECT 1 FROM visti v WHERE v.utente_id = %s AND v.annuncio_id = a.id)",
        ]
        params = [normalized_tipo, current_user["id"]]
        excluded_listing_ids = parse_exclude_ids(exclude_ids)
        distance_params = []
        outer_params = []
        outer_where_clauses = []
        distance_sql = "NULL::DOUBLE PRECISION"
        order_by = "filtered_annunci.created_at DESC NULLS LAST"

        if excluded_listing_ids:
            where_clauses.append("NOT (a.id = ANY(%s))")
            params.append(excluded_listing_ids)

        if prezzo_max is not None:
            where_clauses.append("a.prezzo <= %s")
            params.append(prezzo_max)

        if stanze_min is not None:
            where_clauses.append("COALESCE(a.stanze, 0) >= %s")
            params.append(stanze_min)

        if superficie_min is not None:
            where_clauses.append("COALESCE(a.superficie, 0) >= %s")
            params.append(superficie_min)

        if zona_query and zona_query.strip():
            query = f"%{zona_query.strip()}%"
            where_clauses.append("""
                (
                    COALESCE(a.microzona, '') ILIKE %s
                    OR COALESCE(a.macrozona, '') ILIKE %s
                    OR COALESCE(a.zona, '') ILIKE %s
                    OR COALESCE(a.titolo, '') ILIKE %s
                )
            """)
            params.extend([query, query, query, query])

        if solo_arredato:
            where_clauses.append("COALESCE(a.arredato, FALSE) = TRUE")

        if solo_ascensore:
            where_clauses.append("COALESCE(a.ascensore, FALSE) = TRUE")

        if solo_terrazzo:
            where_clauses.append("COALESCE(a.terrazzo, FALSE) = TRUE")

        if solo_posto_auto:
            where_clauses.append("COALESCE(a.posto_auto, FALSE) = TRUE")

        if center_lat is not None and center_lng is not None:
            distance_sql = build_distance_expression("a")
            distance_params = [center_lat, center_lng, center_lat]
            where_clauses.append("a.latitude IS NOT NULL")
            where_clauses.append("a.longitude IS NOT NULL")
            order_by = "filtered_annunci.distance_km ASC NULLS LAST, filtered_annunci.created_at DESC NULLS LAST"

            if radius_km is not None and radius_km > 0:
                outer_where_clauses.append("distance_km <= %s")
                outer_params.append(radius_km)

        outer_where = ""
        if outer_where_clauses:
            outer_where = f"WHERE {' AND '.join(outer_where_clauses)}"

        query = f"""
            SELECT *
            FROM (
                SELECT
                    a.id,
                    a.titolo,
                    a.prezzo,
                    a.stanze,
                    a.superficie,
                    a.arredato,
                    a.ascensore,
                    a.terrazzo,
                    a.posto_auto,
                    a.url,
                    a.url_immagine,
                    a.url_immagini,
                    a.descrizione,
                    a.zona,
                    a.macrozona,
                    a.microzona,
                    a.latitude,
                    a.longitude,
                    a.tipo,
                    {distance_sql} AS distance_km,
                    a.created_at
                FROM annunci a
                WHERE {" AND ".join(where_clauses)}
            ) filtered_annunci
            {outer_where}
            ORDER BY {order_by}
            LIMIT 1
        """
        cur.execute(query, distance_params + params + outer_params)
        row = cur.fetchone()
        if row is None:
            return None
        descrizione = queue_description_refresh(background_tasks, row["id"], row["descrizione"])
        return serialize_listing(row, descrizione)
    finally:
        put_conn(conn)


@app.post("/annunci/{annuncio_id}/like")
def like_annuncio(annuncio_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO visti (utente_id, annuncio_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (current_user["id"], annuncio_id),
        )
        upsert_preferito(cur, current_user["id"], annuncio_id, "like")
        conn.commit()
        return {"ok": True}
    finally:
        put_conn(conn)


@app.post("/annunci/{annuncio_id}/superlike")
def superlike_annuncio(annuncio_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO visti (utente_id, annuncio_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (current_user["id"], annuncio_id),
        )
        upsert_preferito(cur, current_user["id"], annuncio_id, "superlike")
        conn.commit()
        return {"ok": True}
    finally:
        put_conn(conn)


@app.post("/annunci/{annuncio_id}/skip")
def skip_annuncio(annuncio_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO visti (utente_id, annuncio_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (current_user["id"], annuncio_id),
        )
        conn.commit()
        return {"ok": True}
    finally:
        put_conn(conn)


# --- Preferiti ---

@app.get("/preferiti")
def get_preferiti(current_user: dict = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
                a.id,
                a.titolo,
                a.prezzo,
                a.stanze,
                a.superficie,
                a.arredato,
                a.ascensore,
                a.terrazzo,
                a.posto_auto,
                a.url,
                a.url_immagine,
                a.url_immagini,
                a.descrizione,
                a.zona,
                a.macrozona,
                a.microzona,
                a.latitude,
                a.longitude,
                a.tipo,
                NULL::DOUBLE PRECISION AS distance_km,
                p.decision
            FROM annunci a
            JOIN preferiti p ON a.id = p.annuncio_id
            WHERE p.utente_id = %s
              AND a.titolo IS NOT NULL
              AND a.prezzo IS NOT NULL
              AND a.url IS NOT NULL
              AND a.url_immagine IS NOT NULL
            ORDER BY p.created_at DESC NULLS LAST
        """, (current_user["id"],))
        rows = cur.fetchall()
        return [serialize_preferito(row) for row in rows]
    finally:
        put_conn(conn)


@app.delete("/preferiti/{annuncio_id}")
def remove_preferito(annuncio_id: int, current_user: dict = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM preferiti WHERE utente_id = %s AND annuncio_id = %s",
            (current_user["id"], annuncio_id),
        )
        cur.execute(
            "DELETE FROM visti WHERE utente_id = %s AND annuncio_id = %s",
            (current_user["id"], annuncio_id),
        )
        conn.commit()
        return {"ok": True}
    finally:
        put_conn(conn)
