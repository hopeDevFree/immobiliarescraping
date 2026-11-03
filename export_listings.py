import json
import time
import urllib.request
import psycopg2
from psycopg2.extras import RealDictCursor

# Incolla qui la tua stringa di connessione Supabase (Pooler)
DATABASE_URL = "postgresql://postgres.vbvbzuuxjnqzlnpgkvxm:immobiliarescraping1@aws-0-eu-west-3.pooler.supabase.com:5432/postgres"


def get_description_via_jina(url):
    if not url or not url.startswith("http"):
        return "Link non disponibile."
    jina_url = f"https://r.jina.ai/{url}"
    headers = {"User-Agent": "Mozilla/5.0", "X-No-Cache": "true"}
    try:
        req = urllib.request.Request(jina_url, headers=headers)
        content = urllib.request.urlopen(req, timeout=20).read().decode("utf-8")
        # Jina restituisce già l'intera pagina in Markdown pulito
        return content.strip()
    except Exception as e:
        return f"Descrizione non recuperabile: {e}"


def export():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    with conn.cursor() as cur:
        cur.execute("SELECT id, payload FROM listings WHERE active = 1 ORDER BY id DESC")
        rows = cur.fetchall()
    conn.close()

    total = len(rows)
    print(f"Estratti {total} annunci. Inizio download descrizioni (richiede ~1-2 minuti)...")

    with open("annunci_completi_ai.txt", "w", encoding="utf-8") as f:
        f.write("# REPORT COMPLETO ANNUNCI ATTIVI CON DESCRIZIONI\n\n")

        for i, r in enumerate(rows, start=1):
            data = json.loads(r["payload"])
            source = data.get("source", "immobiliare").upper()
            title = data.get("title", "Senza titolo")
            price = data.get("price") or data.get("rent_eur") or "N/D"
            surface = data.get("surface", "N/D")
            rooms = data.get("rooms", "N/D")
            url = data.get("url")

            print(f"[{i}/{total}] Scarico descrizione: {title[:45]}...")
            description = get_description_via_jina(url)

            f.write("=" * 60 + "\n")
            f.write(f"ANNUNCIO #{i} [{source}] - ID: {r['id']}\n")
            f.write(f"Titolo: {title}\n")
            f.write(f"Prezzo: {price} €/mese | Superficie: {surface} m² | Locali: {rooms}\n")
            f.write(f"Link: {url}\n\n")
            f.write("TESTO E DETTAGLI DELLA SCHEDA:\n")
            f.write(description + "\n\n")

            # Pausa di 1 secondo per non subire il blocco 429 di Jina
            time.sleep(1)

    print("\n✅ File 'annunci_completi_ai.txt' creato con successo!")


if __name__ == "__main__":
    export()
