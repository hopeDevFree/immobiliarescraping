from access_reader import scan_reader

print("Interrogo Immobiliare tramite scan_reader()...")

try:
    # Prova prima con curl_cffi, altrimenti fallback su urllib
    rows = scan_reader(transport="curl")
except Exception:
    rows = scan_reader(transport="urllib")

print(f"\n✅ Trovati {len(rows)} annunci da Immobiliare!")
for r in rows[:3]:
    print("=" * 50)
    print(f"Titolo: {r.get('title')}")
    print(f"Prezzo: {r.get('rent_eur')} € | Superficie: {r.get('surface')} m² | Locali: {r.get('rooms')}")
    desc = r.get("description")
    print(f"Descrizione: {desc[:200] if desc else 'None'}")