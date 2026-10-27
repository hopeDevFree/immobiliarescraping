import json
import re
import urllib.request

subito_url = "https://www.subito.it/annunci-locali/affitto/appartamenti/?rad=5000&lat=40.87414&lon=14.34105&fr=true"
jina_url = f"https://r.jina.ai/{subito_url}"

headers = {
    "User-Agent": "Mozilla/5.0",
    "X-No-Cache": "true",
    "X-Return-Format": "html",
}

req = urllib.request.Request(jina_url, headers=headers)
html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8")

match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
if not match:
    print("Blocco JSON non trovato. Anteprima risposta ricevuta:\n")
    print(html[:400])
    exit()

data = json.loads(match.group(1))
items_state = data.get("props", {}).get("pageProps", {}).get("initialState", {}).get("items", {})
ads = items_state.get("originalList") or items_state.get("list") or []

print(f"✅ Successo! Trovati {len(ads)} annunci su Subito:\n")
for ad in ads[:5]:
    title = ad.get("subject")
    url_ad = ad.get("urls", {}).get("default")
    advertiser = ad.get("advertiser", {}).get("type")  # "private" o "company"
    features = ad.get("features", {})
    price = features.get("/price", {}).get("values", [{}])[0].get("key")
    print(f"- [{advertiser}] {title} -> €{price}")
    print(f"  {url_ad}\n")