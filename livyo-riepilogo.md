# Livyo — Riepilogo progetto
*Generato dalla chat di avvio del progetto*

---

## 🎯 Idea

App immobiliare **mobile-first** con interfaccia swipe stile Tinder.
L'utente scorre gli annunci immobiliari come fossero card — swipe destra per salvare, sinistra per scartare.

**Differenziatori principali:**
- Nessun login richiesto
- Esperienza fluida e addictive (stile gioco)
- Personaggio AI mascotte chiamato **Livyo**
- Punto di partenza basato su GPS dell'utente
- Catalogo praticamente infinito (scraping + nuovi annunci ogni giorno)

---

## 📛 Nome

**Livyo** — nome inventato, globale, pronunciabile in qualsiasi lingua.
Il personaggio e l'app hanno lo stesso nome (come Alexa, Siri).
Ispirazione: l'app Olivia (AI nutrizionista).

---

## 💻 Stack tecnico

| Parte | Tecnologia |
|---|---|
| Frontend | React + Tailwind CSS |
| Backend | Node.js + Express |
| Scraper | Python + Playwright |
| Hosting | Vercel (frontend) + Railway (backend) |

---

## 🎨 Design

- Ispirato ad **Airbnb** — pulito, caldo, premium
- Colore primario: `#FF5A5F`
- Mobile-first (web app, non app nativa — niente App Store)
- Filtri in bottom sheet (non barra orizzontale)
- Tab vendita/affitto/tutti nell'header

---

## 🏠 Come funziona

1. L'utente apre l'app
2. Livyo (il personaggio) lo saluta
3. L'app chiede la posizione GPS o una città preferita
4. Mostra annunci in cerchi concentrici dalla posizione scelta
5. L'utente swipa — destra per salvare, sinistra per scartare, ⭐ per super interesse
6. Il catalogo non finisce mai (nuovi annunci ogni giorno)
7. Filtri disponibili: vendita/affitto, prezzo massimo, raggio km

---

## 🤖 Il personaggio — Livyo

Blob SVG animato, stile **flat cartoon**, completamente diverso da Kimi.

**Caratteristiche:**
- Forma blob organica che si deforma continuamente
- Occhi espressivi che sbattono le palpebre
- Pupille mobili che guardano nella direzione dello swipe
- Antennina in cima che vibra quando è excited
- Guance arrossate
- Bocca che cambia per ogni mood

**Mood:**
- 🔵 **Idle** — blu, respira piano, pupille che vagano
- 🟢 **Happy** (swipe destra) — verde, rimbalza, particelle, dentino
- 🔴 **Sad** (swipe sinistra) — rosso, si scuote, lacrimuccia
- 🟡 **Excited** — giallo, occhi spalancati, antennina vibra, particelle

**Filosofia — quasi sempre fermo:**
Si anima SOLO nei momenti che contano:
- Apertura app → saluto una volta sola
- Swipe destra → esulta brevemente
- Swipe sinistra → si scuote
- Ogni 10 swipe → "sto imparando i tuoi gusti"
- 30 secondi inattività → messaggio gentile
- Click su di lui → risponde con un prompt

**Posizione:** fisso in basso a destra, sopra la bottom nav.

**File:** `Livyo.jsx`

---

## 💰 Monetizzazione

1. **AdSense** — pubblicità per gli utenti (tutto gratis per loro)
2. **Freemium agenzie** — le agenzie pagano per:
   - Mettere annunci in evidenza
   - Vedere chi ha messo ⭐ Super interesse sui loro immobili

---

## 📊 Dati di partenza

- Canale Telegram esistente: **6.000+ iscritti** su annunci lavoro Ferrovie italiane
- Gruppo collegato: **7.000+ iscritti**
- ~1.000 visualizzazioni per annuncio
- Questo pubblico può essere usato come trampolino per i primi utenti di Livyo

---

## 🗺️ Roadmap

| Fase | Obiettivo |
|---|---|
| **MVP (2-3 settimane)** | Scraping + swipe UI + Livyo personaggio |
| **Lancio** | Promuovi sul canale Telegram esistente |
| **Crescita** | SEO organico, AdSense attivo |
| **Monetizzazione agenzie** | Pacchetti freemium |
| **Espansione** | Da Italia → Europa → Mondo |

---

## 📁 File prodotti

- `casaswipe.jsx` — prototipo UI swipe (versione Airbnb style)
- `Livyo.jsx` — personaggio mascotte blob animato v4

---

## 🧠 Decisioni prese

- ✅ Web app mobile-first (no App Store)
- ✅ Scraping da Immobiliare.it + Idealista
- ✅ Nessun login obbligatorio
- ✅ Nome: Livyo (non Livio, non Livy, non Livi)
- ✅ Personaggio blob flat cartoon (non glossy come Kimi)
- ✅ Personaggio quasi sempre fermo, si anima solo nei momenti chiave
- ✅ ⭐ Super interesse = gancio di monetizzazione verso agenzie
