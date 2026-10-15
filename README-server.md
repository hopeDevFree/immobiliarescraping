# Servizio Render: scansione ogni 3 minuti

Il Dockerfile avvia Chromium tramite Xvfb e un singolo processo Python con
server HTTP Waitress e un worker sequenziale. Prima scansione all'avvio;
poi una scansione ogni 180 secondi dall'inizio della precedente. Timeout
di lettura di 120 secondi. Le scansioni non si sovrappongono: dopo uno sforamento
si attende un intervallo intero. Non sono previsti tentativi immediati sui blocchi.

Chromium viene terminato e atteso alla fine di ogni scansione, anche quando
l'avvio fallisce: chiudere solo il websocket di nodriver non basta. La sessione
di debug viene riagganciata se Chromium la sostituisce durante la navigazione.
Su server con CPU limitata nodriver puo esaurire i suoi tentativi iniziali
prima che Chromium sia pronto. Il runtime attende fino a 30 secondi il debugger
dello stesso processo. Non rilancia un secondo browser sullo stesso profilo.
Gli errori di avvio includono display, codice di uscita e stderr di Chromium.

## Ricerca e dati

L'URL in `search_config.py` e esattamente quello dell'utente. La scansione
legge tutte le pagine collegate (massimo dieci), senza limiti di prezzo
o altri filtri locali. Il totale degli ID estratti deve corrispondere al totale
visualizzato. Se cambia durante la paginazione o la pagina viene bloccata,
la scansione fallisce e conserva gli ultimi dati validi.

Il database `DATA_DIR/listings.sqlite3` conserva gli annunci, primo e ultimo
avvistamento, modifiche e storico delle scansioni. La scomparsa dai risultati
significa che l'annuncio non compare piu nella ricerca, non che la casa sia
stata affittata. Questa versione non invia notifiche Telegram.

## Servizio esistente

Render: https://dashboard.render.com/web/srv-d25hi395pdvs73dkduh0

Repository collegato: `hopeDevFree/immobiliarescraping`, ramo locale `master`.
Il push avvia il deployment se il servizio usa questo ramo e l'auto-deploy e attivo.
Runtime richiesto: Docker. Docker Command deve usare il CMD del Dockerfile
(campo vuoto); un comando personalizzato `python main.py` salterebbe Xvfb.
Avviare una sola istanza del servizio.

Variabili predefinite nel container:

| Variabile | Valore |
|---|---|
| `SCRAPE_INTERVAL_SECONDS` | `180` |
| `CHROME_EXECUTABLE` | `/usr/bin/chromium` |
| `DATA_DIR` | `/app/data` |
| `PORT` | `10000` oppure valore fornito da Render |

Per mantenere lo storico attraverso i deployment serve un disco persistente
Render montato su `/app/data` (o `DATA_DIR` su un altro percorso scrivibile).
Senza disco i dati durano solo per la vita dell'istanza. Il profilo browser
e separato dal profilo personale. Non viene incluso nell'immagine.

Il piano Free di Render sospende il web service dopo inattivita HTTP: il
worker interno non garantisce funzionamento 24 ore su 24 su quel piano.
Questa implementazione non cambia il piano e non crea servizi a pagamento.

## Verifica dopo il deployment

Aprire `/health` sull'URL pubblico del servizio. `worker_alive: true` conferma
il worker, `interval_seconds: 180` l'intervallo. `last_scan.ok: 1` indica
una scansione completa; `last_success` deve avanzare dopo tre minuti.
`revision` riporta `RENDER_GIT_COMMIT` e `display` il display Xvfb, per
distinguere un deployment vecchio da quello corrente.
Un HTTP 200 da solo certifica che il worker e vivo, non l'accesso al portale:
controllare sempre anche l'esito della scansione. I log mostrano `SCAN_OK`
con totale/nuovi/modificati/usciti oppure `SCAN_FAILED` con l'errore.

## Verifica locale

```powershell
python -m pip --python .venv/Scripts/python.exe install -r requirements-server.txt
.venv/Scripts/python.exe -m unittest test_service -v
```

Verifica del 15 ottobre 2026: 10 test passati e due scansioni reali consecutive
con lo stesso profilo, entrambe con 27 annunci. La seconda ha registrato zero
nuovi annunci e zero modifiche. Lo scheduler locale di prova e stato fermato.
Successiva verifica Linux: immagine Docker costruita, errore di avvio riprodotto
con 512 MB di RAM e 0.5 CPU. Chromium era ancora vivo e stampava il debugger,
ma nodriver aveva gia terminato i tentativi. Con l'attesa estesa la stessa prova
ha caricato about:blank in circa 6 secondi. Tutti i 12 test passano.
La lettura degli annunci dall'IP Render resta da verificare sul servizio live.

Su Windows specificare `CHROME_EXECUTABLE` prima di eseguire `main.py`.
Per provare il container su un host Docker:

```sh
docker build -t immobiliare .
docker run --rm -p 10000:10000 -v immobiliare-data:/app/data immobiliare
```
