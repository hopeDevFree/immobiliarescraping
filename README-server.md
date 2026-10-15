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
La lettura di pagina usa `document.documentElement.outerHTML`, evitando la
materializzazione dell'intero albero DOM via CDP, con timeout di 25 secondi
per lettura e limite complessivo di scansione di 120 secondi. La scansione
completa del container corretto, sempre con 512 MB e 0.5 CPU, ha letto tutti
i 27 annunci delle due pagine. Il container di prova e terminato.
La lettura degli annunci dall'IP Render resta da verificare sul servizio live:
al controllo successivo ai push rispondeva ancora la versione precedente
senza i campi `revision` e `display`.

## Esito sul servizio Render

Il 15 ottobre 2026 la revisione `13c5770` e stata verificata online:
Chromium si avvia e lo scheduler e vivo, ma il sito restituisce un JSON con
un URL `geo.captcha-delivery.com/captcha/` anziche i risultati. Il servizio
ha zero annunci e nessuna scansione riuscita. Questo e un blocco DataDome
osservato dal server, non una prova di assenza di immobili.

Il riconoscimento del JSON CAPTCHA ora produce `SEARCH_BLOCKED` con motivo
`datadome_captcha`, interrompe subito la lettura e non registra gli identificativi
temporanei del CAPTCHA. L'intervallo richiesto resta 180 secondi e gli ultimi
dati validi vengono conservati. Questa gestione dell'errore non risolve il blocco.
Tutti i 15 test passano, inclusa la riproduzione della risposta vista su Render.

## Confronto gratuito dei metodi di accesso

All'avvio il worker esegue una matrice finita di prove, visibile in sola lettura
su `/diagnostics`: sei GET HTTP fra pagina originale e API JSON pubbliche,
poi lettura della stessa API tramite Jina Reader anonimo e varianti browser
con scheda gia aperta, passaggio dalla home e avvio
di Chromium prima del collegamento CDP. I parametri della ricerca restano quelli
del link originale. Un solo tentativo API aggiunge `path=/search-list/`, parametro
tecnico di instradamento, dichiarato nel resoconto.

Ogni GET ha timeout di 15 secondi; ogni variante browser ha un budget proprio.
La prima scansione puo quindi richiedere diversi minuti. Le prove sono
sequenziali e il server HTTP continua a rispondere. Un metodo e selezionato solo
dopo aver letto tutte le pagine e verificato il totale degli annunci. Alle
scansioni successive viene riutilizzato quel metodo ogni 180 secondi.
Se nessuno funziona, la matrice non viene ripetuta a ogni intervallo: si riprova
una sola navigazione browser, conservando il profilo della variante.

Se l'accesso diretto non funziona, Jina Reader legge le stesse pagine API senza
account o chiavi. Ogni richiesta invia `X-No-Cache: true`, evitando la cache del
lettore. Si verificano anche qui tutte le pagine, totale e ID univoci. Un metodo
HTTP selezionato che in seguito fallisce puo passare al Reader senza ripetere
l'intera matrice. [Uso gratuito e limiti del Reader](https://jina.ai/reader/).

Il resoconto contiene esiti, status HTTP, ID annunci e pochi header consentiti;
non contiene cookie, credenziali o identificativi CAPTCHA. Non risolve CAPTCHA
e non usa servizi a pagamento. `state: working` nel resoconto va sempre
confrontato con `last_scan.ok: 1` in `/health`, che conferma anche il salvataggio.

Verifica automatica della matrice e della conservazione dei dati:

```powershell
.venv/Scripts/python.exe -m unittest test_service test_access_http test_access_reader test_access_strategy -v
```

Verifica del 15 ottobre 2026: 36 test passati. L'API diretta con parametro
tecnico `path=/search-list/` ha letto tutti i 27 annunci nel container da 512 MB
e 0.5 CPU. ID e prezzi corrispondono al confronto completo con il sito;
l'ordine di alcuni annunci cambia. Jina Reader ha restituito gli stessi 27 ID
e prezzi su due pagine. La prova Linux del Reader, con cache disattivata,
ha completato selezione del metodo e salvataggio SQLite in circa quattro secondi.
Le varianti browser diretta e con collegamento ritardato hanno letto 27 annunci;
quella passando dalla home ha esaurito il primo budget di 70 secondi con risposte
HTTP 200. Le prove browser ora hanno tutte un budget di 120 secondi.
Queste sono verifiche dal container locale: l'accesso dal servizio Render
deve ancora essere controllato sulla nuova revisione. Al controllo delle 10:59
UTC era ancora attiva `584c42d`, senza le nuove strategie.

Alle 11:14 UTC la revisione `c177182` e stata controllata su Render:
la matrice ha rilevato CAPTCHA HTTP 403 sugli accessi diretti e blocchi
DataDome in tutte e tre le varianti browser. Il vecchio endpoint `real-estates`
ha risposto 404. Nessuna scansione sul server e riuscita con quella revisione.
La successiva integrazione del Reader deve essere verificata sul servizio live.

Su Windows specificare `CHROME_EXECUTABLE` prima di eseguire `main.py`.
Per provare il container su un host Docker:

```sh
docker build -t immobiliare .
docker run --rm -p 10000:10000 -v immobiliare-data:/app/data immobiliare
```
