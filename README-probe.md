# Prova immobiscraper

## Configurazione attuale: URL identico a quello dell'utente

La ricerca predefinita ora usa letteralmente il link fornito dall'utente:
affitto residenziale, arredato, italiano, raggio 5 km dal punto
`40.87414,14.34105`, pagina 1. Nessun parametro prezzoMassimo, criterio,
ordine o idNazione aggiunto. Il portale applica il suo ordinamento predefinito.
Per le pagine successive cambia solo `pag`; gli annunci non vengono filtrati
localmente per prezzo, piano o distanza.

`compare_searches.py` ora confronta il link esatto con la configurazione
del codice, visitando tutte le pagine collegate (massimo dieci). Verifica URL,
conteggio completo, ID, prezzi e ordine. Il risultato attuale va in
`reports/search-comparison-exact.json`; i vecchi report restano storici.

## Confronto con il link del sito

```powershell
.venv/Scripts/python.exe compare_searches.py --browser "$env:LOCALAPPDATA\ms-playwright\chromium-1243\chrome-win64\chrome.exe"
```

Nel precedente confronto confrontavamo il link fornito dall'utente, l'URL configurato e il link utente con
prezzo e ordinamento allineati. Legge le pagine collegate dalla paginazione,
con limite di dieci pagine per ricerca, e confronta gli ID delle schede.
Salva il report `reports/search-comparison.json`, HTML e screenshot.
`reports/search-comparison.md` contiene il resoconto della verifica del
15 ottobre 2026, con tutti gli annunci e la discrepanza residua di un ID.
Il resoconto Markdown e una fotografia della verifica, non viene rigenerato
automaticamente dal comando. Le ricerche non sono state modificate.

## Configurazione precedente: raggio di 5 km e massimo 700 euro

Ripristinata la configurazione del bot originale, confermata dalla cronologia
Git e dalla ricerca salvata in `immobiliare.json`: centro `40.87414, 14.34105`,
raggio 5000 metri, affitto residenziale, massimo 700 euro di canone,
filtro arredato, annunci ordinati dal piu recente. Le impostazioni condivise
sono in `search_config.py`. Il filtro arredato non certifica un inventario completo.

```powershell
.venv/Scripts/python.exe probe_access.py
```

Questo comando cerca entro il raggio, senza restringere ai comuni di Volla,
Cercola e Napoli/Ponticelli. `--area` permette ancora le vecchie prove per zona;
`--area all` esegue sia la ricerca nel raggio sia le tre vecchie ricerche.
I report aggiungono la distanza in linea d'aria dal centro al punto pubblicato
dell'annuncio. Non e una distanza stradale: con `location_marker: only_area`
il punto e approssimativo; in assenza di coordinate la distanza resta null.
Il filtro geografico viene applicato dal portale; il calcolo locale e informativo.

Verifica del 15 ottobre 2026: il browser ha mostrato "5 Km da Viale Vesuvio,
122 - Volla" e 12 risultati, con 12 link diretti estratti. Su questa rotta
`curl_cffi` restituisce HTTP 200 ma solo il contenitore della ricerca: serve
il browser per caricare i risultati JavaScript. Il report distingue questa
risposta con `access_unverified`. In questa pagina i dati strutturati degli
annunci non sono nell'HTML iniziale: le distanze si ricavano dai dettagli
quando questi pubblicano le coordinate.

## Esito delle prove aggiuntive: accesso riuscito

Il 15 ottobre 2026 `curl_cffi` con profilo Chrome ha letto le tre ricerche
con HTTP 200: due risultati a Volla, zero a Cercola, uno a Ponticelli.
I conteggi sono estratti dai dati strutturati della pagina, non dedotti
dall'assenza di link. Tutte le ricerche avevano al massimo una pagina.
Le due pagine di dettaglio provate con `curl_cffi` hanno restituito 403.

`nodriver` con Chromium visibile, gia presente sul PC, ha invece caricato
la ricerca Ponticelli e i dettagli `106839665` e `133239930`, senza
intervento manuale, account, proxy o servizi a pagamento. Il riferimento
a DataDome e presente anche nelle pagine valide: da solo non indica un blocco.
Questa e una prova limitata, non una garanzia di accesso continuativo.

```powershell
.venv/Scripts/python.exe probe_access.py --transport curl --area all
.venv/Scripts/python.exe probe_access.py --transport nodriver --area ponticelli --listing-id 133239930 --browser "$env:LOCALAPPDATA\ms-playwright\chromium-1243\chrome-win64\chrome.exe"
```

Il percorso del browser va adattato se la versione installata cambia.
`probe_access.py` legge i dati strutturati attuali senza il parser vendite
del progetto originale. Non esegue paginazione automatica: con `--listing-id`
legge un solo dettaglio; senza, legge la prima pagina della ricerca.
Salva HTML e report JSON in `reports/`, insieme allo screenshot piu recente
`nodriver-page.png`. Il browser usa il profilo dedicato `reports/browser-profile`.
Il codice di uscita 1 indica un errore di esecuzione; per blocchi o contenuti
non riconosciuti controllare anche `status: access_unverified` nel JSON.

Nei dettagli verificati, Ponticelli dichiara arredamento completo ma costa
700 euro + 25 euro di condominio, prima delle utenze. Volla costa 700 euro,
e arredato ma ad uso transitorio, piano rialzato; spese non specificate.
Nessuno dei due e confermato entro 700 euro tutto incluso. L'inventario
dei mobili e il totale mensile restano null nei dati estratti.

## Esito iniziale con requests

Il 15 ottobre 2026 la prima pagina delle ricerche affitto per Volla, Cercola
e Napoli/Ponticelli, con canone massimo 700 euro, ha restituito **HTTP 403**
con riferimenti a **DataDome** in tutti e tre i casi. Nessun annuncio recuperato.
Questo risultato riguarda la connessione e il momento della prova; non dimostra
che non ci siano case disponibili o che gli URL siano stati convalidati dal sito.

## Ripetere la prova (PowerShell)

```powershell
git clone --depth 1 https://github.com/Stemanz/immobiscraper.git vendor/immobiscraper
python -m venv --without-pip .venv
python -m pip --python .venv/Scripts/python.exe install -r requirements-probe.txt
.venv/Scripts/python.exe probe_immobiscraper.py
```

I primi tre comandi servono soltanto alla preparazione iniziale, gia effettuata
in questa cartella. Per una sola zona e senza richieste ai dettagli:

```powershell
.venv/Scripts/python.exe probe_immobiscraper.py --area cercola --max-details 0
```

L'esito viene scritto in `reports/immobiscraper-probe.json` (sovrascritto a ogni
esecuzione). Codice di uscita 1 in caso di errori HTTP/rete o parsing. Zero link
estratti non equivale a zero immobili disponibili.

## Limiti

- Sorgente originale separata in `vendor/immobiscraper`, commit `fbbda8c`
  del 22 maggio 2023. Lo script usa il suo estrattore di link e il suo parser,
  aggiungendo timeout e controlli HTTP. Non avvia `main.py` e non invia messaggi.
- Una pagina per zona, al massimo tre dettagli per zona, nessun tentativo di
  superare CAPTCHA o protezioni. Si ferma al primo errore della singola zona.
- Il parser originale nasce per vendite: cerca prezzi con separatori delle
  migliaia e imposta una soglia minima di 10.000 euro. La prova azzera la soglia,
  ma non corregge le espressioni dei prezzi. Anche con accesso riuscito servira
  adattarlo agli affitti e verificare i valori estratti.
- Il limite 700 euro riguarda il canone richiesto, non il totale con utenze e
  condominio. Arredamento completo, camere separate e piano non sono verificati.
- `vendor`, `.venv` e `reports` sono esclusi da Git.
