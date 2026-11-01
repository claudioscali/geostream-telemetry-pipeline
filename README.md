# 🏔️ GeoStream Telemetry Pipeline: Mountain Rescue Hub

Una pipeline Big Data end-to-end per l'ingestione, l'elaborazione e la visualizzazione in tempo reale di dati telemetrici e geospaziali (GeoJSON). 

Il progetto simula un sistema di monitoraggio per il soccorso alpino: riceve dati GPS e parametri vitali da dispositivi indossabili di escursionisti, li gestisce tramite streaming distribuito e li espone su una dashboard interattiva basata su mappa.

## 🎯 Il Problema Risolto (Perché questa architettura?)

Nei sistemi IoT (Internet of Things) moderni, i sensori generano una mole di dati caratterizzata da altissima **Velocità** (aggiornamenti continui al secondo) e **Varietà** (payload JSON che possono cambiare nel tempo).

Utilizzare un classico database relazionale con un'API diretta (es. Sensore -> REST API -> MySQL) creerebbe colli di bottiglia e rigidità. Questo progetto risolve il problema implementando un'architettura **Event-Driven** e **NoSQL**:

1. **Disaccoppiamento e Gestione dei Picchi:** Apache Kafka si interpone tra i sensori e il database funzionando da "ammortizzatore". Se 100.000 escursionisti inviano dati contemporaneamente, Kafka accoda i messaggi e il database li assorbe al suo ritmo, evitando crash di sistema.
2. **Schema Flessibile (NoSQL):** MongoDB salva i dati come documenti BSON/JSON. Se domani aggiungiamo un sensore di "temperatura esterna", non dobbiamo fare complesse `ALTER TABLE` o bloccare il database: il nuovo campo viene semplicemente salvato.
3. **Location Intelligence:** Sfruttando lo standard GeoJSON nativo di MongoDB e gli indici geospaziali (`2dsphere`), le ricerche di prossimità (es. *"Chi è nel raggio di 2km?"*) vengono eseguite a livello database con complessità algoritmica ottimizzata, invece di calcolare le distanze in memoria sul backend.

## ⚙️ Come Funziona il Flusso Dati (Architettura)

L'architettura è suddivisa in 4 micro-componenti principali:

1. **Producer (`sensor_simulator.py`):** Uno script Python simula gli escursionisti in movimento tramite algoritmi di *random walk*. Genera payload JSON contenenti coordinate (GeoJSON), altitudine e battito cardiaco, e li spara sul topic Kafka `telemetry_data`.
2. **Message Broker (Apache Kafka):** Containerizzato via Docker, riceve il flusso di dati. Utilizza KRaft (Kafka Raft) per la gestione del cluster, garantendo alta disponibilità e persistenza dei messaggi.
3. **Consumer (`sensor_consumer.py`):** Un processo Python indipendente in costante ascolto su Kafka. Estrae i messaggi e li "inietta" nel database NoSQL.
4. **Backend REST & Dashboard (`app.py`):** Un server Flask che interroga MongoDB usando *Aggregation Pipelines* e query spaziali (`$near`). Espone i dati a un frontend HTML/JS (Bootstrap + Leaflet.js) che fa polling ogni 3 secondi, aggiornando dinamicamente la mappa e le tabelle senza ricaricare la pagina.

## 🛠️ Stack Tecnologico

* **Linguaggio:** Python 3
* **Message Broker:** Apache Kafka (Confluent Distribution 7.7.0, KRaft mode)
* **Database:** MongoDB 7.0 (NoSQL Documentale)
* **Backend Web:** Flask, PyMongo, Confluent-Kafka
* **Frontend:** HTML5, Bootstrap 5, Vanilla JS, Leaflet.js (Mappe)
* **Infrastruttura:** Docker & Docker Compose

## 🚀 Requisiti e Installazione

1. Assicurati di avere [Docker Desktop](https://www.docker.com/products/docker-desktop/) (con WSL2 se su Windows) e Python 3.9+ installati.
2. Clona il repository e posizionati nella cartella del progetto.
3. Crea un virtual environment e installa le dipendenze:
   ```bash 
   python -m venv .venv
   source .venv/bin/activate  # Su Windows: .venv\Scripts\Activate
   pip install -r requirements.txt
   ```
## 🏃‍♂️ Come Avviare l'Infrastruttura

Per far partire l'intera pipeline in locale, sono necessari 4 step da eseguire in terminali separati:

**1. Avvia Kafka e MongoDB (Background):**

```bash
docker compose up -d
```

Attendi qualche secondo che i container siano marcati come `Up`. Crea l'indice geospaziale accedendo a mongo con `docker exec -it prog_nosql-mongodb-1 mongosh` e digitando `db.telemetry_stream.createIndex({ location: "2dsphere" })`.

**2. Avvia l'API e la Dashboard Web:**

```bash
python app.py
```

*(L'interfaccia sarà disponibile su http://127.0.0.1:5000)*

**3. Avvia il Consumer (Ascolto):**
In un nuovo terminale, lancia:

```bash
python sensor_consumer.py
```

**4. Avvia il Data Generator (Streaming):**
In un ulteriore terminale, avvia la simulazione dei sensori:

```bash
python sensor_simulator.py
```

Non appena il simulatore parte, vedrai i dati attraversare Kafka, atterrare su MongoDB e apparire magicamente in tempo reale sulla mappa del browser!

## 📡 API Endpoints

Il backend Flask espone i seguenti endpoint per l'integrazione di sistemi esterni:

* `GET /api/hikers/latest`
  Ritorna lo stato più recente (aggregato per ID) di tutti gli escursionisti attivi.
* `GET /api/hikers/nearby?lat={float}&lon={float}&max_distance={int}`
  Esegue una query spaziale GeoJSON su MongoDB per trovare dispositivi entro il raggio indicato (default: 5000 metri).