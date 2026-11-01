import time
import random
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
import sys

# Configuriamo 3 escursionisti fittizi con ID univoci
HIKERS = [
    {"id": str(uuid.uuid4()), "lat": 46.3782, "lon": 13.8366, "alt": 2100}, # Coordinate base (es. Alpi Giulie)
    {"id": str(uuid.uuid4()), "lat": 46.3800, "lon": 13.8310, "alt": 2150},
    {"id": str(uuid.uuid4()), "lat": 46.3750, "lon": 13.8400, "alt": 2050},
]

def update_hiker_state(hiker):
    """
    Simula il movimento dell'escursionista modificando leggermente 
    le sue coordinate, l'altitudine e generando parametri vitali.
    """
    # Random walk: spostamento casuale molto piccolo
    hiker["lat"] += random.uniform(-0.0005, 0.0005)
    hiker["lon"] += random.uniform(-0.0005, 0.0005)
    
    # L'altitudine varia in base al terreno simulato
    hiker["alt"] += random.uniform(-2.0, 2.0)
    
    # Generiamo un battito cardiaco da sforzo (90-160 bpm)
    heart_rate = int(random.gauss(125, 10)) 
    
    return {
        "hiker_id": hiker["id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # Formato GeoJSON: attenzione, l'ordine DEVE essere [Longitudine, Latitudine]
        "location": {
            "type": "Point",
            "coordinates": [round(hiker["lon"], 6), round(hiker["lat"], 6)]
        },
        "metrics": {
            "altitude_m": round(hiker["alt"], 1),
            "heart_rate_bpm": heart_rate,
            "device_battery_pct": random.randint(10, 100)
        }
    }
def delivery_report(err, msg):
    """ Callback chiamata quando il messaggio viene consegnato o fallisce """
    if err is not None:
        print(f"Errore consegna messaggio: {err}")
    else:
        print(f"Messaggio inviato a {msg.topic()} - Partizione [{msg.partition()}]")

def main():
    print("Inizializzazione Producer Kafka...")
    
    # Configurazione del producer verso il nostro container Docker
    producer_config = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(producer_config)
    
    topic_name = "telemetry_data"

    try:
        while True:
            active_hiker = random.choice(HIKERS)
            payload = update_hiker_state(active_hiker)
            json_payload = json.dumps(payload)
            
            # Invia il messaggio a Kafka
            producer.produce(
                topic_name, 
                value=json_payload.encode('utf-8'), 
                callback=delivery_report
            )
            
            # Serve per svuotare le code interne del producer
            producer.poll(0)
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nSimulazione interrotta. Chiusura connessione...")
    finally:
        # Assicura che tutti i messaggi in coda vengano inviati prima di uscire
        producer.flush()

if __name__ == "__main__":
    main()