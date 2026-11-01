import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# 1. Configurazione della connessione a MongoDB (in esecuzione sul container locale)
MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mountain_rescue_db"]       # Nome del database NoSQL
collection = db["telemetry_stream"]            # Nome della collezione (simile a una tabella)

# 2. Configurazione del Consumer Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'telemetry-consumer-group',     # Identificativo del gruppo di consumer
    'auto.offset.reset': 'earliest'             # Legge i messaggi dall'inizio se non c'è uno storico salvato
}

def main():
    consumer = Consumer(consumer_config)
    
    # Ci iscriviamo allo stesso topic su cui scrive il simulatore
    topic_name = "telemetry_data"
    consumer.subscribe([topic_name])
    
    print(f"In ascolto sul topic Kafka '{topic_name}'... (Premi CTRL+C per uscire)")
    
    try:
        while True:
            # Attendiamo un messaggio da Kafka (timeout di 1 secondo)
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Errore Kafka: {msg.error()}")
                    break
            
            # Estraiamo e decodifichiamo il JSON ricevuto
            raw_data = msg.value().decode('utf-8')
            telemetry_document = json.loads(raw_data)
            
            # 3. Salvataggio diretto nel database NoSQL MongoDB
            # Nota quanto sia naturale: passiamo un dizionario Python/JSON senza definire colonne rigide
            inserted_id = collection.insert_one(telemetry_document).inserted_id
            
            print(f"[SAVED TO MONGODB] ID documento: {inserted_id} | Hiker: {telemetry_document['hiker_id'][:8]}...")
            
    except KeyboardInterrupt:
        print("\nInterrotto dall'utente. Chiusura consumer...")
    finally:
        # Chiudiamo in modo pulito la connessione a Kafka
        consumer.close()

if __name__ == "__main__":
    main()