# src/producer.py
import time
import json
from kafka import KafkaProducer
from random import randint, choice

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Types de sinistres simulés
types_sinistres = ["accident", "vol", "incendie","maladie","accident de travail"]

try:
    i = 1
    while True:
        # Créer un événement aléatoire
        event = {
            "id": i,
            "type": choice(types_sinistres),
            "montant": randint(100, 10000)
        }
        # Envoyer l'événement au topic
        producer.send("sinistres", event)
        print(f"Envoyé : {event}")
        producer.flush()  # force l'envoi
        i += 1
        time.sleep(1)  # 1 message par seconde
except KeyboardInterrupt:
    print("Producer arrêté manuellement")
finally:
    producer.close()