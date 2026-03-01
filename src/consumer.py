import json
import psycopg2
from kafka import KafkaConsumer

# Connexion PostgreSQL
conn = psycopg2.connect(
    host="172.18.0.4",
    database="sinistres_db",
    user="admin",
    password="admin"
)
cur = conn.cursor()

# Connexion Kafka
consumer = KafkaConsumer(
    'sinistres',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # lire depuis le début si nécessaire
    enable_auto_commit=False,      # commit manuel après insertion
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer démarré...")

try:
    for message in consumer:
        data = message.value
        try:
            cur.execute(
                """
                INSERT INTO staging_sinistres (sinistre_id, type, montant)
                VALUES (%s, %s, %s)
                """,
                (data["id"], data["type"], data["montant"])
            )
            conn.commit()
            # Commit Kafka offset après insertion
            consumer.commit()
            print(f"Inséré dans DB : {data}")
        except Exception as e:
            print(f"Erreur insertion DB : {e}")
            conn.rollback()
except KeyboardInterrupt:
    print("Consumer arrêté manuellement")
finally:
    cur.close()
    conn.close()
    consumer.close()