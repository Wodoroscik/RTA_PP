from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='anomaly-event-time-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)


for message in consumer:
    tx = message.value
    u_id = tx['user_id']
    
    # 1. Konwersja tekstu ISO na obiekt datetime, a potem na sekundy (timestamp)
    # Przykład: "2023-10-27T10:00:00" -> 1698393600.0
    event_time = datetime.fromisoformat(tx['timestamp']).timestamp()
    
    user_history[u_id].append(event_time)
    
    user_history[u_id] = [t for t in user_history[u_id] if event_time - t <= 60]
    
    transaction_count = len(user_history[u_id])
    
    if transaction_count > 3:
        print(f"ALERT (Event Time): {u_id}")
        print(f"Wykonał {transaction_count} transakcji w 60s.")
        print(f"Czas ostatniej transakcji: {tx['timestamp']}\n")