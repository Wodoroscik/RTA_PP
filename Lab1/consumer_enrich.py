from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrichment-group', # Unikalne ID grupy dla tego zadania, aby Kafka nie dzieliła pracy między dwóch consumerów
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    amount = tx['amount']
    
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    tx['risk_level'] = risk_level
    
    print(f"ID: {tx['tx_id']} | Kwota: {tx['amount']} | Ryzyko: {tx['risk_level']}")