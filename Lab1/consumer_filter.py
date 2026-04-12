from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        tx_id = tx['tx_id']
        amount = tx['amount']
        store = tx['store']
        category = tx['category']
        
        print(f"ALERT: {tx_id} | {amount} PLN | {store} | {category}")