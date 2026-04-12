from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='counting-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()  # Liczy wystąpienia (ile transakcji)
total_amount = {}         # Sumuje kwoty (klucz: sklep, wartość: suma PLN)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    msg_count += 1
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + amount
    
    if msg_count % 10 == 0:
        print(f"\n--- RAPORT PO {msg_count} WIADOMOŚCIACH ---")
        print(f"{'Sklep':<12} | {'Liczba':<8} | {'Suma':<10} | {'Średnia':<10}")
        print("-" * 50)
        
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count
            print(f"{s:<12} | {count:<8} | {total:<10.2f} | {avg:<10.2f}")