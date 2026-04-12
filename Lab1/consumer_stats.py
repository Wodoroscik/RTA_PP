from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

counts = defaultdict(int)
sums = defaultdict(float)
min_amounts = defaultdict(lambda: float('inf'))  # Domyślnie nieskończoność
max_amounts = defaultdict(lambda: float('-inf')) # Domyślnie minus nieskończoność

msg_count = 0

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']
    
    msg_count += 1
    counts[cat] += 1
    sums[cat] += amt
    
    if amt < min_amounts[cat]:
        min_amounts[cat] = amt
        
    if amt > max_amounts[cat]:
        max_amounts[cat] = amt
        
    if msg_count % 10 == 0:
        print(f"\n--- STATYSTYKI KATEGORII (Wiadomości: {msg_count}) ---")
        header = f"{'Kategoria':<12} | {'Sztuk':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}"
        print(header)
        print("-" * len(header))
        
        for c in sorted(counts.keys()):
            print(f"{c:<12} | {counts[c]:<6} | {sums[c]:<10.2f} | {min_amounts[c]:<8.2f} | {max_amounts[c]:<8.2f}")