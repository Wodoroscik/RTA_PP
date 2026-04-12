from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction(counter):
    stores = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
    categories = ["elektronika", "odzież", "żywność", "książki"]
    
    transaction = {
        "tx_id": f"TX{counter:04d}", # format TX0001, TX0002
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }
    return transaction

tx_counter = 1
try:
    while True:
        data = generate_transaction(tx_counter)

        producer.send('transactions', value=data)

        print(f"Wysłano: {data}")

        tx_counter += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("\nZatrzymano producenta.")
finally:
    producer.close()