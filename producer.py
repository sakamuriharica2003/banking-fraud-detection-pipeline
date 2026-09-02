import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

merchant_categories = ['grocery', 'electronics', 'travel', 'restaurant', 'online_retail', 'gas_station']

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "card_id": fake.credit_card_number(),
        "amount": round(random.uniform(1, 5000), 2),
        "merchant_category": random.choice(merchant_categories),
        "location": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting transaction stream... press Ctrl+C to stop")
    while True:
        txn = generate_transaction()
        producer.send('transactions', txn)
        print(f"Sent: {txn}")
        time.sleep(1)  # one transaction per second