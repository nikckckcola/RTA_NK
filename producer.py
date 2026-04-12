from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# konfiguracja producenta
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    """Generuje losową transakcję finansową."""
    categories = ['Elektronika', 'Spożywcze', 'Rozrywka', 'Dom', 'Zdrowie']
    stores = ['Sklep A', 'Sklep B', 'Market C', 'Serwis D']
    
    return {
        'tx_id': random.randint(10000, 99999),
        'user_id': random.randint(1, 100),
        'amount': round(random.uniform(5.0, 5000.0), 2),
        'store': random.choice(stores),
        'category': random.choice(categories),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

print("Uruchamianie producenta... Naciśnij Ctrl+C, aby zatrzymać.")

try:
    while True:
        # generowanie danych
        transaction = generate_transaction()
        
        # wysyłka do tematu 'transactions'
        producer.send('transactions', value=transaction)
        
        print(f"Wysłano: {transaction}")
        
        # odczekanie 1 sekundy
        time.sleep(1)
except KeyboardInterrupt:
    print("\nZatrzymywanie producenta...")
finally:
    producer.flush()
    producer.close()