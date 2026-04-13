from kafka import KafkaConsumer
import json

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")
print("-" * 50)

try:
    for message in consumer:
        # wycignięcie danych z wiadomości
        tx = message.value
        
        # pobranie kwoty
        amount = tx.get('amount', 0)
        
        # filtr
        if amount > 3000:
            # wycignięcie pozostałych danych do sformatowania alertu
            tx_id = tx.get('tx_id', 'N/A')
            store = tx.get('store', 'Nieznany')
            category = tx.get('category', 'brak')
            
            # alert
            print(f"ALERT: TX{tx_id} | {amount:.2f} PLN | {store} | {category}")

except KeyboardInterrupt:
    print("\nZamykanie konsumenta...")
finally:
    consumer.close()