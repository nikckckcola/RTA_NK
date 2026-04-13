from kafka import KafkaConsumer
import json
import time
from collections import defaultdict

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='anomaly-detector-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# słownik przechowujący listy czasów transakcji dla każdego użytkownika
# user_id -> [timestamp1, timestamp2, ...]
user_history = defaultdict(list)

# reguła anomalii
TIME_WINDOW = 60  # sekundy
MAX_TRANSACTIONS = 3

print(f"Monitorowanie anomalii: >{MAX_TRANSACTIONS} transakcje w ciągu {TIME_WINDOW}s...")
print("-" * 60)

try:
    for message in consumer:
        tx = message.value
        user_id = tx.get('user_id')
        current_time = time.time()  # pobieranie aktualnego czas systemowy

        if user_id:
            # dodanie aktualnego czasu do historii użytkownika
            user_history[user_id].append(current_time)

            # usunięcie z historii czasy starsze niż 60 sekund
            user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= TIME_WINDOW]

            # sprawdzenie, czy liczba transakcji w oknie przekracza limit
            if len(user_history[user_id]) > MAX_TRANSACTIONS:
                print(f"⚠️  ALERT ANOMALII: Użytkownik {user_id} wykonał "
                      f"{len(user_history[user_id])} transakcje w ostatnie {TIME_WINDOW}s!")
                print(f"   Szczegóły ostatniej: {tx.get('amount')} PLN w {tx.get('store')}")

except KeyboardInterrupt:
    print("\nZatrzymywanie detektora anomalii...")
finally:
    consumer.close()