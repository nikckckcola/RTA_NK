from kafka import KafkaConsumer
from collections import Counter
import json

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Rozpoczynam zbieranie statystyk per sklep...")
print("Podsumowanie będzie wyświetlane co 10 odebranych wiadomości.")
print("-" * 60)

try:
    for message in consumer:
        tx = message.value
        store = tx.get('store', 'Nieznany')
        amount = tx.get('amount', 0)

        # zwiększenie licznika trx per sklep
        store_counts[store] += 1
        
        # dodajemy kwotę do sumy per sklep
        total_amount[store] = total_amount.get(store, 0) + amount
        
        # zwiększenie licznika wszystkich odebranych wiadomości
        msg_count += 1

        # co 10 wiadomości wypisanie tabeli
        if msg_count % 10 == 0:
            print(f"\n--- PODSUMOWANIE (po {msg_count} wiadomościach) ---")
            print(f"{'Sklep':<15} | {'Liczba':<8} | {'Suma':<10} | {'Średnia':<10}")
            print("-" * 55)
            
            # Iterujemy po wszystkich sklepach, które do tej pory wystąpiły
            for s in store_counts:
                count = store_counts[s]
                suma = total_amount[s]
                srednia = suma / count
                print(f"{s:<15} | {count:<8} | {suma:<10.2f} | {srednia:<10.2f}")
            print("-" * 55)

except KeyboardInterrupt:
    print("\nZatrzymywanie statystyk...")
finally:
    consumer.close()