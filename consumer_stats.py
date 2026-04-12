from kafka import KafkaConsumer
from collections import defaultdict
import json

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='category-stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# automatyczny nowy słownik dla każdej nowej kategorii
stats = defaultdict(lambda: {
    'count': 0, 
    'total_revenue': 0.0, 
    'min_amount': float('inf'), 
    'max_amount': float('-inf')
})

msg_count = 0

print("Analiza statystyk per kategoria (count, revenue, min, max)...")
print("-" * 70)

try:
    for message in consumer:
        tx = message.value
        category = tx.get('category', 'Inne')
        amount = tx.get('amount', 0.0)

        # aktualizacja statystyk dla kategorii
        s = stats[category]
        s['count'] += 1
        s['total_revenue'] += amount
        
        # logika min/max
        if amount < s['min_amount']:
            s['min_amount'] = amount
        if amount > s['max_amount']:
            s['max_amount'] = amount

        msg_count += 1

        # wyświetlanie co 10 wiadomości
        if msg_count % 10 == 0:
            print(f"\n--- STATYSTYKI KATEGORII (Odebrano: {msg_count}) ---")
            header = f"{'Kategoria':<15} | {'Liczba':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}"
            print(header)
            print("-" * len(header))
            
            for cat, data in stats.items():
                print(f"{cat:<15} | {data['count']:<6} | {data['total_revenue']:<10.2f} | "
                      f"{data['min_amount']:<8.2f} | {data['max_amount']:<8.2f}")
            print("-" * len(header))

except KeyboardInterrupt:
    print("\nZatrzymywanie statystyk kategorii...")
finally:
    consumer.close()