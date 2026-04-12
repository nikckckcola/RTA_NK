from kafka import KafkaConsumer
import json

# konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='enrich-group',  # inna grupa żeby skrypty mogły działać naraz
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono moduł wzbogacania transakcji o poziom ryzyka...")
print("-" * 60)

try:
    for message in consumer:
        # pobieranie danych z trx
        transaction = message.value
        amount = transaction.get('amount', 0)

        # logika poziomu ryzyka
        if amount > 3000:
            risk_level = "HIGH"
        elif amount > 1000:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        # nowe pole do słownika
        transaction['risk_level'] = risk_level

        # wypisanie
        print(f"[{risk_level}] ID: {transaction.get('tx_id')} | Kwota: {amount} PLN | Sklep: {transaction.get('store')}")

except KeyboardInterrupt:
    print("\nZatrzymywanie modułu enrichment...")
finally:
    consumer.close()