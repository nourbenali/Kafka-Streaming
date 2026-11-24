# Consumer CSV : lit les messages du topic et affiche les lignes brutes
# Utilisation : python consumer_csv.py <group_id>
import sys
from kafka import KafkaConsumer

TOPIC = "transactions_csv"

if len(sys.argv) > 1:
    groupe = sys.argv[1]
else:
    groupe = "groupe-csv-1"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=groupe,
    auto_offset_reset="earliest",  # si le groupe est nouveau on lit depuis le debut
    enable_auto_commit=True,
)

print("Consumer demarre, groupe =", groupe)

for message in consumer:
    ligne = message.value.decode("utf-8")
    print("partition", message.partition, "| offset", message.offset, "|", ligne)
