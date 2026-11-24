# Consumer JSON : lit les evenements bruts, verifie chaque evenement et le
# renvoie dans un autre topic :
#   - transactions_clean  -> evenements valides
#   - transactions_errors -> evenements mal formes (avec la raison)
#
# Utilisation : python consumer_json.py <group_id>
import sys
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

TOPIC_RAW = "transactions_raw"
TOPIC_CLEAN = "transactions_clean"
TOPIC_ERRORS = "transactions_errors"

groupe = "groupe-json-1"
if len(sys.argv) > 1:
    groupe = sys.argv[1]

consumer = KafkaConsumer(
    TOPIC_RAW,
    bootstrap_servers="localhost:9092",
    group_id=groupe,
    auto_offset_reset="earliest",
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# pour reperer les doublons (meme transaction_id envoye deux fois)
deja_vus = set()


def verifier(event):
    """Renvoie None si l'evenement est valide, sinon la raison de l'erreur."""
    if "raw_line" in event:
        return "nombre de colonnes incorrect"

    if event.get("transaction_id") is None:
        return "transaction_id manquant ou invalide"
    if event.get("user_id") is None:
        return "user_id manquant ou invalide"
    if event.get("amount") is None:
        return "amount manquant ou invalide"

    # le champ timestamp n'existe plus en v3
    if "timestamp" not in event:
        return "champ timestamp absent"
    if event["timestamp"] is None:
        return "timestamp vide"
    try:
        datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return "format de date invalide"

    if event["transaction_id"] in deja_vus:
        return "doublon"

    return None


print("Consumer demarre, groupe =", groupe)

nb_ok = 0
nb_erreurs = 0

for message in consumer:
    texte = message.value.decode("utf-8")

    # 1) est-ce que c'est du JSON valide ?
    try:
        event = json.loads(texte)
    except json.JSONDecodeError:
        print("[ERREUR] offset", message.offset, ": pas du JSON ->", texte)
        producer.send(TOPIC_ERRORS, value={"raw": texte, "error": "JSON invalide"})
        nb_erreurs += 1
        continue

    # 2) est-ce que les champs sont corrects ?
    erreur = verifier(event)

    if erreur is None:
        deja_vus.add(event["transaction_id"])
        # le champ currency n'existe qu'a partir de la v2 -> valeur par defaut
        devise = event.get("currency", "EUR")
        print("[OK] p", message.partition, "offset", message.offset, "|",
              "transaction", event["transaction_id"], "user", event["user_id"],
              event["amount"], devise, "(v" + str(event.get("schema_version", 1)) + ")")
        producer.send(TOPIC_CLEAN, value=event)
        nb_ok += 1
    else:
        print("[ERREUR] p", message.partition, "offset", message.offset, ":", erreur, "->", event)
        event["error"] = erreur
        producer.send(TOPIC_ERRORS, value=event)
        nb_erreurs += 1

    print("   total valides :", nb_ok, "| erreurs :", nb_erreurs)
