# Producer JSON : lit le CSV, transforme chaque ligne en objet JSON et l'envoie
# dans le topic des evenements bruts (transactions_raw).
#
# Utilisation :
#   python producer_json.py ../csv_lab/data/transactions.csv
#   python producer_json.py ../csv_lab/data/transactions_dirty.csv
#   python producer_json.py ../csv_lab/data/transactions.csv 2    (schema v2)
#   python producer_json.py ../csv_lab/data/transactions.csv 3    (schema v3)
import sys
import json
import time
from kafka import KafkaProducer

TOPIC_RAW = "transactions_raw"

fichier = "../csv_lab/data/transactions.csv"
version = 1
if len(sys.argv) > 1:
    fichier = sys.argv[1]
if len(sys.argv) > 2:
    version = int(sys.argv[2])


def en_entier(valeur):
    try:
        return int(valeur)
    except ValueError:
        return None


def en_decimal(valeur):
    try:
        return float(valeur)
    except ValueError:
        return None


def ligne_vers_json(ligne):
    colonnes = ligne.split(",")

    # ligne mal formee (pas le bon nombre de colonnes) : on ne peut pas
    # la convertir, on garde quand meme la ligne brute pour pouvoir la debugger
    if len(colonnes) != 4:
        return {"schema_version": version, "raw_line": ligne}

    # si une valeur ne se convertit pas on met None (null en JSON)
    # c'est le consumer qui decidera si l'evenement est valide ou pas
    event = {
        "schema_version": version,
        "transaction_id": en_entier(colonnes[0]),
        "user_id": en_entier(colonnes[1]),
        "amount": en_decimal(colonnes[2]),
        "timestamp": colonnes[3] if colonnes[3] != "" else None,
    }

    # evolution du schema
    if version >= 2:
        event["currency"] = "EUR"   # v2 : nouveau champ
    if version >= 3:
        del event["timestamp"]      # v3 : on supprime un champ

    return event


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

f = open(fichier, "r")
lignes = f.readlines()
f.close()

for ligne in lignes[1:]:  # on saute l'en-tete
    ligne = ligne.strip()
    if ligne == "":
        continue

    event = ligne_vers_json(ligne)

    # meme cle que dans le lab CSV : le user_id
    cle = event.get("user_id")
    if cle is not None:
        producer.send(TOPIC_RAW, key=str(cle).encode("utf-8"), value=event)
    else:
        producer.send(TOPIC_RAW, value=event)

    print("Envoye :", event)
    time.sleep(0.5)

producer.flush()
producer.close()
print("Fin de l'envoi")
