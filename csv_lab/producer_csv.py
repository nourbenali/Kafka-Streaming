# Producer CSV : lit le fichier ligne par ligne et envoie chaque ligne dans Kafka
# Utilisation : python producer_csv.py data/transactions.csv
import sys
import time
from kafka import KafkaProducer

TOPIC = "transactions_csv"

if len(sys.argv) > 1:
    fichier = sys.argv[1]
else:
    fichier = "data/transactions.csv"

producer = KafkaProducer(bootstrap_servers="localhost:9092")

f = open(fichier, "r")
lignes = f.readlines()
f.close()

# la premiere ligne c'est l'en-tete, on ne l'envoie pas
for ligne in lignes[1:]:
    ligne = ligne.strip()
    if ligne == "":
        continue

    # on prend le user_id comme cle -> toutes les transactions d'un meme
    # utilisateur vont dans la meme partition (donc restent dans l'ordre)
    colonnes = ligne.split(",")
    if len(colonnes) > 1:
        cle = colonnes[1]
    else:
        cle = None

    if cle is not None:
        producer.send(TOPIC, key=cle.encode("utf-8"), value=ligne.encode("utf-8"))
    else:
        producer.send(TOPIC, value=ligne.encode("utf-8"))

    print("Envoye :", ligne)
    time.sleep(0.5)  # pour simuler un flux et pas tout envoyer d'un coup

producer.flush()
producer.close()
print("Fin de l'envoi")
