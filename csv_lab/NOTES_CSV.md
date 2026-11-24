# Notes — Lab CSV

## Lancer le lab

```bash
docker compose -f docker_compose.yml up -d
docker exec kafka-lab kafka-topics --bootstrap-server localhost:9092 --create --topic transactions_csv --partitions 3 --replication-factor 1
cd csv_lab
python producer_csv.py data/transactions.csv
python consumer_csv.py groupe-csv-1
```

## Choix du topic

- Nom : `transactions_csv`
- 3 partitions : assez pour tester plusieurs consumers dans le même groupe.
- Clé = `user_id` : les transactions d'un même utilisateur vont toujours dans la même partition, donc elles restent dans l'ordre.

## Observations

- **Offsets / replay** : si on relance le consumer avec le même groupe, il reprend là où il s'était arrêté. Avec un nouveau nom de groupe, il relit tout depuis le début (`auto_offset_reset="earliest"`).
- **Consumer groups** : avec 2 ou 3 consumers dans le même groupe, les partitions sont réparties entre eux, chaque message n'est lu qu'une fois. Avec plus de consumers que de partitions, certains ne reçoivent rien.
- **Ordre** : l'ordre est garanti seulement à l'intérieur d'une partition. Avec 3 partitions, le consumer n'affiche pas les transactions dans l'ordre des `transaction_id`.
- **Données sales** : Kafka accepte toutes les lignes sans broncher (montant vide, `abc` comme user_id, date invalide, doublon de la transaction 2, ligne `13,109` incomplète). Kafka ne fait que stocker des octets, il ne vérifie rien.

## Questions

- Kafka ne s'occupe pas de la structure CSV car pour lui un message est juste une suite d'octets.
- Problèmes du CSV : pas de types, pas de noms de champs dans le message, une virgule dans une valeur casse tout, si l'ordre des colonnes change le consumer lit n'importe quoi.
- Les offsets permettent de savoir où on en est, de reprendre après un crash et de relire les données.
