# Notes — Lab JSON

## Lancer le lab

```bash
docker exec kafka-lab kafka-topics --bootstrap-server localhost:9092 --create --topic transactions_raw --partitions 3 --replication-factor 1
docker exec kafka-lab kafka-topics --bootstrap-server localhost:9092 --create --topic transactions_clean --partitions 3 --replication-factor 1
docker exec kafka-lab kafka-topics --bootstrap-server localhost:9092 --create --topic transactions_errors --partitions 3 --replication-factor 1
cd json_lab
python consumer_json.py groupe-json-1
python producer_json.py ../csv_lab/data/transactions_dirty.csv
```

Pour voir le contenu des topics filtrés :

```bash
docker exec kafka-lab kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions_errors --from-beginning
```

## Schéma choisi (v1)

| champ | type | remarque |
|---|---|---|
| schema_version | int | pour savoir quelle version du schéma a produit l'événement |
| transaction_id | int | |
| user_id | int | |
| amount | float | |
| timestamp | string | format `YYYY-MM-DD HH:MM:SS` |

Si une valeur ne peut pas être convertie, le producer met `null` au lieu de planter. Si la ligne n'a pas 4 colonnes, il envoie `{"raw_line": "..."}` pour garder la ligne d'origine.

## Topics

- `transactions_raw` : tout ce qui sort du producer (événements bruts)
- `transactions_clean` : événements valides (rempli par le consumer)
- `transactions_errors` : événements rejetés, avec un champ `error` qui donne la raison

## Résultats avec les données sales

Sur `transactions_dirty.csv`, le consumer rejette :
- transaction 3 : montant vide
- transaction 4 : user_id `abc`
- transaction 6 : date `not-a-date`
- transaction 9 : montant `seven`
- transaction 2 (2e fois) : doublon
- transaction 11 : timestamp vide
- transaction 12 : date au format `2024/01/01`
- ligne `13,109` : nombre de colonnes incorrect

Remarque : les doublons sont détectés en mémoire, donc seulement tant que le consumer tourne.

## Évolution du schéma

- `python producer_json.py ../csv_lab/data/transactions.csv 2` : ajoute un champ `currency`. Le consumer continue de fonctionner car il lit les champs avec `.get()` et met `EUR` par défaut pour les anciens messages.
- `python producer_json.py ../csv_lab/data/transactions.csv 3` : supprime `timestamp`. Tous les événements partent dans `transactions_errors` → supprimer un champ obligatoire casse les consumers.

Conclusion : ajouter un champ optionnel est sans risque, en supprimer un ne l'est pas.

## Replay

- Relancer le consumer avec un nouveau groupe (`python consumer_json.py groupe-json-2`) relit tout l'historique de `transactions_raw`.
- On peut aussi remettre les offsets d'un groupe à zéro (consumer arrêté) :

```bash
docker exec kafka-lab kafka-consumer-groups --bootstrap-server localhost:9092 --group groupe-json-1 --topic transactions_raw --reset-offsets --to-earliest --execute
```

Ça permet de corriger le code de validation puis de retraiter les anciens événements.

## Questions

- JSON est mieux que le CSV car chaque message contient le nom des champs et des types (nombre, texte, null). On peut ajouter un champ sans casser l'ordre des colonnes.
- Le problème de l'évolution du schéma : les producers et les consumers ne sont pas mis à jour en même temps, donc un consumer peut recevoir des messages d'une ancienne ou d'une nouvelle version.
- Kafka sert de source de vérité car il garde les événements dans l'ordre et pendant un certain temps : on peut toujours les relire pour reconstruire un état ou corriger une erreur.
