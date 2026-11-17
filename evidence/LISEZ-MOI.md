# Dossier evidence

Captures d'écran réelles du samedi 21/11/2026 (heure de Paris, visible dans la barre de menu de l'ordinateur).
Namespace Onyxia : `user-r-combe-tamain-ece` ; service `vscode-pyspark` (pod `vscode-pyspark-24230-0`, limite 2 000 Mi).
Les journaux `results/*.log` portent aussi le namespace et l'heure d'exécution en UTC (heure de Paris = UTC + 1).

| N° | Contenu | Heure (Paris) |
| --- | --- | --- |
| `01` | Onyxia, Mes services : `vscode-pyspark` en cours, quotas du namespace | 18:34 |
| `02` | Terminal : commit de départ, namespace, date (départ) | 17:52 |
| `03` | Terminal : `01_ingest` (manifeste, envoi S3, `OK`) | 18:13 |
| `04` | Terminal : `02_silver` (rapport qualité, 66 771 valides / 892 rejetées, `OK`) | 18:14 |
| `05` | Terminal : `03_gold` (top 10 des hubs, `GOLD OK hubs=3199`) | 18:21 |
| `06` | Terminal : `04_benchmark` (5 exécutions, résultats identiques : OUI) | 18:23 |
| `07`, `07b` | Terminal : `05_verify` (7 contrôles sur 7) | 18:23 / 18:24 |
| `08` | Terminal : `06_scale` (DuckDB ×1/×10/×100, Spark ×1/×10/×100) | 18:33 |
| `09` | Terminal : capture de `steps.tsv` montrant les six lignes `01_ingest` à `06_scale` en `OK`, puis la liste des fichiers `results/` | 18:33 |
| `10` | Onyxia, Stockage de données : racine du bucket | 18:34 |
| `11` | Onyxia : `openflights/raw/` (3 `.dat` et manifeste) | 18:34 |
| `12` | Onyxia : `openflights/silver/` (5 dossiers) | 18:34 |
| `13` | Onyxia : `openflights/gold/hubs/` (Parquet) | 18:35 |
| `14` | Onyxia, Événements du namespace : redémarrage du conteneur (`Back-off restarting failed container`) | 18:35 |

Remarques.
- La capture `02` date du premier lancement sur ce service (16:52 UTC) ; les journaux `01` et `02` retenus sont ceux de la relance propre de 17:13 UTC (captures `03` et `04`).
- Dans la racine du bucket (`10`), seul `openflights/` appartient au projet ; `bronze`, `silver` et `warehouse` viennent d'un TP précédent.
- Spark ×100 est mesuré : 1 423 ms en Parquet et 6 839 ms en CSV. Le redémarrage visible en capture `14` vers 18:15 n’est pas attribué à un essai précis.
