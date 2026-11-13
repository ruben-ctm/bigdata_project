# Mesures et choix techniques

Requête mesurée : top 10 des aéroports par routes sortantes (routes dont les deux extrémités existent), même résultat vérifié
pour tous les moteurs et formats. Exécution du 21/11/2026 sur Onyxia (`vscode-pyspark`, Spark en `local[*]`) ;
le benchmark utilise cinq exécutions, un warm-up hors mesure et un ordre aléatoire reproductible (`BENCH_SEED`).

| Variante | Exécutions (ms) | Médiane (ms) |
| --- | --- | --- |
| Spark, Parquet, `shuffle.partitions = 200` (défaut) | 1 464 ; 1 536 ; 1 315 ; 891 ; 1 354 | 1 354 |
| Spark, Parquet, `shuffle.partitions = 8` | 862 ; 597 ; 569 ; 677 ; 588 | 597 |
| Spark, CSV brut, `shuffle.partitions = 8` | 967 ; 1 419 ; 954 ; 1 006 ; 783 | 967 |
| DuckDB, Parquet | 15 ; 15 ; 13 ; 13 ; 12 | 13 |
| DuckDB, CSV brut | 332 ; 334 ; 333 ; 328 ; 323 | 332 |

Attention : la requête CSV valide les deux extrémités de chaque route (2 jointures), la requête Parquet lit des routes déjà validées (1 jointure). L’écart CSV/Parquet mélange donc format et validation.

Autres mesures : démarrage de la session Spark 11 233 ms ; CSV brut 3 901 269 octets contre 1 129 655 octets pour le silver
Parquet (3,5 fois plus petit).

**Lectures.**
- Parquet contre CSV : 1,6 fois plus rapide pour Spark (597 contre 967 ms à 8 partitions), environ 25 fois pour DuckDB (13 contre 332 ms).
- `shuffle.partitions` : 8 au lieu de 200 réduit la médiane de 1 354 à 597 ms, soit environ 2,3 fois.
- Spark contre DuckDB : DuckDB est environ 46 fois plus rapide sur le même Parquet (13 contre 597 ms). Spark met 11,2 s à démarrer, donc son surcoût domine à 4 Mo.

**Alternatives écartées.**
- *Trino et Polaris* : non mesurés dans ce projet : retour d’expérience du TP du 09/11, aucun log conservé.
- *Spark seul, sans DuckDB* : écarté pour la vérification indépendante, qui doit utiliser un autre moteur et un autre code, d'où DuckDB.
- *Delta/Iceberg à la place de Parquet* : non retenu, aucun besoin de transactions ni de voyage dans le temps sur un jeu de 4 Mo ; non mesuré.
## Passage à l'échelle

Spark ×100 est mesuré sur le pod de 2 000 Mi avec `SPARK_MAX_FACTOR=100`.
Les nouvelles médianes DuckDB sont 414 ms en Parquet et 3 404 ms en CSV à ×100. Les médianes Spark sont 932/777 ms en
Parquet et 1 275/1 691 ms en CSV pour ×1/×10. À ×100, Spark mesure 1 423 ms en Parquet et 6 839 ms en CSV.
Voir [docs/echelle_x100.md](echelle_x100.md).
