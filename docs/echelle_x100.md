# Et avec 100 fois plus de données ?

## Mesure sur le cluster Onyxia
`src/06_scale.sh` reprend le brut depuis S3, réplique `routes.dat` ×1, ×10 et ×100 sur le disque du service, puis
`src/scale_test.py` rejoue la requête avec trois exécutions (défaut de `scale_test.py`). À ×100, les données représentent 6 766 300 routes,
237,7 Mo en CSV et 45,6 Mo en Parquet. Service `vscode-pyspark` du namespace `user-r-combe-tamain-ece`, 2 CPU et
2 000 Mi, exécution du 21/11/2026. Traces : `results/06_scale.log`, `results/scale_test.csv`.

| Moteur, format | ×1 | ×10 | ×100 |
| --- | ---: | ---: | ---: |
| DuckDB, Parquet | 163 ms | 178 ms | 414 ms |
| DuckDB, CSV | 345 ms | 672 ms | 3 404 ms |
| Spark, Parquet | 932 ms | 777 ms | 1 423 ms |
| Spark, CSV | 1 275 ms | 1 691 ms | 6 839 ms |

Spark ×100 est mesuré sur Onyxia : 1 423 ms en Parquet et 6 839 ms en CSV.

Les nouvelles médianes DuckDB sont 414 ms en Parquet et 3 404 ms en CSV à ×100. Le Parquet reste nettement plus rapide.
Les ×1/×10 Spark mesurés sont 932/777 ms en Parquet et 1 275/1 691 ms en CSV.

**Réserve.** Répliquer les lignes donne des données très compressibles et très régulières : des données réelles 100 fois plus
grosses auraient plus de valeurs distinctes. Les temps mesurés sont donc des bornes basses, pas des prévisions.

## Ce qui changerait (raisonnement, non mesuré)
- **Volume** : ≈ 390 Mo de CSV brut, ≈ 113 Mo en Parquet (à raison de 3,5 fois plus petit mesuré sur le jeu réel). Cela tient encore sur un seul nœud : le pod est limité à 2 000 Mi, avec un quota du namespace de 4 Gi.
- **Spark** : les 200 partitions de shuffle par défaut deviennent adaptées (elles étaient du surcoût à 4 Mo). Le gold est écrit en un seul fichier (`coalesce(1)`), acceptable pour 3 199 lignes mais à remplacer par une écriture partitionnée si le gold grossit.
- **Stockage** : éviter les petits fichiers ; écrire le brut directement en Parquet partitionné (par pays par exemple).
- **Qualité** : la jointure de validation des routes sur les aéroports reste peu coûteuse tant que `airports` (1 Mo) tient en mémoire ; Spark la diffuserait (broadcast). Si la table des aéroports devenait elle aussi grosse, il faudrait un join distribué.
- **Limite du cluster** : au-delà de quelques gigaoctets, il faudrait un vrai cluster Spark (plusieurs exécuteurs) ; le mode `local[*]` d'un service Onyxia ne suffirait plus.
