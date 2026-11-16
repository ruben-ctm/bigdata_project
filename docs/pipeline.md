# Pipeline et choix

```mermaid
flowchart LR
  A[GitHub OpenFlights<br/>3 fichiers .dat, 3,9 Mo] -->|01_ingest.sh| B[(S3 raw<br/>openflights/raw)]
  B -->|02_silver.sh Spark| C[(S3 silver<br/>Parquet + rejets + qualité)]
  C -->|03_gold.sh Spark| D[(S3 gold<br/>hubs)]
  B -->|05_verify.sh DuckDB| E{Comparaison<br/>7 contrôles}
  C --> E
  D --> E
  C -.->|04_benchmark.sh| F[Mesures Spark / DuckDB]
  B -->|06_scale.sh| G[Passage ×1/×10/×100]
```

- **raw** : les 3 fichiers tels quels, plus un manifeste (date, taille, empreinte SHA-256).
- **silver** : schémas explicites (pas d'inférence), `\N` lu comme `NULL`, routes valides et routes rejetées séparées, rapport qualité.
- **gold.hubs** : une ligne par aéroport ayant au moins une route valide sortante (3 199 aéroports), avec `routes_out`, `routes_out_sans_codeshare`, `destinations_distinctes`, `destinations_distinctes_sans_codeshare`, `routes_in`, `airlines_out`, le classement après nettoyage (`rank_clean`), le classement brut par code IATA (`routes_out_raw`, `rank_raw`) et l'écart `rank_shift`.
- **Vérification** : DuckDB recalcule les résultats depuis les CSV bruts, avec son propre code, et compare sept sorties : routes totales, routes valides, somme de `routes_out`, top 10 après nettoyage, top 5 brut, les quatre colonnes gold sur tous les aéroports, puis ATL sur routes/destinations/codeshare. Ces contrôles prouvent la cohérence Spark/DuckDB, pas l'exactitude métier.

## Effet du nettoyage sur le classement
Les cinq premiers sont inchangés (ATL 915, ORD 558, PEK 531, LHR 525, CDG 524). Écarts liés à la validation : PEK passe de 535
à 531 routes, LHR de 527 à 525, LAX de 492 à 489. L'aéroport `ISL` passe du rang 3 179 (comptage brut par code IATA) au rang
19 (comptage par identifiant) : des routes l'identifient par un autre code que son IATA, ce qui confirme les 362 et 365
codes incohérents du rapport qualité.

**Limite de vérification.** La vérification compare deux implémentations sur les mêmes règles. Elle prouve la cohérence entre moteurs, pas l’exactitude métier ; aucune source externe n’a été confrontée.
