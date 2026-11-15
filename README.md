# Framework Big Data : hubs aériens mondiaux

## Auteurs

COMBE-TAMAIN Ruben, REVERSAT Antoine, DREAN-AVRILLON Edwin.

## Prérequis

- Service Onyxia `vscode-pyspark` avec au moins 2 CPU et 2 000 Mi.
- `git`, `aws`, Spark/PySpark, DuckDB, PyArrow et Python 3.
- Un stockage S3 accessible depuis le pod, avec le profil AWS `default` ou les variables AWS temporaires.
- Variables optionnelles : `SPARK_MAX_FACTOR=100` autorise Spark ×100, `BENCH_RUNS=5`,
  `BENCH_SEED=20261121`, `BUCKET`, `PREFIX`, `AWS_PROFILE` et `AWS_CMD`.

## Dataset

OpenFlights, https://github.com/jpatokal/openflights (`data/`), licence ODbL, téléchargé le **21/11/2026** :
81 523 lignes et 3 901 269 octets.

## Problématique

Quels aéroports sont les principaux hubs par routes sortantes ? Quelle part des routes est invalidée par le nettoyage ?
La montée en charge change-t-elle les performances des moteurs testés ?

## Chemins S3

- Brut : `s3://user-r-combe-tamain-ece/openflights/raw/` pour les trois fichiers `.dat` et `MANIFEST.txt`.
- Silver : `s3://user-r-combe-tamain-ece/openflights/silver/` pour les tables Spark en Parquet et les rejets.
- Gold : `s3://user-r-combe-tamain-ece/openflights/gold/hubs/` pour la table finale.

## Reproduire

```bash
git clone https://github.com/ruben-ctm/bigdata_project.git
cd bigdata_project
bash src/run_all.sh
```

`src/run_all.sh` exécute six étapes : `01_ingest`, `02_silver`, `03_gold`, `04_benchmark`, `05_verify`, `06_scale`.
Pour rejouer une étape : `STEPS="05_verify" bash src/run_all.sh`. Utiliser `SPARK_MAX_FACTOR=100` pour mesurer Spark jusqu’à ×100.
Les traces sont dans `results/` ; les étapes sont idempotentes.
`data/raw/*.dat` est ignoré par Git ; `data/raw/MANIFEST.txt` conserve la date, les tailles et les empreintes SHA-256.

## Résultats

- 67 663 routes : 66 771 valides, 892 rejetées (1,32 %) ;
- ATL arrive en tête avec 915 routes sortantes (codeshare compris) ;
- mesure alternative : routes sans codeshare et destinations distinctes dans `gold.hubs` ;

Rapport : [RapportProjet_COMBE-TAMAIN_REVERSAT_DREAN-AVRILLON.pdf](report/RapportProjet_COMBE-TAMAIN_REVERSAT_DREAN-AVRILLON.pdf). Mesures : [benchmark.csv](results/benchmark.csv),
[scale_test.csv](results/scale_test.csv). Documentation : [qualité](docs/qualite.md), [mesures](docs/mesures.md),
[pipeline](docs/pipeline.md), [échelle ×100](docs/echelle_x100.md). Preuves : [evidence/](evidence/) et [results/](results/).

