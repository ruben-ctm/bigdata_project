#!/usr/bin/env bash
# Etape 6 : mesure du passage a l'echelle (x1, x10, x100) sur le service Onyxia.
# Reprend le brut depuis S3, replique routes.dat x1/x10/x100 sur le disque du service, rejoue la meme requete
# avec DuckDB et Spark (CSV et Parquet). Resultats : results/scale_test.csv.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
need_py
stamp "copie locale du brut depuis S3"
mkdir -p "$DATA_DIR/raw"
$AWS s3 sync "s3://$BUCKET/$PREFIX/raw" "$DATA_DIR/raw" --exclude "*" --include "*.dat" --only-show-errors
ls -l "$DATA_DIR/raw"
stamp "mesure du passage a l'echelle"
spark_run "$ROOT/src/scale_test.py"
stamp "espace disque utilise par les jeux repliques"
du -sh "$DATA_DIR/scale"
