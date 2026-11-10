#!/usr/bin/env bash
# Etape 2 : nettoyage avec Spark (silver Parquet, table de rejets, rapport qualite).
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
stamp "silver : Spark nettoie le brut s3://$BUCKET/$PREFIX/raw/"
spark_run "$ROOT/src/silver.py"
stamp "contenu de silver/"
$AWS s3 ls "s3://$BUCKET/$PREFIX/silver/" --recursive --human-readable | grep -E "parquet|csv" | head -20
