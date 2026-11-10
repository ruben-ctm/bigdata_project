#!/usr/bin/env bash
# Etape 3 : table finale gold.hubs avec Spark.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
stamp "gold : construction de hubs"
spark_run "$ROOT/src/gold.py"
stamp "contenu de gold/"
$AWS s3 ls "s3://$BUCKET/$PREFIX/gold/" --recursive --human-readable
