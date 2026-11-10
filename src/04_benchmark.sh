#!/usr/bin/env bash
# Etape 4 : mesures Spark / DuckDB, CSV / Parquet (warm-up + 5 executions, mediane).
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
need_py
stamp "copie locale de silver pour les mesures"
$AWS s3 sync "s3://$BUCKET/$PREFIX/silver" "$DATA_DIR/silver" --delete --only-show-errors
stamp "benchmark"
spark_run "$ROOT/src/benchmark.py"
