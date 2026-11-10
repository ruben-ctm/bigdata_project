#!/usr/bin/env bash
# Etape 5 : verification independante (DuckDB sur les CSV bruts) contre silver et gold.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
need_py
stamp "copie locale de silver et gold"
$AWS s3 sync "s3://$BUCKET/$PREFIX/silver" "$DATA_DIR/silver" --delete --only-show-errors
$AWS s3 sync "s3://$BUCKET/$PREFIX/gold" "$DATA_DIR/gold" --delete --only-show-errors
stamp "verification independante (DuckDB)"
"$PY" "$ROOT/src/verify.py"
