#!/usr/bin/env bash
# Variables communes aux scripts. Surchargeables par l'environnement.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUCKET="${BUCKET:-${KUBERNETES_NAMESPACE:?KUBERNETES_NAMESPACE manquant : lancer depuis un service Onyxia ou definir BUCKET}}"
PREFIX="${PREFIX:-openflights}"
# Commande AWS : profil Onyxia "default" s'il existe, sinon variables d'environnement AWS_* + endpoint.
# AWS_CMD permet de surcharger la commande AWS.
if [ -n "${AWS_CMD:-}" ]; then
  AWS="$AWS_CMD"
elif aws configure list-profiles 2>/dev/null | grep -qx default; then
  AWS="aws --profile default"
else
  S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-https://${AWS_S3_ENDPOINT:?profil AWS default absent et AWS_S3_ENDPOINT manquant}}"
  AWS="aws --endpoint-url $S3_ENDPOINT_URL"
fi
SRC_URL="${SRC_URL:-https://raw.githubusercontent.com/jpatokal/openflights/master/data}"
RAW_DIR="$ROOT/data/raw"

stamp() { echo "== $(date '+%F %T') | ${KUBERNETES_NAMESPACE:-$BUCKET} | $*"; }

# --- Environnement Python / Spark / S3 (etapes 2 a 5) -------------------------------------------
PY="${PYTHON:-python3}"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/results}"
mkdir -p "$RESULTS_DIR"
export BUCKET PREFIX DATA_DIR RESULTS_DIR PYTHONPATH="$ROOT/src:${PYTHONPATH:-}"

spark_env() {  # identifiants et endpoint S3 pour Spark (S3A)
  if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then eval "$(aws configure export-credentials --profile default --format env)"; fi
  if [ -z "${S3_ENDPOINT_URL:-}" ]; then
    S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default 2>/dev/null || true)"
    [ -n "$S3_ENDPOINT_URL" ] || S3_ENDPOINT_URL="https://${AWS_S3_ENDPOINT:?AWS_S3_ENDPOINT manquant}"
  fi
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN S3_ENDPOINT_URL
}

spark_run() {  # lance un script Spark : python3 si pyspark est importable, sinon spark-submit
  spark_env
  if "$PY" -c 'import pyspark' 2>/dev/null; then "$PY" "$@"
  else "${SPARK_HOME:-/opt/spark}/bin/spark-submit" --master 'local[*]' "$@"; fi
}

need_py() {  # dependances Python hors Spark : duckdb, pyarrow
  "$PY" -c 'import duckdb, pyarrow' 2>/dev/null && return 0
  "$PY" -m pip install --user -q duckdb pyarrow 2>/dev/null || uv pip install --system --break-system-packages -q duckdb pyarrow
}
