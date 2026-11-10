#!/usr/bin/env bash
# Lance une fois toutes les étapes depuis un service Onyxia.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
bash src/run_all.sh
