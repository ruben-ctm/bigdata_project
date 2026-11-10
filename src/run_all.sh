#!/usr/bin/env bash
# Enchaine les etapes (6 par defaut), garde une trace horodatee de chacune dans results/ (preuves d'execution).
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
# STEPS (optionnel) restreint l'execution a certaines etapes, ex. STEPS="06_scale" ; sans STEPS : les 6 etapes.
if [ -z "${STEPS:-}" ]; then STEPS="01_ingest 02_silver 03_gold 04_benchmark 05_verify 06_scale"; : > "$RESULTS_DIR/steps.tsv"; else touch "$RESULTS_DIR/steps.tsv"; fi
for step in $STEPS; do
  stamp "DEBUT $step" | tee -a "$RESULTS_DIR/$step.log"
  set +e
  bash "$ROOT/src/$step.sh" 2>&1 | tee -a "$RESULTS_DIR/$step.log"
  rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -ne 0 ]; then
    echo -e "$step\tECHEC" >> "$RESULTS_DIR/steps.tsv"; stamp "ECHEC $step"; cat "$RESULTS_DIR/steps.tsv"; exit 1
  fi
  echo -e "$step\tOK" >> "$RESULTS_DIR/steps.tsv"
  stamp "FIN $step" | tee -a "$RESULTS_DIR/$step.log"
done
cat "$RESULTS_DIR/steps.tsv"
