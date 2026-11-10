#!/usr/bin/env bash
# Script d'initialisation Onyxia facultatif.
# Clone le depot sur le disque du service et lance une execution unique en arriere-plan.
W="${HOME:-/home/onyxia}/work"
mkdir -p "$W"
nohup setsid bash -c '
  cd "'"$W"'" && rm -rf bigdata_project &&
  git clone -q --depth 1 https://github.com/ruben-ctm/bigdata_project &&
  cd bigdata_project && bash src/onyxia_run.sh
' > "$W/bigdata_project_run.out" 2>&1 &
exit 0
