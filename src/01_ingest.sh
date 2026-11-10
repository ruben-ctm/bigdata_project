#!/usr/bin/env bash
# Etape 1 : telecharge OpenFlights, controle la taille (1 a 5 Mo), ecrit un manifeste, envoie sur S3.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

FILES=(airports.dat airlines.dat routes.dat)
declare -A EXPECTED_LINES=([airports.dat]=7698 [airlines.dat]=6162 [routes.dat]=67663)
mkdir -p "$RAW_DIR"

stamp "telechargement depuis $SRC_URL"
for f in "${FILES[@]}"; do
  curl -fsSL --retry 3 -o "$RAW_DIR/$f" "$SRC_URL/$f"
done

total=0
: > "$RAW_DIR/MANIFEST.txt"
{
  echo "date_telechargement: $(date '+%F %T %Z')"
  echo "source: $SRC_URL"
  echo "licence: ODbL (Open Database License)"
  echo "fichier | lignes | octets | sha256"
} >> "$RAW_DIR/MANIFEST.txt"
for f in "${FILES[@]}"; do
  bytes=$(wc -c < "$RAW_DIR/$f"); lines=$(wc -l < "$RAW_DIR/$f"); sum=$(sha256sum "$RAW_DIR/$f" | cut -d' ' -f1)
  total=$((total + bytes))
  echo "$f | $lines | $bytes | $sum" >> "$RAW_DIR/MANIFEST.txt"
  [ "$lines" = "${EXPECTED_LINES[$f]}" ] || echo "ATTENTION : $f a $lines lignes (attendu ${EXPECTED_LINES[$f]}) ; la source a peut-etre change" >&2
done
echo "total_octets: $total" >> "$RAW_DIR/MANIFEST.txt"
cat "$RAW_DIR/MANIFEST.txt"

# Contrainte du sujet : donnees brutes entre 1 Mo et 5 Mo
if [ "$total" -lt 1000000 ] || [ "$total" -gt 5000000 ]; then
  echo "ERREUR : taille totale $total octets hors de [1 Mo, 5 Mo]" >&2; exit 1
fi

stamp "envoi vers s3://$BUCKET/$PREFIX/raw/"
for f in "${FILES[@]}" MANIFEST.txt; do
  $AWS s3 cp "$RAW_DIR/$f" "s3://$BUCKET/$PREFIX/raw/$f" --only-show-errors
done
stamp "contenu du bucket"
$AWS s3 ls "s3://$BUCKET/$PREFIX/raw/" --human-readable
