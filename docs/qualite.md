# Qualité des données (OpenFlights)

Source des chiffres : `results/02_silver.log` (exécution sur Onyxia le 21/11/2026) et `openflights/silver/quality_report`.

| Constat | Lignes touchées | Traitement |
| --- | --- | --- |
| Aéroports sans code IATA (`\N`) | 1 626 / 7 698 | `NULL`, aéroports conservés (identifiés par `airport_id`) |
| Aéroports sans fuseau horaire (`\N`) | 1 021 | `NULL` |
| Compagnies inactives | 4 906 / 6 162 | colonne booléenne `active` |
| Compagnies dont `active` n'est ni `Y` ni `N` | 1 | `NULL` |
| Compagnies sans code IATA (`-`, vide, `\N`) | 4 628 | `NULL` |
| Routes sans aéroport de départ (`\N`) | 220 | rejetées (`src_id_null`) |
| Routes sans aéroport d'arrivée (`\N`) | 221 | rejetées (`dst_id_null`) |
| Routes dont l'id de départ n'existe pas dans `airports` | 263 | rejetées (`src_id_inconnu`) |
| Routes dont l'id d'arrivée n'existe pas dans `airports` | 267 | rejetées (`dst_id_inconnu`) |
| Routes sans compagnie (`\N`) | 479 | conservées (compagnie `NULL`) |
| Routes dont le code de départ diffère de l'IATA de l'aéroport pointé par l'id | 362 | conservées, signalées |
| Routes dont le code d'arrivée diffère de l'IATA de l'aéroport pointé par l'id | 365 | conservées, signalées |
| Routes en partage de code (`codeshare = Y`) | 14 597 | colonne booléenne, conservées |
| Routes avec escale | 11 | conservées |
| Doublons exacts de routes | 0 | contrôlé, rien à traiter |
| `airline_id` absent de `airlines.dat` | 0 (hors 479 NULL) | contrôlé |
| IATA dupliqués dans `airports.dat` | 0 | contrôlé |
| Latitude hors [-90, 90] | 0 | contrôlé |
| Longitude hors [-180, 180] | 0 | contrôlé |

**Bilan des routes.** 67 663 routes brutes = 66 771 valides + 892 rejetées (1,32 %). Les rejets sont comptés par motif en
appliquant les règles dans cet ordre : départ vide (220), arrivée vide (203), départ inconnu (259), arrivée inconnue (210).
Les comptes « inconnu » du tableau (263 et 267) sont indépendants de cet ordre, donc supérieurs aux rejets par motif : une
même route peut cumuler plusieurs défauts.

**Pourquoi ces choix.** Une route dont une extrémité est inconnue ne peut pas être rattachée à un aéroport : la compter
fausserait le classement. Elle est écrite dans `silver/routes_rejected` avec son motif plutôt que supprimée, ce qui garde la
traçabilité. Les codes incohérents ne sont pas rejetés car l'identifiant est valide ; ils expliquent en revanche l'écart entre
le classement brut (par code IATA) et le classement nettoyé (par identifiant), par exemple `ISL` (voir `docs/pipeline.md`).
