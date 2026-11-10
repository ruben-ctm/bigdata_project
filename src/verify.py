"""Etape 5 : verification independante avec DuckDB. Recalcule les resultats a partir des CSV bruts
(autre moteur, autre code que le pipeline Spark) et les compare a silver et a gold."""
import os, sys, duckdb

D = os.environ["DATA_DIR"]
RAW, SIL, GOLD = f"{D}/raw", f"{D}/silver", f"{D}/gold"
con = duckdb.connect()
COLS_A = "{'airport_id':'INTEGER','name':'VARCHAR','city':'VARCHAR','country':'VARCHAR','iata':'VARCHAR','icao':'VARCHAR','latitude':'DOUBLE','longitude':'DOUBLE','altitude_ft':'INTEGER','tz_offset':'DOUBLE','dst':'VARCHAR','tz_name':'VARCHAR','type':'VARCHAR','source':'VARCHAR'}"
COLS_R = "{'airline_code':'VARCHAR','airline_id':'INTEGER','src_code':'VARCHAR','src_id':'INTEGER','dst_code':'VARCHAR','dst_id':'INTEGER','codeshare':'VARCHAR','stops':'INTEGER','equipment':'VARCHAR'}"
con.execute(f"CREATE VIEW A AS SELECT * FROM read_csv('{RAW}/airports.dat', header=false, nullstr='\\N', columns={COLS_A})")
con.execute(f"CREATE VIEW R AS SELECT * FROM read_csv('{RAW}/routes.dat', header=false, nullstr='\\N', columns={COLS_R})")
con.execute("CREATE VIEW V AS SELECT r.* FROM R r JOIN A s ON r.src_id = s.airport_id JOIN A d ON r.dst_id = d.airport_id")
con.execute(f"CREATE VIEW silver_routes AS SELECT * FROM read_parquet('{SIL}/routes/*.parquet')")
con.execute(f"CREATE VIEW silver_rejected AS SELECT * FROM read_parquet('{SIL}/routes_rejected/*.parquet')")
con.execute(f"CREATE VIEW hubs AS SELECT * FROM read_parquet('{GOLD}/hubs/*.parquet')")
one = lambda q: con.execute(q).fetchone()[0]

checks = []
def check(name, expected, got):
    ok = expected == got
    checks.append(ok)
    print(("OK  " if ok else "KO  ") + f"{name} : DuckDB(brut)={expected} | pipeline={got}")

check("routes brutes = valides + rejetees (silver)", one("SELECT count(*) FROM R"), one("SELECT count(*) FROM silver_routes") + one("SELECT count(*) FROM silver_rejected"))
check("routes valides", one("SELECT count(*) FROM V"), one("SELECT count(*) FROM silver_routes"))
check("somme des routes_out de gold = routes valides", one("SELECT count(*) FROM V"), one("SELECT sum(routes_out) FROM hubs"))

top_duck = con.execute("""
    WITH c AS (SELECT src_id, count(*) AS n FROM V GROUP BY src_id),
         k AS (SELECT src_id, n, rank() OVER (ORDER BY n DESC) AS rk FROM c)
    SELECT src_id, n FROM k WHERE rk <= 10 ORDER BY n DESC, src_id""").fetchall()
top_gold = con.execute("SELECT airport_id, routes_out FROM hubs WHERE rank_clean <= 10 ORDER BY routes_out DESC, airport_id").fetchall()
check("top 10 des hubs apres nettoyage (id, routes sortantes)", top_duck, top_gold)

raw_duck = con.execute("SELECT src_code, count(*) AS n FROM R WHERE src_code IS NOT NULL GROUP BY src_code ORDER BY n DESC, src_code LIMIT 5").fetchall()
raw_gold = con.execute("SELECT iata, routes_out_raw FROM hubs ORDER BY routes_out_raw DESC, iata LIMIT 5").fetchall()
check("top 5 du classement brut (IATA, routes)", raw_duck, raw_gold)

# Colonnes ajoutees a gold : recalcul DuckDB par aeroport (codeshare = 'Y', NULL sinon)
ref_cols = con.execute("""
    SELECT src_id,
           count(*)                                                              AS routes_out,
           count(DISTINCT dst_id)                                                AS dest,
           count(*) FILTER (WHERE codeshare IS DISTINCT FROM 'Y')                AS out_nc,
           count(DISTINCT dst_id) FILTER (WHERE codeshare IS DISTINCT FROM 'Y')  AS dest_nc
    FROM V GROUP BY src_id ORDER BY src_id""").fetchall()
gold_cols = con.execute("""
    SELECT airport_id, routes_out, destinations_distinctes,
           routes_out_sans_codeshare, destinations_distinctes_sans_codeshare
    FROM hubs ORDER BY airport_id""").fetchall()
check("gold : routes, destinations distinctes, sans codeshare (tous les aeroports)", ref_cols, gold_cols)

# Ligne ATL pour le rapport (valeurs affichees, comparees au brut)
atl_raw = con.execute("""
    SELECT count(*), count(DISTINCT v.dst_id), count(*) FILTER (WHERE v.codeshare IS DISTINCT FROM 'Y')
    FROM V v JOIN A a ON v.src_id = a.airport_id WHERE a.iata = 'ATL'""").fetchone()
atl_gold = con.execute("""SELECT routes_out, destinations_distinctes, routes_out_sans_codeshare
                          FROM hubs WHERE iata = 'ATL'""").fetchone()
check("ATL (routes, destinations distinctes, sans codeshare)", tuple(atl_raw), tuple(atl_gold))

print("\nTop 10 recalcule par DuckDB a partir du brut :")
for iata, n in con.execute("""WITH c AS (SELECT src_id, count(*) AS n FROM V GROUP BY src_id)
    SELECT a.iata, c.n FROM c JOIN A a ON a.airport_id = c.src_id ORDER BY c.n DESC, a.airport_id LIMIT 10""").fetchall():
    print(f"  {iata}  {n}")
print(f"\nVERIFICATION : {sum(checks)}/{len(checks)} controles OK")
print("Limite : Spark et DuckDB appliquent les memes regles ; cela prouve la coherence, pas l'exactitude metier.")
sys.exit(0 if all(checks) else 1)
