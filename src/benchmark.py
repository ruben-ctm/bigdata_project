"""Etape 4 : mesures de comparaison sur la meme requete (top 10 des aeroports par routes sortantes).
Moteurs : Spark (local[*]) et DuckDB. Formats : CSV brut et Parquet (silver).
Chaque variante recoit un warm-up, puis BENCH_RUNS=5 executions (defaut), mediane."""
import csv, os, statistics, sys, time
import random
import duckdb
from spark_common import session, AIRPORTS, AIRLINES, ROUTES

D = os.environ["DATA_DIR"]            # contient raw/ et silver/ (copies sur le disque du service)
OUT = os.environ.get("RESULTS_DIR", ".")
RUNS = int(os.environ.get("BENCH_RUNS", "5"))
random.seed(int(os.environ.get("BENCH_SEED", "20261121")))
RAW = f"{D}/raw"; SIL = f"{D}/silver"

# --- Requetes equivalentes (la version "brut" doit parser le CSV et valider les 2 extremites)
Q_SILVER = """
SELECT r.src_id, count(*) AS routes_out, count(DISTINCT r.airline_id) AS airlines_out
FROM routes r JOIN airports a ON r.src_id = a.airport_id
GROUP BY r.src_id ORDER BY routes_out DESC, r.src_id LIMIT 10"""
Q_RAW = """
SELECT r.src_id, count(*) AS routes_out, count(DISTINCT r.airline_id) AS airlines_out
FROM routes r JOIN airports s ON r.src_id = s.airport_id JOIN airports d ON r.dst_id = d.airport_id
GROUP BY r.src_id ORDER BY routes_out DESC, r.src_id LIMIT 10"""

results = []   # (variante, moteur, format, [ms...], resultat)

def measure(name, engine, fmt, fn):
    times, res = [], None
    fn()  # warm-up hors mesure pour echauffer le moteur et les caches locaux
    for _ in range(RUNS):
        t0 = time.perf_counter(); res = fn(); times.append((time.perf_counter() - t0) * 1000)
    results.append((name, engine, fmt, times, res))
    print(f"{name:<34} {engine:<7} {fmt:<8} " + " | ".join(f"{t:8.0f} ms" for t in times) + f"   mediane {statistics.median(times):8.0f} ms")

# ---------------- Spark
t0 = time.perf_counter(); spark = session("openflights-benchmark", 200); startup = (time.perf_counter() - t0) * 1000
print(f"Demarrage de la session Spark : {startup:.0f} ms")
spark.read.parquet(f"file://{SIL}/routes").createOrReplaceTempView("routes_p")
spark.read.parquet(f"file://{SIL}/airports").createOrReplaceTempView("airports_p")
spark.read.csv(f"file://{RAW}/routes.dat", schema=ROUTES, nullValue="\\N").createOrReplaceTempView("routes_c")
spark.read.csv(f"file://{RAW}/airports.dat", schema=AIRPORTS, nullValue="\\N").createOrReplaceTempView("airports_c")
def spark_q(q, r, a, shuffle):
    def run():
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle))
        return [tuple(x) for x in spark.sql(q.replace("routes r", f"{r} r").replace("airports a", f"{a} a").replace("airports s", f"{a} s").replace("airports d", f"{a} d")).collect()]
    return run
spark_variants = [
    ("Spark, Parquet, shuffle=200 (defaut)", "Spark", "Parquet", spark_q(Q_SILVER, "routes_p", "airports_p", 200)),
    ("Spark, Parquet, shuffle=8", "Spark", "Parquet", spark_q(Q_SILVER, "routes_p", "airports_p", 8)),
    ("Spark, CSV brut, shuffle=8", "Spark", "CSV", spark_q(Q_RAW, "routes_c", "airports_c", 8)),
]
random.shuffle(spark_variants)
for name, engine, fmt, fn in spark_variants:
    measure(name, engine, fmt, fn)
spark.stop()

# ---------------- DuckDB
con = duckdb.connect()
COLS_A = "{'airport_id':'INTEGER','name':'VARCHAR','city':'VARCHAR','country':'VARCHAR','iata':'VARCHAR','icao':'VARCHAR','latitude':'DOUBLE','longitude':'DOUBLE','altitude_ft':'INTEGER','tz_offset':'DOUBLE','dst':'VARCHAR','tz_name':'VARCHAR','type':'VARCHAR','source':'VARCHAR'}"
COLS_R = "{'airline_code':'VARCHAR','airline_id':'INTEGER','src_code':'VARCHAR','src_id':'INTEGER','dst_code':'VARCHAR','dst_id':'INTEGER','codeshare':'VARCHAR','stops':'INTEGER','equipment':'VARCHAR'}"
con.execute(f"CREATE VIEW routes_p AS SELECT * FROM read_parquet('{SIL}/routes/*.parquet')")
con.execute(f"CREATE VIEW airports_p AS SELECT * FROM read_parquet('{SIL}/airports/*.parquet')")
con.execute(f"CREATE VIEW routes_c AS SELECT * FROM read_csv('{RAW}/routes.dat', header=false, nullstr='\\N', columns={COLS_R})")
con.execute(f"CREATE VIEW airports_c AS SELECT * FROM read_csv('{RAW}/airports.dat', header=false, nullstr='\\N', columns={COLS_A})")
def duck_q(q, r, a):
    return lambda: [tuple(x) for x in con.execute(q.replace("routes r", f"{r} r").replace("airports a", f"{a} a").replace("airports s", f"{a} s").replace("airports d", f"{a} d")).fetchall()]
duck_variants = [
    ("DuckDB, Parquet", "DuckDB", "Parquet", duck_q(Q_SILVER, "routes_p", "airports_p")),
    ("DuckDB, CSV brut", "DuckDB", "CSV", duck_q(Q_RAW, "routes_c", "airports_c")),
]
random.shuffle(duck_variants)
for name, engine, fmt, fn in duck_variants:
    measure(name, engine, fmt, fn)

# ---------------- Coherence : tous les moteurs doivent renvoyer le meme top 10
ref = [tuple(x) for x in results[0][4]]
ok = all([tuple(x) for x in r[4]] == ref for r in results)
print("Resultats identiques entre moteurs/formats :", "OUI" if ok else "NON")
if not ok:
    sys.exit(1)

def size(path_glob_dir, ext=None):
    tot = 0
    for root, _, files in os.walk(path_glob_dir):
        for f in files:
            if ext is None or f.endswith(ext):
                tot += os.path.getsize(os.path.join(root, f))
    return tot
csv_bytes = sum(os.path.getsize(f"{RAW}/{f}") for f in ("airports.dat", "airlines.dat", "routes.dat"))
pq_bytes = size(SIL, ".parquet")
print(f"Taille : CSV brut {csv_bytes} octets ; silver Parquet {pq_bytes} octets (x{csv_bytes / pq_bytes:.1f} plus petit)")

os.makedirs(OUT, exist_ok=True)
with open(f"{OUT}/benchmark.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["variante", "moteur", "format", "ms_runs", "mediane_ms", "min_ms"])
    for name, eng, fmt, times, _ in results:
        w.writerow([name, eng, fmt, ";".join(f"{t:.0f}" for t in times), f"{statistics.median(times):.0f}", f"{min(times):.0f}"])
    w.writerow(["demarrage session Spark", "Spark", "-", f"{startup:.0f}", f"{startup:.0f}", f"{startup:.0f}"])
    w.writerow(["taille CSV brut (octets)", "-", "CSV", csv_bytes, csv_bytes, csv_bytes])
    w.writerow(["taille silver Parquet (octets)", "-", "Parquet", pq_bytes, pq_bytes, pq_bytes])
print("BENCHMARK OK ->", f"{OUT}/benchmark.csv")
