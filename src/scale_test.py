"""Mesure du passage a l'echelle : meme requete que le benchmark (top 10 des aeroports par routes sortantes,
routes valides) sur routes.dat replique x1, x10, x100. DuckDB et Spark (local[*]), CSV et Parquet.
Executee sur le service Onyxia par src/06_scale.sh (donnees sur le disque du service, sans S3 pendant la mesure) :
elle isole le moteur et le format."""
import csv, os, statistics, sys, time
import duckdb

D = os.environ.get("DATA_DIR", "data"); RAW = f"{D}/raw"
OUT = os.environ.get("RESULTS_DIR", "results"); W = f"{D}/scale"
FACTORS = [int(x) for x in os.environ.get("SCALE_FACTORS", "1,10,100").split(",")]
RUNS = int(os.environ.get("BENCH_RUNS", "3"))
os.makedirs(W, exist_ok=True); os.makedirs(OUT, exist_ok=True)

COLS_A = "{'airport_id':'INTEGER','name':'VARCHAR','city':'VARCHAR','country':'VARCHAR','iata':'VARCHAR','icao':'VARCHAR','latitude':'DOUBLE','longitude':'DOUBLE','altitude_ft':'INTEGER','tz_offset':'DOUBLE','dst':'VARCHAR','tz_name':'VARCHAR','type':'VARCHAR','source':'VARCHAR'}"
COLS_R = "{'airline_code':'VARCHAR','airline_id':'INTEGER','src_code':'VARCHAR','src_id':'INTEGER','dst_code':'VARCHAR','dst_id':'INTEGER','codeshare':'VARCHAR','stops':'INTEGER','equipment':'VARCHAR'}"
SPARK_A = ("airport_id INT, name STRING, city STRING, country STRING, iata STRING, icao STRING, latitude DOUBLE, longitude DOUBLE, "
           "altitude_ft INT, tz_offset DOUBLE, dst STRING, tz_name STRING, type STRING, source STRING")
SPARK_R = ("airline_code STRING, airline_id INT, src_code STRING, src_id INT, dst_code STRING, dst_id INT, codeshare STRING, stops INT, equipment STRING")
Q = """SELECT r.src_id, count(*) AS routes_out, count(DISTINCT r.airline_id) AS airlines_out
FROM routes r JOIN airports s ON r.src_id = s.airport_id JOIN airports d ON r.dst_id = d.airport_id
GROUP BY r.src_id ORDER BY routes_out DESC, r.src_id LIMIT 10"""

def med(f):
    t = []
    for _ in range(RUNS):
        a = time.perf_counter(); res = f(); t.append((time.perf_counter() - a) * 1000)
    return statistics.median(t), res

# --- jeux repliques
src = open(f"{RAW}/routes.dat", "rb").read()
for n in FACTORS:
    p = f"{W}/routes_x{n}.dat"
    if not os.path.exists(p):
        with open(p, "wb") as f:
            for _ in range(n): f.write(src)
con = duckdb.connect()
con.execute("SET memory_limit='1GB'")
for n in FACTORS:  # versions Parquet (ecrites une fois par DuckDB)
    p = f"{W}/routes_x{n}.parquet"
    if not os.path.exists(p):
        con.execute(f"COPY (SELECT * FROM read_csv('{W}/routes_x{n}.dat', header=false, nullstr='\\N', columns={COLS_R})) TO '{p}' (FORMAT parquet)")
con.execute(f"CREATE VIEW airports AS SELECT * FROM read_csv('{RAW}/airports.dat', header=false, nullstr='\\N', columns={COLS_A})")

rows, ref = [], {}
def record(n, engine, fmt, ms, res):
    key = n
    if key not in ref: ref[key] = res
    assert res == ref[key], f"resultat different pour x{n} {engine} {fmt}"
    size = os.path.getsize(f"{W}/routes_x{n}.{'dat' if fmt == 'CSV' else 'parquet'}")
    rows.append((n, engine, fmt, size, round(ms)))
    print(f"x{n:<4} {engine:<7} {fmt:<8} {size/1e6:8.1f} Mo  mediane {ms:9.0f} ms", flush=True)

for n in FACTORS:
    for fmt, ddl in (("CSV", f"read_csv('{W}/routes_x{n}.dat', header=false, nullstr='\\N', columns={COLS_R})"),
                     ("Parquet", f"read_parquet('{W}/routes_x{n}.parquet')")):
        con.execute(f"CREATE OR REPLACE VIEW routes AS SELECT * FROM {ddl}")
        ms, res = med(lambda: [tuple(x) for x in con.execute(Q).fetchall()])
        record(n, "DuckDB", fmt, ms, [(a, b, c) for a, b, c in res])

# --- Spark (valeurs normalisees : le count est multiplie par n, on compare donc par facteur)
con.close()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("scale").config("spark.ui.showConsoleProgress", "false").config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.read.csv(f"{RAW}/airports.dat", schema=SPARK_A, nullValue="\\N").createOrReplaceTempView("airports")
for n in [f for f in FACTORS if f <= int(os.environ.get('SPARK_MAX_FACTOR', '100'))]:
    for fmt in ("CSV", "Parquet"):
        df = (spark.read.csv(f"{W}/routes_x{n}.dat", schema=SPARK_R, nullValue="\\N") if fmt == "CSV"
              else spark.read.parquet(f"{W}/routes_x{n}.parquet"))
        df.createOrReplaceTempView("routes")
        ms, res = med(lambda: [tuple(x) for x in spark.sql(Q).collect()])
        record(n, "Spark", fmt, ms, res)
spark.stop()

with open(f"{OUT}/scale_test.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["facteur", "moteur", "format", "taille_octets", "mediane_ms"]); w.writerows(rows)
print("Resultats identiques entre moteurs et formats pour chaque facteur : OUI")
print("SCALE TEST OK ->", f"{OUT}/scale_test.csv")
