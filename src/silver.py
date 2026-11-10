"""Etape 2 : nettoyage Spark. Raw CSV -> silver Parquet + table de rejets + rapport qualite."""
import json
from pyspark.sql import functions as F
from spark_common import session, path, read_raw, AIRPORTS, AIRLINES, ROUTES

spark = session("openflights-silver")
airports = read_raw(spark, "airports", AIRPORTS)
airlines = read_raw(spark, "airlines", AIRLINES)
routes = read_raw(spark, "routes", ROUTES)

# --- Typage / normalisation
airlines = (airlines
    .withColumn("iata", F.when(F.col("iata").isin("-", "", "N/A"), None).otherwise(F.col("iata")))
    .withColumn("active", F.when(F.col("active") == "Y", True).when(F.col("active") == "N", False)))
routes = (routes
    .withColumn("codeshare", F.coalesce(F.col("codeshare") == "Y", F.lit(False)))
    .withColumn("stops", F.coalesce(F.col("stops"), F.lit(0))))

# --- Validation des routes : les deux aeroports doivent exister dans airports
ref = airports.select("airport_id", "iata")
s_ref = ref.select(F.col("airport_id").alias("_s"), F.col("iata").alias("_s_iata"))
d_ref = ref.select(F.col("airport_id").alias("_d"), F.col("iata").alias("_d_iata"))
r = (routes
     .join(s_ref, routes.src_id == F.col("_s"), "left").withColumn("src_known", F.col("_s").isNotNull()).drop("_s")
     .join(d_ref, F.col("dst_id") == F.col("_d"), "left").withColumn("dst_known", F.col("_d").isNotNull()).drop("_d"))
reason = (F.when(F.col("src_id").isNull(), "src_id_null")
           .when(F.col("dst_id").isNull(), "dst_id_null")
           .when(~F.col("src_known"), "src_id_inconnu")
           .when(~F.col("dst_known"), "dst_id_inconnu"))
r = r.withColumn("reject_reason", reason).cache()
EXTRA = ["src_known", "dst_known", "_s_iata", "_d_iata"]
valid = r.where(F.col("reject_reason").isNull()).drop(*EXTRA, "reject_reason")
rejected = r.where(F.col("reject_reason").isNotNull()).drop(*EXTRA)

# --- Rapport qualite (comptes reels)
def cnt(df, cond=None):
    return (df.where(cond) if cond is not None else df).count()

n_air, n_line, n_route = airports.count(), airlines.count(), routes.count()
airline_ids = airlines.select("airline_id").where(F.col("airline_id").isNotNull()).distinct()
duplicate_iata = (airports.where(F.col("iata").isNotNull())
                  .groupBy("iata").count().where(F.col("count") > 1).count())
route_columns = ["airline_code", "airline_id", "src_code", "src_id", "dst_code", "dst_id",
                 "codeshare", "stops", "equipment"]
duplicate_routes = n_route - routes.select(*route_columns).distinct().count()
q = {
    "airports_total": n_air,
    "airports_iata_null": cnt(airports, F.col("iata").isNull()),
    "airports_iata_dupliques": duplicate_iata,
    "airports_latitude_hors_plage": cnt(airports, F.col("latitude").isNotNull() & ((F.col("latitude") < -90) | (F.col("latitude") > 90))),
    "airports_longitude_hors_plage": cnt(airports, F.col("longitude").isNotNull() & ((F.col("longitude") < -180) | (F.col("longitude") > 180))),
    "airports_tz_name_null": cnt(airports, F.col("tz_name").isNull()),
    "airlines_total": n_line,
    "airlines_inactives": cnt(airlines, F.col("active") == False),  # noqa: E712
    "airlines_active_inconnu": cnt(airlines, F.col("active").isNull()),
    "airlines_iata_absent": cnt(airlines, F.col("iata").isNull()),
    "routes_total": n_route,
    "routes_src_id_null": cnt(routes, F.col("src_id").isNull()),
    "routes_dst_id_null": cnt(routes, F.col("dst_id").isNull()),
    "routes_airline_id_null": cnt(routes, F.col("airline_id").isNull()),
    "routes_airline_id_inconnu": cnt(routes.where(F.col("airline_id").isNotNull()).join(airline_ids, "airline_id", "left_anti")),
    "routes_exactement_dupliquees": duplicate_routes,
    "routes_src_id_inconnu": cnt(r, F.col("src_id").isNotNull() & ~F.col("src_known")),
    "routes_dst_id_inconnu": cnt(r, F.col("dst_id").isNotNull() & ~F.col("dst_known")),
    "routes_src_code_incoherent": cnt(r, F.col("src_code").isNotNull() & F.col("_s_iata").isNotNull() & (F.col("src_code") != F.col("_s_iata"))),
    "routes_dst_code_incoherent": cnt(r, F.col("dst_code").isNotNull() & F.col("_d_iata").isNotNull() & (F.col("dst_code") != F.col("_d_iata"))),
    "routes_codeshare": cnt(routes, F.col("codeshare")),
    "routes_avec_escale": cnt(routes, F.col("stops") > 0),
    "routes_valides": valid.count(),
    "routes_rejetees": rejected.count(),
}
assert q["routes_valides"] + q["routes_rejetees"] == q["routes_total"], "perte ou doublon de lignes"
for reason_name, n in sorted((x[0], x[1]) for x in rejected.groupBy("reject_reason").count().collect()):
    q["rejet_" + reason_name] = n
print("=== RAPPORT QUALITE ===")
print(json.dumps(q, indent=2, ensure_ascii=False))

# --- Ecriture silver (Parquet)
airports.write.mode("overwrite").parquet(path("silver/airports"))
airlines.write.mode("overwrite").parquet(path("silver/airlines"))
valid.write.mode("overwrite").parquet(path("silver/routes"))
rejected.write.mode("overwrite").parquet(path("silver/routes_rejected"))
spark.createDataFrame([(k, int(v)) for k, v in q.items()], "metrique STRING, valeur LONG") \
     .coalesce(1).write.mode("overwrite").option("header", True).csv(path("silver/quality_report"))
print("SILVER OK routes_valides=%d routes_rejetees=%d" % (q["routes_valides"], q["routes_rejetees"]))
spark.stop()
