"""Etape 3 : table finale gold.hubs (classement des aeroports par routes sortantes, avant/apres nettoyage)."""
from pyspark.sql import functions as F, Window
from spark_common import session, path, read_raw, ROUTES

spark = session("openflights-gold")
airports = spark.read.parquet(path("silver/airports"))
routes = spark.read.parquet(path("silver/routes"))          # routes valides uniquement
raw_routes = read_raw(spark, "routes", ROUTES)               # brut, pour le classement "sans nettoyage"

out = routes.groupBy("src_id").agg(
    F.count("*").alias("routes_out"),
    F.countDistinct("dst_id").alias("destinations_distinctes"),
    F.countDistinct("airline_id").alias("airlines_out"))
out_nocodeshare = (routes.where(~F.col("codeshare"))
                   .groupBy("src_id").agg(
                       F.count("*").alias("routes_out_sans_codeshare"),
                       F.countDistinct("dst_id").alias("destinations_distinctes_sans_codeshare"))
                   .withColumnRenamed("src_id", "_nc_src"))
inn = routes.groupBy("dst_id").agg(F.count("*").alias("routes_in"))
raw = (raw_routes.where(F.col("src_code").isNotNull()).groupBy("src_code").agg(F.count("*").alias("routes_out_raw")))

hubs = (airports.select("airport_id", "iata", "name", "city", "country")
        .join(out, airports.airport_id == out.src_id, "inner").drop("src_id")
        .join(out_nocodeshare, airports.airport_id == F.col("_nc_src"), "left").drop("_nc_src")
        .join(inn, airports.airport_id == inn.dst_id, "left").drop("dst_id")
        .join(raw, airports.iata == raw.src_code, "left").drop("src_code")
        .withColumn("routes_in", F.coalesce("routes_in", F.lit(0)))
        .withColumn("routes_out_sans_codeshare", F.coalesce("routes_out_sans_codeshare", F.lit(0)))
        .withColumn("destinations_distinctes_sans_codeshare", F.coalesce("destinations_distinctes_sans_codeshare", F.lit(0)))
        .withColumn("routes_out_raw", F.coalesce("routes_out_raw", F.lit(0))))
hubs = (hubs
        .withColumn("rank_clean", F.rank().over(Window.orderBy(F.col("routes_out").desc())))
        .withColumn("rank_raw", F.rank().over(Window.orderBy(F.col("routes_out_raw").desc())))
        .withColumn("rank_shift", F.col("rank_raw") - F.col("rank_clean")))
assert hubs.count() == hubs.select("airport_id").distinct().count(), "doublons apres jointure (IATA dupliques ?)"
hubs.coalesce(1).write.mode("overwrite").parquet(path("gold/hubs"))

print("=== TOP 10 HUBS (apres nettoyage) ===")
hubs.orderBy("rank_clean", "airport_id").select("rank_clean", "iata", "name", "country", "routes_out", "routes_out_raw", "rank_raw", "airlines_out").show(10, truncate=False)
print("=== MOUVEMENTS DE RANG (|rank_raw - rank_clean| > 0 parmi les 20 premiers) ===")
hubs.where((F.col("rank_clean") <= 20) & (F.col("rank_shift") != 0)).orderBy("rank_clean").select("rank_clean", "iata", "rank_raw", "rank_shift").show(20, truncate=False)
print("GOLD OK hubs=%d" % hubs.count())
spark.stop()
