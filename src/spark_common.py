"""Session Spark et schemas communs. Parametrable par variables d'environnement sur le cluster Onyxia."""
import os
from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "openflights")
ENDPOINT = os.environ.get("S3_ENDPOINT_URL") or f"https://{os.environ['AWS_S3_ENDPOINT']}"
# Onyxia fournit des identifiants S3 temporaires (avec jeton de session)
PROVIDER = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"

# Schemas explicites (pas d'inferSchema) : \N est lu comme NULL via nullValue
AIRPORTS = ("airport_id INT, name STRING, city STRING, country STRING, iata STRING, icao STRING, "
            "latitude DOUBLE, longitude DOUBLE, altitude_ft INT, tz_offset DOUBLE, dst STRING, "
            "tz_name STRING, type STRING, source STRING")
AIRLINES = ("airline_id INT, name STRING, alias STRING, iata STRING, icao STRING, callsign STRING, "
            "country STRING, active STRING")
ROUTES = ("airline_code STRING, airline_id INT, src_code STRING, src_id INT, dst_code STRING, "
          "dst_id INT, codeshare STRING, stops INT, equipment STRING")


def path(sub):
    return f"s3a://{BUCKET}/{PREFIX}/{sub}"


def session(app, shuffle_partitions=8):
    b = (
        SparkSession.builder.appName(app).master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", PROVIDER)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def read_raw(spark, name, schema):
    return spark.read.csv(path(f"raw/{name}.dat"), schema=schema, nullValue="\\N", quote='"', escape='"')
