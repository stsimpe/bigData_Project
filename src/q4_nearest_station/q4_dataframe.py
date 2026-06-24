"""Q4: nearest police station ανά έγκλημα + μέση απόσταση (DataFrame API)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import (
    CRIME_2010_2019_CSV,
    CRIME_2020_2025_CSV,
    POLICE_STATIONS_CSV,
    describe,
)
from src.common.spark_session import build_session
from src.common.timing import timed

EARTH_RADIUS_KM = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    """Απόσταση σε χλμ μεταξύ δύο σημείων (lat,lon) σε μοίρες."""
    lat1r = F.radians(lat1)
    lat2r = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1r) * F.cos(lat2r) * F.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def load_crimes(spark: SparkSession) -> DataFrame:
    cols = ["LAT", "LON"]
    df1 = spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
    df2 = spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
    return (
        df1.unionByName(df2)
        .select(
            F.col("LAT").cast("double").alias("lat"),
            F.col("LON").cast("double").alias("lon"),
        )
        # (0, 0) είναι placeholder στο dataset για άγνωστη τοποθεσία
        .filter((F.col("lat") != 0) & (F.col("lon") != 0))
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    )


def load_stations(spark: SparkSession) -> DataFrame:
    # Στο police stations CSV: X = longitude, Y = latitude
    return (
        spark.read.csv(POLICE_STATIONS_CSV, header=True)
        .select(
            F.col("DIVISION"),
            F.col("Y").cast("double").alias("st_lat"),
            F.col("X").cast("double").alias("st_lon"),
        )
    )


def run_query(crimes: DataFrame, stations: DataFrame) -> DataFrame:
    crimes_id = crimes.withColumn("crime_id", F.monotonically_increasing_id())

    # Broadcast: ο stations DataFrame είναι μόνο 21 γραμμές
    pairs = crimes_id.crossJoin(F.broadcast(stations))
    pairs = pairs.withColumn(
        "dist",
        haversine(F.col("lat"), F.col("lon"), F.col("st_lat"), F.col("st_lon")),
    )

    # Για κάθε έγκλημα κρατάμε τη ζευγαρωμένη απόσταση/τμήμα με την μικρότερη
    # απόσταση. Το F.min(struct(...)) επιστρέφει το struct με το μικρότερο
    # πρώτο πεδίο, οπότε προτάσσουμε το dist.
    nearest = (
        pairs
        .groupBy("crime_id")
        .agg(F.min(F.struct("dist", "DIVISION")).alias("m"))
        .select(F.col("m.DIVISION").alias("division"), F.col("m.dist").alias("dist"))
    )

    return (
        nearest
        .groupBy("division")
        .agg(
            F.round(F.avg("dist"), 3).alias("average_distance"),
            F.count("*").alias("#"),
        )
        .orderBy(F.col("#").desc())
    )


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q4-DataFrame")
    crimes = load_crimes(spark)
    stations = load_stations(spark)

    with timed("Q4 DataFrame"):
        run_query(crimes, stations).show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
