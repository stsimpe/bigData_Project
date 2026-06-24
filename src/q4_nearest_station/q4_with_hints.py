"""Q4 με διαφορετικές join strategies για το cross-join crimes × stations.

Σε ένα cross join (χωρίς join key) ισχύουν μόνο οι BROADCAST και
SHUFFLE_REPLICATE_NL — οι άλλες δύο (MERGE, SHUFFLE_HASH) χρειάζονται key.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.paths import describe
from src.common.spark_session import build_session
from src.common.timing import timed
from src.q4_nearest_station.q4_dataframe import (
    haversine, load_crimes, load_stations,
)


HINTS = [None, "BROADCAST", "SHUFFLE_REPLICATE_NL"]


def run_with(crimes_id, stations, hint):
    target = stations.hint(hint) if hint else stations
    pairs = crimes_id.crossJoin(target).withColumn(
        "dist",
        haversine(F.col("lat"), F.col("lon"), F.col("st_lat"), F.col("st_lon")),
    )
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
    spark = build_session("Q4-Hints")

    crimes = load_crimes(spark)
    stations = load_stations(spark)

    crimes_id = crimes.withColumn("crime_id", F.monotonically_increasing_id())
    crimes_id.cache()
    stations.cache()
    crimes_id.count()
    stations.count()

    for hint in HINTS:
        label = hint or "(default)"
        print(f"\n=== Hint: {label} ===")
        result = run_with(crimes_id, stations, hint)
        result.explain()
        with timed(f"Q4 hint={label}"):
            result.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
