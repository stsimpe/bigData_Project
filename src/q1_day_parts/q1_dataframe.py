"""Q1: ποσοστά STREET εγκλημάτων ανά τμήμα ημέρας (DataFrame API)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import CRIME_2010_2019_CSV, CRIME_2020_2025_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def day_part(time_col="TIME OCC"):
    # TIME OCC είναι 4-digit military time σαν string ('0045', '1350', '2100')
    t = F.col(time_col).cast("int")
    return (
        F.when((t >= 500) & (t < 1200), "Πρωί")
         .when((t >= 1200) & (t < 1700), "Απόγευμα")
         .when((t >= 1700) & (t < 2100), "Βράδυ")
         .when((t >= 2100) | (t < 500), "Νύχτα")
    )


def load_crimes(spark: SparkSession) -> DataFrame:
    cols = ["TIME OCC", "Premis Desc"]
    df1 = spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
    df2 = spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
    return df1.unionByName(df2)


def run_query(crimes: DataFrame) -> DataFrame:
    street = (
        crimes
        .filter(F.col("Premis Desc") == "STREET")
        .withColumn("day_part", day_part())
    )

    counts = street.groupBy("day_part").agg(F.count("*").alias("crimes"))
    counts.cache()
    total = counts.agg(F.sum("crimes")).first()[0]

    return (
        counts
        .withColumn("percentage", F.round(F.col("crimes") / F.lit(total) * 100, 2))
        .orderBy(F.col("percentage").desc())
    )


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q1-DataFrame")
    crimes = load_crimes(spark)

    with timed("Q1 DataFrame (no UDF)"):
        run_query(crimes).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
