"""Q1: ίδιο με q1_dataframe αλλά με Python UDF για το categorization.

Αναμενόμενα πιο αργό γιατί η UDF παίζει εκτός Catalyst (serialize κάθε row
από JVM σε Python και πίσω).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.common.paths import CRIME_2010_2019_CSV, CRIME_2020_2025_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def day_part_py(time_str):
    if time_str is None:
        return None
    try:
        t = int(time_str)
    except (ValueError, TypeError):
        return None
    if 500 <= t < 1200:
        return "Πρωί"
    if 1200 <= t < 1700:
        return "Απόγευμα"
    if 1700 <= t < 2100:
        return "Βράδυ"
    if t >= 2100 or t < 500:
        return "Νύχτα"
    return None


def load_crimes(spark: SparkSession) -> DataFrame:
    cols = ["TIME OCC", "Premis Desc"]
    df1 = spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
    df2 = spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
    return df1.unionByName(df2)


def run_query(crimes: DataFrame) -> DataFrame:
    day_part_udf = F.udf(day_part_py, StringType())

    street = (
        crimes
        .filter(F.col("Premis Desc") == "STREET")
        .withColumn("day_part", day_part_udf(F.col("TIME OCC")))
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
    spark = build_session("Q1-DataFrame-UDF")
    crimes = load_crimes(spark)

    with timed("Q1 DataFrame (with UDF)"):
        run_query(crimes).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
