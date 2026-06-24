"""Q3: μέσο ετήσιο κατά κεφαλήν εισόδημα ανά ZIP για 2020-2021 (DataFrame API)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import CENSUS_BLOCKS_GEOJSON, INCOME_2021_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def load_census_blocks(spark: SparkSession) -> DataFrame:
    # multiLine=true γιατί το geojson είναι ένα ενιαίο JSON object,
    # όχι one-feature-per-line.
    raw = spark.read.option("multiLine", "true").json(CENSUS_BLOCKS_GEOJSON)
    return (
        raw
        .select(F.explode("features").alias("f"))
        .select(
            F.col("f.properties.ZCTA20").alias("zip"),
            F.col("f.properties.POP20").alias("pop"),
            F.col("f.properties.HOUSING20").alias("households"),
        )
        .filter(F.col("zip").isNotNull())
    )


def load_income(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("delimiter", ";").csv(INCOME_2021_CSV, header=True)
    return (
        raw
        .withColumn(
            "median_income",
            F.regexp_replace(F.col("Estimated Median Income"), "[$,]", "").cast("int"),
        )
        .select(F.col("Zip Code").alias("zip"), "median_income")
        .filter(F.col("median_income").isNotNull())
    )


def run_query(blocks: DataFrame, income: DataFrame) -> DataFrame:
    by_zip = (
        blocks
        .groupBy("zip")
        .agg(
            F.sum("pop").alias("total_pop"),
            F.sum("households").alias("total_households"),
        )
        .filter(F.col("total_pop") > 0)
    )

    # per capita = (διάμεσο εισόδημα νοικοκυριού × νοικοκυριά) / πληθυσμός
    return (
        by_zip
        .join(income, on="zip", how="inner")
        .withColumn(
            "per_capita_income",
            F.round(
                F.col("median_income") * F.col("total_households") / F.col("total_pop"),
                2,
            ),
        )
        .select("zip", "total_pop", "total_households", "median_income", "per_capita_income")
        .orderBy(F.col("per_capita_income").desc())
    )


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q3-DataFrame")
    blocks = load_census_blocks(spark)
    income = load_income(spark)

    with timed("Q3 DataFrame"):
        run_query(blocks, income).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
