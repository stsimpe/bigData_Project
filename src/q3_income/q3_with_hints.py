"""Q3 με διαφορετικές join strategies (BROADCAST/MERGE/SHUFFLE_HASH/SHUFFLE_REPLICATE_NL).

Τυπώνει explain() + timing για κάθε στρατηγική ώστε να συγκρίνουμε.
Ζητούμενο 6 της εκφώνησης.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.paths import describe
from src.common.spark_session import build_session
from src.common.timing import timed
from src.q3_income.q3_dataframe import load_census_blocks, load_income


HINTS = [None, "BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]


def run_with(by_zip, income, hint):
    target = income.hint(hint) if hint else income
    return (
        by_zip
        .join(target, on="zip", how="inner")
        .withColumn(
            "per_capita_income",
            F.round(
                F.col("median_income") * F.col("total_households") / F.col("total_pop"),
                2,
            ),
        )
        .orderBy(F.col("per_capita_income").desc())
    )


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q3-Hints")

    blocks = load_census_blocks(spark)
    income = load_income(spark)

    by_zip = (
        blocks
        .groupBy("zip")
        .agg(
            F.sum("pop").alias("total_pop"),
            F.sum("households").alias("total_households"),
        )
        .filter(F.col("total_pop") > 0)
    )

    # Warm-up: cache + materialize ώστε οι μετρήσεις να συγκρίνουν μόνο
    # το join, όχι το read + aggregate.
    by_zip.cache()
    income.cache()
    by_zip.count()
    income.count()

    for hint in HINTS:
        label = hint or "(default)"
        print(f"\n=== Hint: {label} ===")
        result = run_with(by_zip, income, hint)
        result.explain()
        with timed(f"Q3 hint={label}"):
            result.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
