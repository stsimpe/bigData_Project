"""Q2: Top 3 μήνες με τα περισσότερα εγκλήματα ανά έτος (DataFrame API).

Στρατηγική: column pruning στο `DATE OCC`, parsing της ημερομηνίας,
groupBy(year, month) -> count, window ranking ανά έτος, filter <= 3.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.paths import (
    CRIME_2010_2019_CSV,
    CRIME_2020_2025_CSV,
    describe,
)
from src.common.spark_session import build_session
from src.common.timing import timed


def load_crimes(spark: SparkSession) -> DataFrame:
    """Φορτώνει μόνο τη στήλη DATE OCC από τα δύο CSVs."""
    cols = ["DATE OCC"]
    df1 = spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
    df2 = spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
    return df1.unionByName(df2)


def run_query(crimes: DataFrame) -> DataFrame:
    with_date = (
        crimes
        .filter(F.col("DATE OCC").isNotNull())
        # Το DATE OCC στα HDFS δεδομένα είναι "2010 Feb 20 12:00:00 AM"
        # (yyyy MMM dd ...), ενώ στο τοπικό sample "MM/dd/yyyy ...". Κρατάμε
        # μόνο το date κομμάτι και δοκιμάζουμε και τα δύο formats με coalesce,
        # ώστε ο κώδικας να δουλεύει και στο cluster και τοπικά. Η ώρα είναι
        # πάντα μεσάνυχτα (dummy), οπότε δεν χρειάζεται AM/PM timestamp parsing.
        .withColumn(
            "_d",
            F.coalesce(
                F.to_date(F.substring_index(F.col("DATE OCC"), " ", 3), "yyyy MMM dd"),
                F.to_date(F.substring_index(F.col("DATE OCC"), " ", 1), "MM/dd/yyyy"),
            ),
        )
        .filter(F.col("_d").isNotNull())
        .withColumn("year", F.year("_d"))
        .withColumn("month", F.month("_d"))
        .drop("_d", "DATE OCC")
    )

    monthly = (
        with_date
        .groupBy("year", "month")
        .agg(F.count("*").alias("crime_total"))
    )

    # row_number() ανά έτος ώστε σε ισοβαθμία να βγαίνουν ακριβώς 3 γραμμές.
    w = Window.partitionBy("year").orderBy(F.col("crime_total").desc())
    ranked = monthly.withColumn("ranking", F.row_number().over(w))

    return (
        ranked
        .filter(F.col("ranking") <= 3)
        .orderBy(F.col("year").asc(), F.col("ranking").asc())
    )


def main() -> None:
    print(f"[config] {describe()}")
    spark = build_session("Q2-DataFrame")

    crimes = load_crimes(spark)

    with timed("Q2 DataFrame"):
        result = run_query(crimes)
        result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
