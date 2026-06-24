"""Q2: Top 3 μήνες με τα περισσότερα εγκλήματα ανά έτος (DataFrame API)
Στρατηγική:
    1. column pruning νωρίς — κρατάμε μόνο `DATE OCC`.
    2. `to_date()` για robust parsing του format.
    3. groupBy(year, month) → count.
    4. window function ανά έτος για ranking — ΚΑΤΑΛΛΗΛΗ χρήση
       window εδώ (partitionBy="year"), σε αντίθεση με το Q1.
    5. filter ranking <= 3.
    6. order by year asc, ranking asc.
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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_crimes(spark: SparkSession) -> DataFrame:
    """Φορτώνει μόνο τη στήλη DATE OCC από τα δύο CSVs."""
    cols = ["DATE OCC"]
    df1 = spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
    df2 = spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
    return df1.unionByName(df2)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------
def run_query(crimes: DataFrame) -> DataFrame:
    # Parse την ημερομηνία. Χρησιμοποιούμε `to_timestamp` αντί `to_date`
    # γιατί το format περιλαμβάνει ώρα + AM/PM (`a`). Μετά εξάγουμε
    # year & month.
    with_date = (
        crimes
        .filter(F.col("DATE OCC").isNotNull())
        .withColumn(
            "_ts", F.to_timestamp(F.col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")
        )
        .filter(F.col("_ts").isNotNull())  # πέταξε rows που δεν παρσαρανε
        .withColumn("year", F.year("_ts"))
        .withColumn("month", F.month("_ts"))
        .drop("_ts", "DATE OCC")
    )

    # Πλήθος εγκλημάτων ανά (έτος, μήνα).
    monthly = (
        with_date
        .groupBy("year", "month")
        .agg(F.count("*").alias("crime_total"))
    )

    # Ranking ανά έτος, από μεγαλύτερο σε μικρότερο count.
    # row_number() για να μη βγαίνουν duplicates όπως με rank().
    w = Window.partitionBy("year").orderBy(F.col("crime_total").desc())
    ranked = monthly.withColumn("ranking", F.row_number().over(w))

    # Top 3 ανά έτος.
    return (
        ranked
        .filter(F.col("ranking") <= 3)
        .orderBy(F.col("year").asc(), F.col("ranking").asc())
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[config] {describe()}")
    spark = build_session("Q2-DataFrame")

    crimes = load_crimes(spark)

    with timed("Q2 DataFrame"):
        result = run_query(crimes)
        # Στο sample έχουμε ~16 χρόνια × 3 μήνες = ~48 γραμμές. Εμφανίζω
        # όλες με show(50).
        result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
