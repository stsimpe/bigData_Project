"""Επιβεβαιώνει ότι το sample dataset διαβάζεται σωστά από το PySpark.

Τι κάνει: φορτώνει τα paths (mode=local), φτιάχνει SparkSession, διαβάζει το
CSV 2010-2019 ως DataFrame και τυπώνει schema, πλήθος και τις πρώτες γραμμές.
Αν δεις στήλες όπως DR_NO, DATE OCC, TIME OCC, Premis Desc -- όλα ΟΚ.
"""
from __future__ import annotations

from src.common.paths import CRIME_2010_2019_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def main() -> None:
    print(f"[config] {describe()}")
    print(f"[config] CRIME_2010_2019_CSV = {CRIME_2010_2019_CSV}")

    spark = build_session("verify-data")

    with timed("CSV read + count"):
        df = spark.read.csv(CRIME_2010_2019_CSV, header=True, inferSchema=False)
        n = df.count()

    print(f"\nRows: {n:,}")
    print(f"Columns ({len(df.columns)}):")
    for c in df.columns:
        print(f"  - {c!r}")
    print("\nFirst 5 rows:")
    df.show(5, truncate=40)

    spark.stop()


if __name__ == "__main__":
    main()
