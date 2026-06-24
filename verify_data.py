"""Επιβεβαιώνει ότι το sample dataset διαβάζεται σωστά από το PySpark.

Run: άνοιξε αυτό το αρχείο στο VS Code και πάτα F5.

Τι κάνει:
  1. Φορτώνει τα paths από το `src.common.paths` (mode = local).
  2. Δημιουργεί SparkSession.
  3. Διαβάζει το CSV του 2010-2019 ως DataFrame.
  4. Τυπώνει schema, μέγεθος, και τις πρώτες γραμμές.

Αν δεις schema με στήλες όπως `DR_NO`, `DATE OCC`, `TIME OCC`, `AREA NAME`,
`Premis Desc` κλπ. — το pipeline είναι έτοιμο για Query 1.
"""
from __future__ import annotations

from src.common.paths import CRIME_2010_2019_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def main() -> None:
    print(f"[config] {describe()}")
    print(f"[config] CRIME_2010_2019_CSV = {CRIME_2010_2019_CSV}")
    print()

    spark = build_session("verify-data")

    with timed("CSV read + count"):
        df = spark.read.csv(CRIME_2010_2019_CSV, header=True, inferSchema=False)
        n = df.count()

    print(f"\nRows: {n:,}")
    print(f"Columns ({len(df.columns)}):")
    for c in df.columns:
        print(f"  - {c!r}")
    print()
    print("First 5 rows (truncated):")
    df.show(5, truncate=40)

    spark.stop()


if __name__ == "__main__":
    main()
