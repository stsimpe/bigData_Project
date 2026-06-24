"""Ζητούμενο 1: μετατροπή των crime CSV σε Parquet και σύγκριση επίδοσης
ενός query (Q1) ανάμεσα στα δύο formats.

Το Parquet είναι columnar + συμπιεσμένο: γρηγορότερο read όταν διαβάζουμε
λίγες στήλες και κερδίζει σε I/O χρόνο. Επίσης φέρει schema, οπότε δεν
χρειάζεται type inference.
"""
from __future__ import annotations

from src.common.paths import (
    CRIME_2010_2019_CSV,
    CRIME_2020_2025_CSV,
    CRIME_PARQUET_DIR,
    describe,
)
from src.common.spark_session import build_session
from src.common.timing import timed
from src.q1_day_parts.q1_dataframe import run_query


CSV_FILES = [
    (CRIME_2010_2019_CSV, f"{CRIME_PARQUET_DIR}/2010_2019"),
    (CRIME_2020_2025_CSV, f"{CRIME_PARQUET_DIR}/2020_2025"),
]


def convert(spark):
    for csv_path, parquet_path in CSV_FILES:
        spark.read.csv(csv_path, header=True).write.mode("overwrite").parquet(parquet_path)


def load_csv(spark):
    cols = ["TIME OCC", "Premis Desc"]
    return (
        spark.read.csv(CRIME_2010_2019_CSV, header=True).select(*cols)
        .unionByName(
            spark.read.csv(CRIME_2020_2025_CSV, header=True).select(*cols)
        )
    )


def load_parquet(spark):
    cols = ["TIME OCC", "Premis Desc"]
    return (
        spark.read.parquet(f"{CRIME_PARQUET_DIR}/2010_2019").select(*cols)
        .unionByName(
            spark.read.parquet(f"{CRIME_PARQUET_DIR}/2020_2025").select(*cols)
        )
    )


def main():
    print(f"[config] {describe()}")
    spark = build_session("Format-Comparison")

    with timed("CSV → Parquet conversion"):
        convert(spark)

    print("\n--- Q1 από CSV ---")
    with timed("Q1 on CSV"):
        run_query(load_csv(spark)).show(truncate=False)

    print("\n--- Q1 από Parquet ---")
    with timed("Q1 on Parquet"):
        run_query(load_parquet(spark)).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
