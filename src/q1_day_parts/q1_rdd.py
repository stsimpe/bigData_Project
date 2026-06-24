"""Q1: RDD API. Παρόμοιο pipeline αλλά χωρίς schema, με χειροκίνητο CSV parse."""
from __future__ import annotations

import csv
from io import StringIO

from src.common.paths import CRIME_2010_2019_CSV, CRIME_2020_2025_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def day_part(time_str):
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


def parse_row(line):
    # Χρησιμοποιούμε το csv module για να σεβαστούμε quoted fields με κόμματα
    return next(csv.reader(StringIO(line)))


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q1-RDD")
    sc = spark.sparkContext

    with timed("Q1 RDD"):
        rdd_a = sc.textFile(CRIME_2010_2019_CSV)
        rdd_b = sc.textFile(CRIME_2020_2025_CSV)

        # Διαβάζουμε το header μία φορά για να βρούμε τα indices
        header = rdd_a.first()
        cols = parse_row(header)
        time_idx = cols.index("TIME OCC")
        premis_idx = cols.index("Premis Desc")

        # Αφαιρούμε το header από κάθε αρχείο και ενώνουμε
        data = (
            rdd_a.filter(lambda x: x != header)
            .union(rdd_b.filter(lambda x: x != header))
        )

        counts = (
            data
            .map(parse_row)
            .filter(lambda row: len(row) > max(time_idx, premis_idx)
                                and row[premis_idx] == "STREET")
            .map(lambda row: (day_part(row[time_idx]), 1))
            .filter(lambda kv: kv[0] is not None)
            .reduceByKey(lambda a, b: a + b)
        )

        counts.cache()
        total = counts.values().sum()

        result = (
            counts
            .map(lambda kv: (kv[0], kv[1], round(kv[1] / total * 100, 2)))
            .sortBy(lambda x: -x[2])
            .collect()
        )

        # Output σε στυλ DataFrame.show() για ευκολότερη σύγκριση
        print("+--------+------+----------+")
        print("|day_part|crimes|percentage|")
        print("+--------+------+----------+")
        for dp, n, pct in result:
            print(f"|{dp:<8}|{n:<6}|{pct:<10}|")
        print("+--------+------+----------+")

    spark.stop()


if __name__ == "__main__":
    main()
