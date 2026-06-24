"""Q3: RDD API. Διαβάζουμε το GeoJSON ως whole-file string, παρσάρουμε με json
και κάνουμε flatMap πάνω στο features array."""
from __future__ import annotations

import json

from src.common.paths import CENSUS_BLOCKS_GEOJSON, INCOME_2021_CSV, describe
from src.common.spark_session import build_session
from src.common.timing import timed


def clean_income(s):
    if not s:
        return None
    try:
        return int(s.replace("$", "").replace(",", "").strip())
    except ValueError:
        return None


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q3-RDD")
    sc = spark.sparkContext

    with timed("Q3 RDD"):
        # wholeTextFiles διαβάζει κάθε αρχείο σαν ένα (filepath, content) ζεύγος,
        # έτσι ώστε να μπορέσουμε να το παρσάρουμε ολόκληρο σαν JSON.
        features = (
            sc.wholeTextFiles(CENSUS_BLOCKS_GEOJSON)
            .flatMap(lambda kv: json.loads(kv[1])["features"])
            .map(lambda f: f["properties"])
        )

        by_zip = (
            features
            .filter(lambda p: p.get("ZCTA20"))
            .map(lambda p: (
                p["ZCTA20"],
                (p.get("POP20") or 0, p.get("HOUSING20") or 0),
            ))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .filter(lambda kv: kv[1][0] > 0)
        )

        # Income CSV: delimiter ';'
        lines = sc.textFile(INCOME_2021_CSV)
        header = lines.first()
        income = (
            lines.filter(lambda x: x != header)
            .map(lambda line: line.split(";"))
            .filter(lambda parts: len(parts) >= 3)
            .map(lambda parts: (parts[0], clean_income(parts[2])))
            .filter(lambda kv: kv[1] is not None)
        )

        # Join in zip: (zip, ((pop, households), median_income))
        joined = by_zip.join(income)

        result = (
            joined
            .map(lambda kv: (
                kv[0],           # zip
                kv[1][0][0],     # pop
                kv[1][0][1],     # households
                kv[1][1],        # median_income
                round(kv[1][1] * kv[1][0][1] / kv[1][0][0], 2),
            ))
            .sortBy(lambda r: -r[4])
            .take(20)
        )

        print("+-----+---------+----------------+-------------+-----------------+")
        print("|zip  |total_pop|total_households|median_income|per_capita_income|")
        print("+-----+---------+----------------+-------------+-----------------+")
        for row in result:
            print(f"|{row[0]:<5}|{row[1]:<9}|{row[2]:<16}|{row[3]:<13}|{row[4]:<17}|")
        print("+-----+---------+----------------+-------------+-----------------+")

    spark.stop()


if __name__ == "__main__":
    main()
