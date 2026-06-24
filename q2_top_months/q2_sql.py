"""Q2: η ίδια λογική με q2_dataframe, αλλά σε Spark SQL."""
from __future__ import annotations

from src.common.spark_session import build_session
from src.common.timing import timed
from src.common.paths import describe
from src.q2_top_months.q2_dataframe import load_crimes


SQL = """
WITH parsed AS (
    SELECT
        year(to_date(substring_index(`DATE OCC`, ' ', 1), 'MM/dd/yyyy'))  AS year,
        month(to_date(substring_index(`DATE OCC`, ' ', 1), 'MM/dd/yyyy')) AS month
    FROM crimes
    WHERE `DATE OCC` IS NOT NULL
),
monthly AS (
    SELECT year, month, count(*) AS crime_total
    FROM parsed
    WHERE year IS NOT NULL
    GROUP BY year, month
),
ranked AS (
    SELECT
        year, month, crime_total,
        row_number() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
    FROM monthly
)
SELECT year, month, crime_total, ranking
FROM ranked
WHERE ranking <= 3
ORDER BY year ASC, ranking ASC
"""


def main():
    print(f"[config] {describe()}")
    spark = build_session("Q2-SQL")

    crimes = load_crimes(spark)
    crimes.createOrReplaceTempView("crimes")

    with timed("Q2 SQL"):
        spark.sql(SQL).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
