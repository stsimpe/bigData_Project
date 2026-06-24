"""Q2: η ίδια λογική με q2_dataframe, αλλά σε Spark SQL."""
from __future__ import annotations

from src.common.spark_session import build_session
from src.common.timing import timed
from src.common.paths import describe
from src.q2_top_months.q2_dataframe import load_crimes


SQL = """
WITH parsed AS (
    -- DATE OCC: "2010 Feb 20 12:00:00 AM" (HDFS) h "MM/dd/yyyy ..." (sample).
    -- Dokimazoume kai ta dyo formats; kratame mono to date kommati.
    SELECT
        coalesce(
            to_date(substring_index(`DATE OCC`, ' ', 3), 'yyyy MMM dd'),
            to_date(substring_index(`DATE OCC`, ' ', 1), 'MM/dd/yyyy')
        ) AS d
    FROM crimes
    WHERE `DATE OCC` IS NOT NULL
),
monthly AS (
    SELECT year(d) AS year, month(d) AS month, count(*) AS crime_total
    FROM parsed
    WHERE d IS NOT NULL
    GROUP BY year(d), month(d)
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
