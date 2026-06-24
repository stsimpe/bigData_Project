"""Κοινός builder για SparkSession (κρατάμε τα executor configs εκτός
κώδικα, για να δίνονται από spark-submit)."""
from pyspark.sql import SparkSession


def build_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
