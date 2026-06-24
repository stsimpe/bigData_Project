"""Paths για τα datasets της εργασίας.

DATA_MODE=local (default): διαβάζει από data/sample/ ή data/full/ ανάλογα με LOCAL_VARIANT.
DATA_MODE=hdfs: διαβάζει από το HDFS του cluster.
"""
import os
from pathlib import Path

DATA_MODE = os.environ.get("DATA_MODE", "local").lower()
LOCAL_VARIANT = os.environ.get("LOCAL_VARIANT", "sample").lower()
USER = os.environ.get("HADOOP_USER_NAME", "user")

_ROOT = Path(__file__).resolve().parents[2]
_LOCAL = _ROOT / "data" / LOCAL_VARIANT
HDFS_PREFIX = "hdfs://hdfs-namenode:9000"


def _p(local_name, hdfs_rel):
    if DATA_MODE == "hdfs":
        return f"{HDFS_PREFIX}{hdfs_rel}"
    return str(_LOCAL / local_name)


CRIME_2010_2019_CSV = _p(
    "LA_Crime_Data_2010_2019.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
)
CRIME_2020_2025_CSV = _p(
    "LA_Crime_Data_2020_2025.csv",
    "/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
)
CENSUS_BLOCKS_GEOJSON = _p(
    "LA_Census_Blocks_2020.geojson",
    "/data/LA_Census_Blocks_2020.geojson",
)
CENSUS_FIELDS_CSV = _p(
    "LA_Census_Blocks_2020_fields.csv",
    "/data/LA_Census_Blocks_2020_fields.csv",
)
INCOME_2021_CSV = _p(
    "LA_income_2021.csv",
    "/data/LA_income_2021.csv",
)
POLICE_STATIONS_CSV = _p(
    "LA_Police_Stations.csv",
    "/data/LA_Police_Stations.csv",
)

if DATA_MODE == "hdfs":
    CRIME_PARQUET_DIR = f"{HDFS_PREFIX}/user/{USER}/crime_parquet"
    RESULTS_DIR = f"{HDFS_PREFIX}/user/{USER}/results"
else:
    CRIME_PARQUET_DIR = str(_ROOT / "data" / "parquet")
    RESULTS_DIR = str(_ROOT / "results")


def describe():
    return f"DATA_MODE={DATA_MODE} LOCAL_VARIANT={LOCAL_VARIANT} USER={USER}"
