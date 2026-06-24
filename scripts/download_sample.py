"""Κατεβάζει τοπικά τα δείγματα όλων των datasets που χρησιμοποιεί η εργασία.

Χρήση:

    python scripts/download_sample.py
    python scripts/download_sample.py --rows 50000

Πηγές (όπως αναφέρει η εκφώνηση):
    • LA Crime Data 2010-2019 & 2020-2025 → Socrata API (data.lacity.org)
    • LA Census Blocks 2020 → ArcGIS Hub (data.lacounty.gov)
    • LAPD Police Stations → ArcGIS Hub (geohub.lacity.org)
    • Median Household Income 2021 → laalmanac.com (HTML scraping)

Τα crime αρχεία είναι τεράστια (~500 MB το καθένα), οπότε διαβάζουμε
με streaming και κρατάμε τις πρώτες N γραμμές. Τα υπόλοιπα είναι μικρά
και τα κατεβάζουμε ολόκληρα.

Τα ονόματα στηλών διατηρούνται ίδια με εκείνα στο HDFS του εργαστηρίου
ώστε ο ίδιος query κώδικας να δουλεύει και τοπικά και στο cluster.
"""
from __future__ import annotations

import argparse
import io
import sys
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Crime CSVs — Socrata CSV exports διατηρούν τα original ονόματα στηλών.
# ---------------------------------------------------------------------------
CRIME_DATASETS = {
    "LA_Crime_Data_2010_2019.csv": (
        "https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD"
    ),
    "LA_Crime_Data_2020_2025.csv": (
        "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    ),
}

# ---------------------------------------------------------------------------
# ArcGIS Hub datasets — μικρά αρχεία, τα κατεβάζουμε ολόκληρα.
# ---------------------------------------------------------------------------
ARCGIS_DATASETS = {
    "LA_Census_Blocks_2020.geojson": (
        # LA County Enterprise GIS — 2020 Census Blocks
        "https://opendata.arcgis.com/api/v3/datasets/"
        "8a29319474fe44bb96152d0be8e778af_16/downloads/data"
        "?format=geojson&spatialRefId=4326"
    ),
    "LA_Police_Stations.csv": (
        # LA GeoHub — LAPD Police Stations (21 σταθμοί)
        "https://opendata.arcgis.com/api/v3/datasets/"
        "1dd3271db7bd44f28285041058ac4612_0/downloads/data"
        "?format=csv&spatialRefId=4326"
    ),
}

# ---------------------------------------------------------------------------
# Income data — HTML scraping (η σελίδα δεν προσφέρει direct CSV).
# ---------------------------------------------------------------------------
INCOME_URL = "http://www.laalmanac.com/employment/em12c_2021.php"
INCOME_FILENAME = "LA_income_2021.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _streaming_download(url: str, dest: Path, max_rows: int | None) -> int:
    """Streaming CSV download. Επιστρέφει αριθμό γραμμών που γράφτηκαν."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        for line in resp:
            out.write(line)
            written += 1
            if max_rows is not None and written > max_rows:
                break
    return written


def _full_download(url: str, dest: Path) -> int:
    """Κατεβάζει όλο το αρχείο. Επιστρέφει bytes."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        data = resp.read()
        out.write(data)
        return len(data)


def _scrape_income(url: str, dest: Path) -> int:
    """Παίρνει τον HTML πίνακα από το laalmanac και τον γράφει ως CSV
    με delimiter ';' (όπως ζητάει η εκφώνηση: «ως delimitter, ο
    χαρακτήρας ;»). Επιστρέφει τις γραμμές που γράφτηκαν.

    Το laalmanac μπλοκάρει το default urllib User-Agent. Κατεβάζουμε
    με ρητό browser User-Agent και μετά το δίνουμε στο pandas
    via io.StringIO (αντί να αφήσουμε το pandas να ξανακάνει request).
    """
    try:
        import pandas as pd
    except ImportError:
        print("    ✗ pandas/lxml δεν είναι εγκατεστημένα. Τρέξε: "
              "pip install -r requirements.txt")
        raise

    dest.parent.mkdir(parents=True, exist_ok=True)

    # Κατεβάζουμε χειροκίνητα το HTML με σωστό User-Agent
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    # pandas διαβάζει από string instead of URL ώστε να μη χρειάζεται
    # να ξανακάνει request (που θα ξανά-έβγαζε 403).
    tables = pd.read_html(io.StringIO(html), header=0)
    # Διαλέγουμε τον πιο «πλούσιο» πίνακα: συνήθως αυτός με τα ZIP+income.
    df = max(tables, key=lambda t: len(t.columns) * len(t))
    df.to_csv(dest, sep=";", index=False, encoding="utf-8")
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--rows", type=int, default=100_000,
        help="Όριο γραμμών για τα μεγάλα crime CSVs. 0 για όλο το αρχείο.",
    )
    p.add_argument(
        "--dest", type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "sample",
    )
    p.add_argument(
        "--skip-crime", action="store_true",
        help="Παράκαμψη των crime CSVs (αν τα έχεις ήδη).",
    )
    args = p.parse_args()

    print(f"Destination: {args.dest}")
    print()

    max_rows = None if args.rows == 0 else args.rows

    # Crime CSVs
    if not args.skip_crime:
        for name, url in CRIME_DATASETS.items():
            dest = args.dest / name
            if dest.exists():
                sz = dest.stat().st_size / (1024 * 1024)
                print(f"  ✓ {name} (already exists, {sz:.1f} MB)")
                continue
            try:
                print(f"  → {name} ...", end=" ", flush=True)
                n = _streaming_download(url, dest, max_rows)
                sz = dest.stat().st_size / (1024 * 1024)
                print(f"{n} lines, {sz:.1f} MB")
            except Exception as exc:
                print(f"FAILED: {exc}")

    # ArcGIS Hub datasets (μικρά, ολόκληρα)
    for name, url in ARCGIS_DATASETS.items():
        dest = args.dest / name
        if dest.exists():
            sz = dest.stat().st_size / (1024 * 1024)
            print(f"  ✓ {name} (already exists, {sz:.1f} MB)")
            continue
        try:
            print(f"  → {name} ...", end=" ", flush=True)
            n = _full_download(url, dest)
            sz = n / (1024 * 1024)
            print(f"{sz:.1f} MB")
        except Exception as exc:
            print(f"FAILED: {exc}")

    # Income (HTML scraping)
    dest_income = args.dest / INCOME_FILENAME
    if dest_income.exists():
        print(f"  ✓ {INCOME_FILENAME} (already exists)")
    else:
        try:
            print(f"  → {INCOME_FILENAME} (scraping HTML) ...", end=" ", flush=True)
            n = _scrape_income(INCOME_URL, dest_income)
            print(f"{n} rows")
        except Exception as exc:
            print(f"FAILED: {exc}")
            print(f"    fallback: μπορείς να κάνεις copy-paste τον πίνακα")
            print(f"    από {INCOME_URL} σε CSV με delimiter ';'")

    print()
    print("Done. Files in:", args.dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
