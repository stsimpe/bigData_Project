#!/usr/bin/env bash
# Generic wrapper γύρω από spark-submit.
#
# Παράδειγμα:
#     ./scripts/submit.sh --executors 2 --cores 1 --memory 2g \
#         src/q1_day_parts/q1_dataframe.py
#
# Το master/namespace προέρχονται από τις προ-ρυθμίσεις του cluster του
# εργαστηρίου (spark-defaults.conf στο pod). Αν χρειαστεί override, βάζουμε
# επιπλέον --conf εδώ.

set -euo pipefail

EXECUTORS=2
CORES=1
MEMORY=2g

# --- parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --executors) EXECUTORS="$2"; shift 2 ;;
        --cores)     CORES="$2";     shift 2 ;;
        --memory)    MEMORY="$2";    shift 2 ;;
        --) shift; break ;;
        -*) echo "Unknown flag: $1" >&2; exit 1 ;;
        *)  break ;;
    esac
done

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--executors N] [--cores N] [--memory G] <script.py> [args...]" >&2
    exit 1
fi

SCRIPT="$1"; shift

# Φάκελος src/ στο PYTHONPATH ώστε να βρίσκεται το `common` package
# από τα query scripts.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

exec spark-submit \
    --num-executors "$EXECUTORS" \
    --executor-cores "$CORES" \
    --executor-memory "$MEMORY" \
    --conf spark.driver.memory=2g \
    --py-files "$ROOT/src/common/spark_session.py,$ROOT/src/common/paths.py,$ROOT/src/common/timing.py" \
    "$ROOT/$SCRIPT" "$@"
