#!/usr/bin/env bash
# Τρέχει το Query 4 με όλα τα executor configurations που ζητάει
# το Ζητούμενο 5 (κάθετη + οριζόντια κλιμακωσιμότητα).
#
# Τα αποτελέσματα timing γράφονται σε CSV ώστε να μπουν αυτούσια
# στους πίνακες της αναφοράς.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="src/q4_nearest_station/q4_dataframe.py"
OUT="$ROOT/results/q4_scalability.csv"
mkdir -p "$(dirname "$OUT")"
echo "executors,cores,memory,seconds" > "$OUT"

run() {
    local execs="$1" cores="$2" mem="$3"
    echo ">>> Q4 with $execs executors, $cores cores, $mem memory"
    local log
    log=$( "$ROOT/scripts/submit.sh" \
        --executors "$execs" --cores "$cores" --memory "$mem" \
        "$SCRIPT" 2>&1 | tee /dev/stderr )

    local secs
    secs=$(echo "$log" | awk -F': ' '/\[TIMER\] Q4/ {print $2}' | sed 's/ s$//')
    echo "$execs,$cores,$mem,$secs" >> "$OUT"
}

# A: 2 executors, διαφορετική κάθετη κλιμάκωση
run 2 1 2g
run 2 2 4g
run 2 4 8g

# B: σταθεροί συνολικοί πόροι 8 cores / 16GB, διαφορετική οριζόντια κατανομή
run 2 4 8g     # ίδιο με τελευταίο του A
run 4 2 4g
run 8 1 2g

echo "Done. Results: $OUT"
