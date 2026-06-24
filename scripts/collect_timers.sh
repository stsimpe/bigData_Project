#!/usr/bin/env bash
# Παίρνει τα [TIMER] outputs από όλα τα completed driver pods στο namespace,
# και τα γράφει σε CSV για την αναφορά.

set -uo pipefail   # ΟΧΙ -e: θέλουμε να συνεχίζει ακόμη κι αν λείπουν pods/logs

NAMESPACE="dsml00318-priv"
OUT="${1:-$HOME/timers.csv}"
LOG_DIR="$(cd "$(dirname "$0")/.." && pwd)/results/logs"

echo "source,timer_label,seconds" > "$OUT"

# --- Πηγή 1: live driver pods που είναι ακόμη στο namespace ---
pods="$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3=="Completed" {print $1}')"
if [[ -n "$pods" ]]; then
    for pod in $pods; do
        kubectl logs -n "$NAMESPACE" "$pod" 2>/dev/null \
          | grep -E '^\[TIMER\]' \
          | sed -E "s|^\[TIMER\] (.+): ([0-9.]+) s|${pod},\1,\2|" \
          >> "$OUT" || true
    done
fi

# --- Πηγή 2 (fallback): τα αποθηκευμένα logs από τα cluster_*.sh ---
# Χρήσιμο όταν ο k8s GC έχει ήδη σβήσει τα pods (συχνό σε shared cluster).
if [[ -d "$LOG_DIR" ]]; then
    for log in "$LOG_DIR"/*.log; do
        [[ -e "$log" ]] || continue
        base="$(basename "$log" .log)"
        grep -E '^\[TIMER\]' "$log" 2>/dev/null \
          | sed -E "s|^\[TIMER\] (.+): ([0-9.]+) s|${base},\1,\2|" \
          >> "$OUT" || true
    done
fi

echo "---"
cat "$OUT"
lines=$(($(wc -l < "$OUT") - 1))
echo "---"
echo "Συγκεντρώθηκαν ${lines} εγγραφές [TIMER]."
[[ "$lines" -eq 0 ]] && echo "ΠΡΟΣΟΧΗ: καμία εγγραφή — δες αν έτρεξαν τα jobs και αν υπάρχουν logs στο ${LOG_DIR}."
