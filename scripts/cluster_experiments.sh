#!/usr/bin/env bash
# Τρέχει τα 3 υπόλοιπα πειράματα στο cluster:
#   1. Q4 scalability (6 configs: vertical + horizontal scaling)
#   2. CSV → Parquet conversion + Q1 comparison
#   3. Q3 + Q4 με join hints (Ζητούμενο 6)

set -euo pipefail

PROJECT_DIR="/mnt/c/Users/tsimp/bigdata_project"
USER_NAME="dsml00318"
HDFS_CODE="hdfs://hdfs-namenode:9000/user/${USER_NAME}/code"
RESULTS_DIR="${PROJECT_DIR}/results"
mkdir -p "$RESULTS_DIR" "${RESULTS_DIR}/logs"

# Τα jobs διαβάζουν από HDFS — το DATA_MODE πρέπει να φτάσει στον driver pod.
export DATA_MODE=hdfs
export HADOOP_USER_NAME="$USER_NAME"
SPARK_CONF=(
    --conf spark.kubernetes.driverEnv.DATA_MODE=hdfs
    --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME="$USER_NAME"
)

cd "$PROJECT_DIR"

# ---- Refresh src.zip στο HDFS ----
echo "[refresh] Updating src.zip and query scripts on HDFS ..."
rm -f /tmp/src.zip
zip -qr /tmp/src.zip src/
hdfs dfs -put -f /tmp/src.zip "${HDFS_CODE}/src.zip"

# ---- Πείραμα 1: Q4 scalability ----
SCALA="${RESULTS_DIR}/q4_scalability.csv"
echo "label,executors,cores,memory,wall_seconds,app_id" > "$SCALA"

declare -a Q4_RUNS=(
    # Part A: 2 executors, διαφορετική κάθετη κλιμάκωση
    "Q4-A1|q4_dataframe.py|2|1|2g"
    "Q4-A2|q4_dataframe.py|2|2|4g"
    "Q4-A3|q4_dataframe.py|2|4|8g"
    # Part B: σταθεροί συνολικοί πόροι 8 cores / 16GB, διαφορετική οριζόντια
    "Q4-B1|q4_dataframe.py|2|4|8g"   # ίδιο με A3
    "Q4-B2|q4_dataframe.py|4|2|4g"
    "Q4-B3|q4_dataframe.py|8|1|2g"
)

echo ""
echo "=== Πείραμα 1: Q4 scalability ==="
for run in "${Q4_RUNS[@]}"; do
    IFS='|' read -r label script execs cores mem <<< "$run"
    echo ">>> $label ($execs x $cores cores x $mem)"
    start=$(date +%s)
    output=$(spark-submit \
        "${SPARK_CONF[@]}" \
        --num-executors "$execs" \
        --executor-cores "$cores" \
        --executor-memory "$mem" \
        --py-files "${HDFS_CODE}/src.zip" \
        "${HDFS_CODE}/${script}" 2>&1 || true)
    end=$(date +%s)
    elapsed=$((end - start))
    printf '%s\n' "$output" > "${RESULTS_DIR}/logs/${label}.log"
    app_id=$(echo "$output" | grep -oP 'spark-[a-f0-9]{32}' | head -1 || echo "?")
    echo "    -> ${elapsed}s, app_id=${app_id}"
    echo "${label},${execs},${cores},${mem},${elapsed},${app_id}" >> "$SCALA"
done

# ---- Πείραμα 2: CSV → Parquet ----
echo ""
echo "=== Πείραμα 2: CSV → Parquet conversion + Q1 comparison ==="
hdfs dfs -put -f src/conversion/csv_to_parquet.py "${HDFS_CODE}/csv_to_parquet.py"

PARQ="${RESULTS_DIR}/parquet_comparison.csv"
echo "label,wall_seconds,app_id" > "$PARQ"

start=$(date +%s)
output=$(spark-submit \
    "${SPARK_CONF[@]}" \
    --num-executors 2 --executor-cores 1 --executor-memory 2g \
    --py-files "${HDFS_CODE}/src.zip" \
    "${HDFS_CODE}/csv_to_parquet.py" 2>&1 || true)
end=$(date +%s)
elapsed=$((end - start))
printf '%s\n' "$output" > "${RESULTS_DIR}/logs/Format-Comparison.log"
app_id=$(echo "$output" | grep -oP 'spark-[a-f0-9]{32}' | head -1 || echo "?")
echo "Format-Comparison -> ${elapsed}s"
echo "Format-Comparison,${elapsed},${app_id}" >> "$PARQ"

# ---- Πείραμα 3: Join hints ----
echo ""
echo "=== Πείραμα 3: Join hints comparison ==="
hdfs dfs -put -f src/q3_income/q3_with_hints.py "${HDFS_CODE}/q3_with_hints.py"
hdfs dfs -put -f src/q4_nearest_station/q4_with_hints.py "${HDFS_CODE}/q4_with_hints.py"

HINTS="${RESULTS_DIR}/hints_comparison.csv"
echo "label,wall_seconds,app_id" > "$HINTS"

for script_label in "Q3-Hints|q3_with_hints.py" "Q4-Hints|q4_with_hints.py"; do
    IFS='|' read -r label script <<< "$script_label"
    echo ">>> $label"
    start=$(date +%s)
    output=$(spark-submit \
        "${SPARK_CONF[@]}" \
        --num-executors 3 --executor-cores 1 --executor-memory 2g \
        --py-files "${HDFS_CODE}/src.zip" \
        "${HDFS_CODE}/${script}" 2>&1 || true)
    end=$(date +%s)
    elapsed=$((end - start))
    # Τα per-hint timings είναι στις [TIMER] γραμμές του output (όχι στο wall_seconds).
    printf '%s\n' "$output" > "${RESULTS_DIR}/logs/${label}.log"
    printf '%s\n' "$output" | grep -E '^\[TIMER\]' >> "${RESULTS_DIR}/logs/${label}_timers.txt" || true
    app_id=$(echo "$output" | grep -oP 'spark-[a-f0-9]{32}' | head -1 || echo "?")
    echo "    -> ${elapsed}s"
    echo "${label},${elapsed},${app_id}" >> "$HINTS"
done

echo ""
echo "=== Όλα τα αποτελέσματα ==="
echo "--- Q4 scalability ---"
cat "$SCALA"
echo ""
echo "--- Parquet comparison ---"
cat "$PARQ"
echo ""
echo "--- Hints comparison ---"
cat "$HINTS"
