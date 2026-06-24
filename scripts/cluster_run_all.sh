#!/usr/bin/env bash
# Runs all queries στο cslab cluster με τα executor configs που ζητάει η εκφώνηση.
# Σώζει timings σε CSV για την αναφορά.

set -euo pipefail

PROJECT_DIR="/mnt/c/Users/tsimp/bigdata_project"
USER_NAME="dsml00318"
HDFS_CODE="hdfs://hdfs-namenode:9000/user/${USER_NAME}/code"
RESULTS_FILE="${PROJECT_DIR}/results/cluster_timings.csv"

# Τα jobs πρέπει να διαβάζουν από το HDFS, όχι τοπικά. Το env του shell ΔΕΝ
# φτάνει στον driver pod σε k8s cluster mode, οπότε το περνάμε ρητά με --conf
# σε κάθε spark-submit (βλ. SPARK_CONF παρακάτω). Το export εδώ καλύπτει
# client mode / τυχόν τοπικές κλήσεις.
export DATA_MODE=hdfs
export HADOOP_USER_NAME="$USER_NAME"
SPARK_CONF=(
    --conf spark.kubernetes.driverEnv.DATA_MODE=hdfs
    --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME="$USER_NAME"
)

cd "$PROJECT_DIR"
mkdir -p "$(dirname "$RESULTS_FILE")"

# ---- Step 1: συσκευασία και upload κώδικα ----
echo "[1/3] Packaging src/ ..."
rm -f /tmp/src.zip
zip -qr /tmp/src.zip src/
hdfs dfs -put -f /tmp/src.zip "${HDFS_CODE}/src.zip"

echo "[2/3] Uploading individual query scripts ..."
for f in \
    src/q1_day_parts/q1_dataframe.py \
    src/q1_day_parts/q1_dataframe_udf.py \
    src/q1_day_parts/q1_rdd.py \
    src/q2_top_months/q2_dataframe.py \
    src/q2_top_months/q2_sql.py \
    src/q3_income/q3_dataframe.py \
    src/q3_income/q3_rdd.py \
    src/q3_income/q3_with_hints.py \
    src/q4_nearest_station/q4_dataframe.py \
    src/q4_nearest_station/q4_with_hints.py \
    src/conversion/csv_to_parquet.py
do
    hdfs dfs -put -f "$f" "${HDFS_CODE}/$(basename "$f")"
done

# ---- Step 2: ορισμός runs ----
# Format: "label|script|num_execs|cores|mem"
declare -a RUNS=(
    # Q1: 2 executors x 1 core x 2GB
    "Q1-DF|q1_dataframe.py|2|1|2g"
    "Q1-UDF|q1_dataframe_udf.py|2|1|2g"
    "Q1-RDD|q1_rdd.py|2|1|2g"

    # Q2: 4 executors x 1 core x 2GB
    "Q2-DF|q2_dataframe.py|4|1|2g"
    "Q2-SQL|q2_sql.py|4|1|2g"

    # Q3: 3 executors x 1 core x 2GB
    "Q3-DF|q3_dataframe.py|3|1|2g"
    "Q3-RDD|q3_rdd.py|3|1|2g"

    # Q4 default config για αρχή
    "Q4-DF|q4_dataframe.py|2|1|2g"
)

# ---- Step 3: εκτέλεση και timing capture ----
echo "[3/3] Running queries ..."
echo "label,executors,cores,memory,wall_seconds,app_id" > "$RESULTS_FILE"

for run in "${RUNS[@]}"; do
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

    # Σώσε και το raw output κάθε run, για να μπορούμε να ανακτήσουμε τους
    # [TIMER] χρόνους και το app-id ακόμη κι αν τα pods σβηστούν από τον k8s GC.
    mkdir -p "${PROJECT_DIR}/results/logs"
    printf '%s\n' "$output" > "${PROJECT_DIR}/results/logs/${label}.log"

    elapsed=$((end - start))
    app_id=$(echo "$output" | grep -oP 'spark-[a-f0-9]{32}' | head -1 || echo "?")

    echo "    -> ${elapsed}s, app_id=${app_id}"
    echo "${label},${execs},${cores},${mem},${elapsed},${app_id}" >> "$RESULTS_FILE"
done

echo ""
echo "Done. Results:"
cat "$RESULTS_FILE"
