# SETUP — Πώς να τρέξεις το project σε νέο μηχάνημα

Οδηγός εκτέλεσης της εργασίας. Το project τρέχει σε **δύο modes**, που
επιλέγονται από την env variable `DATA_MODE` (διαβάζεται στο
`src/common/paths.py`):

| Mode | `DATA_MODE` | Δεδομένα | Σκοπός |
|------|-------------|----------|--------|
| Local | `local` (default) | `data/sample/` στο δίσκο | Γρήγορος έλεγχος / ανάπτυξη στο laptop |
| Cluster | `hdfs` | HDFS του cslab | **Το πραγματικό παραδοτέο** (σωστά executor configs, ίχνος στο k8s) |

---

## Μέρος Α — Τοπική εκτέλεση (sample data)

Χρήσιμο για να επιβεβαιώσεις ότι ο κώδικας τρέχει, πριν πας στο cluster.

### 1. Προαπαιτούμενα
- **Python 3.11.x**, **Java JDK 11** (ίδια έκδοση με το cluster image), **Git**.
- Έλεγχος: `python --version` → 3.11.x, `java -version` → 11.x.
- Σημαντικό: ο φάκελος του project να **μην** είναι σε OneDrive ή σε path
  με ελληνικούς χαρακτήρες (σπάει το Spark). Π.χ. `C:\Users\<user>\bigdata_project`.

### 2. Πάρε τον κώδικα και φτιάξε περιβάλλον
```powershell
git clone <το GitHub repo url> bigdata_project
cd bigdata_project
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Κατέβασε τα sample δεδομένα
Τα δεδομένα **δεν** είναι στο repo (είναι μεγάλα — βλ. `.gitignore`). Κατέβασέ τα:
```powershell
python scripts\download_sample.py
```
Δημιουργείται ο φάκελος `data/sample/` με τα CSV/GeoJSON.

### 4. Επιβεβαίωση
```powershell
python verify_data.py
```
Αν δεις schema με στήλες `DATE OCC`, `TIME OCC`, `Premis Desc` — όλα ΟΚ.

### 5. Τρέξε ένα query τοπικά
```powershell
# DATA_MODE=local είναι το default, δεν χρειάζεται να το ορίσεις
python -m src.q1_day_parts.q1_dataframe
python -m src.q2_top_months.q2_sql
python -m src.q3_income.q3_dataframe
python -m src.q4_nearest_station.q4_dataframe
```
> Τρέχε τα ως modules (`python -m ...`) από τη ρίζα του project, ώστε να
> βρίσκεται το package `src`.

---

## Μέρος Β — Εκτέλεση στο cluster (παραδοτέο)

Εδώ παράγονται οι χρόνοι της αναφοράς και το ίχνος εκτέλεσης στο Kubernetes.
Όλα γίνονται από **WSL** με ενεργό **OpenVPN**.

### 1. Προαπαιτούμενα σύνδεσης (μία φορά)
1. **WSL Ubuntu** + **OpenVPN** προς το cslab (το `.ovpn` που σου δόθηκε).
2. **kubectl** + το `kubeconfig` στο `~/.kube/config`. Έλεγχος:
   `kubectl get pods -n dsml00318-priv`
3. **Spark 3.5.x + Hadoop 3 client** στο WSL, με `core-site.xml` /
   `spark-defaults.conf` κατά τον οδηγό `ikons/bigdata-dsml`
   (`04_remote-spark-kubernetes`).

### 2. Πρόσβαση στο HDFS από το client (ΚΡΙΣΙΜΟ)
Το hostname `hdfs-namenode` είναι **εσωτερικό του Kubernetes**: τα driver pods
το αναλύουν, αλλά το WSL client σου όχι — και το `hdfs dfs` σκάει με
`UnknownHostException: hdfs-namenode`. Το VPN δρομολογεί το δίκτυο `10.233.x`
του cluster, οπότε αρκεί μια εγγραφή στο `/etc/hosts`:

```bash
sudo bash -c 'echo "10.233.49.220  hdfs-namenode" >> /etc/hosts'
```
> Το `10.233.49.220` ήταν το ClusterIP του `hdfs-namenode` στη δική μας
> εκτέλεση. Αν στο δικό σου cluster είναι άλλο, πάρ' το από τον οδηγό 04 του
> εργαστηρίου (ή από το service `hdfs-namenode`).

Έλεγχος ότι δουλεύει (πρέπει να λιστάρει τα datasets):
```bash
export HADOOP_USER_NAME=dsml00318
hdfs dfs -ls hdfs://hdfs-namenode:9000/data/
```

### 3. Ανέβασε τον κώδικα στο HDFS
Τα **δεδομένα** είναι ήδη στο HDFS (`/data/`). Τον **κώδικά σου** όμως πρέπει να
τον ανεβάσεις, για να τον διαβάσουν τα driver pods. (Το flag
`dfs.client.use.datanode.hostname=false` είναι το default· το βάζουμε ρητά ως
ασφάλεια ώστε το write να μη μπλέκει με ονόματα datanode.)

```bash
cd /mnt/c/Users/<user>/bigdata_project
export HADOOP_USER_NAME=dsml00318
HC=hdfs://hdfs-namenode:9000/user/dsml00318/code

zip -qr /tmp/src.zip src/
hdfs dfs -D dfs.client.use.datanode.hostname=false -put -f /tmp/src.zip "$HC/src.zip"
for f in src/q1_day_parts/q1_dataframe.py src/q1_day_parts/q1_dataframe_udf.py \
         src/q1_day_parts/q1_rdd.py src/q2_top_months/q2_dataframe.py \
         src/q2_top_months/q2_sql.py src/q3_income/q3_dataframe.py \
         src/q3_income/q3_rdd.py src/q3_income/q3_with_hints.py \
         src/q4_nearest_station/q4_dataframe.py src/q4_nearest_station/q4_with_hints.py \
         src/conversion/csv_to_parquet.py ; do
  hdfs dfs -D dfs.client.use.datanode.hostname=false -put -f "$f" "$HC/$(basename "$f")"
done
```

### 4. Τρέξε ένα query
Ο driver τρέχει **μέσα** στο cluster και διαβάζει τον κώδικα από το HDFS. Το
`DATA_MODE=hdfs` και το `HADOOP_USER_NAME` περνάνε στον driver με `--conf`:

```bash
spark-submit \
  --conf spark.kubernetes.driverEnv.DATA_MODE=hdfs \
  --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=dsml00318 \
  --num-executors 4 --executor-cores 1 --executor-memory 2g \
  --py-files hdfs://hdfs-namenode:9000/user/dsml00318/code/src.zip \
  hdfs://hdfs-namenode:9000/user/dsml00318/code/q2_dataframe.py
```
Δες το αποτέλεσμα από το driver pod:
```bash
kubectl get pods -n dsml00318-priv | grep q2
kubectl logs -n dsml00318-priv <driver-pod> | tail -30
```

### 5. Όλα μαζί + συλλογή χρόνων
Τα scripts κάνουν μόνα τους zip+upload+submit (με τα σωστά executor configs)
και μαζεύουν τους χρόνους. Απαιτούν να έχει γίνει το βήμα 2 (`/etc/hosts`):
```bash
bash scripts/cluster_run_all.sh        # Q1-Q4
bash scripts/cluster_experiments.sh    # scalability + parquet + hints
bash scripts/collect_timers.sh results/cluster_timers.csv   # [TIMER] -> CSV
```

### Executor configs ανά ζητούμενο
| Query | Executors × cores × memory |
|-------|-----------------------------|
| Q1 | 2 × 1 × 2GB |
| Q2 | 4 × 1 × 2GB |
| Q3 | 3 × 1 × 2GB |
| Q4 (scalability) | A: 2×{1c/2g, 2c/4g, 4c/8g} · B: {2×4c/8g, 4×2c/4g, 8×1c/2g} |

---

## Δομή του project
```
src/        κώδικας queries (common/, conversion/, q1_/, q2_/, q3_/, q4_/)
scripts/    cluster_run_all.sh, cluster_experiments.sh, collect_timers.sh, submit.sh
report/     report.tex + report.pdf
results/    *.csv με τα timings (+ results/logs/ raw output, εκτός git)
data/       sample δεδομένα (εκτός git, μέσω download_sample.py)
```

---

## Συχνά προβλήματα
- **`ModuleNotFoundError: No module named 'src'`** → τρέξε από τη ρίζα του
  project ως module (`python -m src....`) ή με `--py-files src.zip` στο cluster.
- **`ModuleNotFoundError: pyspark`** → δεν είναι ενεργό το venv
  (`.\.venv\Scripts\Activate.ps1`).
- **`Could not find or load main class ...launcher.Main`** → path με ελληνικούς
  χαρακτήρες ή OneDrive· μετακίνησε το project.
- **Java mismatch** → χρειάζεται Java 11 (όχι 17/21)· restart μετά την αλλαγή
  `JAVA_HOME`.
- **Στο cluster διαβάζει τοπικά αρχεία** → ξέχασες το `DATA_MODE=hdfs` / το
  `--conf spark.kubernetes.driverEnv.DATA_MODE=hdfs`.
