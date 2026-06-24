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

### 1. Προαπαιτούμενα σύνδεσης (μία φορά)
1. **WSL Ubuntu** (preparatory οδηγός του μαθήματος).
2. **OpenVPN** προς το cslab με το `.ovpn` που σου δόθηκε.
3. **kubectl** + το `kubeconfig` στο `~/.kube/config`.
4. **Spark 3.5.x + Hadoop 3 client** στο WSL, με `core-site.xml` και
   `spark-defaults.conf` ρυθμισμένα κατά τους οδηγούς `ikons/bigdata-dsml`
   (`04_remote-spark-kubernetes`).
5. Πρόσβαση HDFS μέσω `HADOOP_USER_NAME` (το username σου, π.χ. `dsml00318`).

Τα δεδομένα είναι ήδη στο HDFS:
`hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/` — δεν ανεβάζεις
τίποτα.

### 2. Τρέξε όλα τα queries με τα σωστά executor configs
Από WSL, μέσα στον φάκελο του project:
```bash
# Q1 (2 exec ×1c×2g), Q2 (4×1×2g), Q3 (3×1×2g), Q4 (default) — με timings σε CSV
bash scripts/cluster_run_all.sh

# Scalability Q4 (6 configs) + CSV→Parquet + join hints (Ζητούμενο 6)
bash scripts/cluster_experiments.sh

# Συγκέντρωση των [TIMER] χρόνων από τα pods / logs
bash scripts/collect_timers.sh results/cluster_timers.csv
```
Τα scripts περνούν `--conf spark.kubernetes.driverEnv.DATA_MODE=hdfs` ώστε ο
driver να διαβάζει από το HDFS, σώζουν τα timings στο `results/*.csv` και το
raw output κάθε run στο `results/logs/` (ως ανιχνεύσιμο ίχνος).

### 3. Μεμονωμένο query (αν θες χειροκίνητα)
```bash
export DATA_MODE=hdfs
export HADOOP_USER_NAME=<το username σου>
zip -qr src.zip src/
spark-submit \
  --conf spark.kubernetes.driverEnv.DATA_MODE=hdfs \
  --num-executors 2 --executor-cores 1 --executor-memory 2g \
  --py-files src.zip \
  src/q1_day_parts/q1_dataframe.py
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
