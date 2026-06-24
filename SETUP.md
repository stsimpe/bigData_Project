# Setup guide — εγκατάσταση σε καινούργιο laptop

Οδηγός βήμα-βήμα για να στήσεις τοπικό περιβάλλον ανάπτυξης του project
σε άλλον υπολογιστή. Ακολουθεί τη γραμμή του επίσημου repo
[ikons/bigdata-dsml](https://github.com/ikons/bigdata-dsml) με τις
εκδόσεις που χρησιμοποιεί το cluster του cslab.

Όλα τα βήματα είναι για Windows + VS Code. Αν θες WSL βλ. το επίσημο
repo.

---

## 0. Τι θα εγκαταστήσεις

| Λογισμικό | Έκδοση | Σκοπός |
|-----------|--------|--------|
| Python    | 3.11.x | Runtime του pyspark |
| Java JDK  | 11     | JVM για το Spark (ίδια με του cluster image) |
| VS Code   | latest | IDE |
| Git       | latest | Clone του project |
| PySpark   | 3.5.8  | Spark engine |

---

## 1. Python 3.11

Από https://www.python.org/downloads/release/python-3119/ κατέβασε το
**Windows installer (64-bit)** για 3.11.9.

Κατά την εγκατάσταση:
- Tick: **Add python.exe to PATH**
- Customize → Optional features: **py launcher** ON
- Advanced: **Install for all users** ON

Έλεγχος σε PowerShell:
```powershell
py -3.11 --version
# Python 3.11.9
```

## 2. Java JDK 11

Microsoft Build of OpenJDK 11 (.msi installer):
https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-11

→ **Windows x64 MSI**. Double-click → Next → Finish.

### JAVA_HOME

1. Right-click Start → System → Advanced system settings
2. Environment Variables
3. System variables → New:
   - Variable name: `JAVA_HOME`
   - Variable value: `C:\Program Files\Microsoft\jdk-11.0.x.x-hotspot`
   (άνοιξε `C:\Program Files\Microsoft\` για να δεις το ακριβές όνομα)
4. System variables → Path → Edit → New: `%JAVA_HOME%\bin`

Έλεγχος (νέο PowerShell):
```powershell
java -version
# openjdk version "11.0.x"
```

## 3. VS Code

https://code.visualstudio.com/Download → Windows x64. Install.

Άνοιξε VS Code → `Ctrl+Shift+X` → εγκατάστησε:
- **Python** (Microsoft)
- **Pylance** (έρχεται μαζί)

## 4. Git

https://git-scm.com/downloads → Windows. Default options.

## 5. Clone του project

Σε PowerShell:
```powershell
cd C:\Users\<your-user>
git clone <το δικό σου GitHub repo url> bigdata_project
cd bigdata_project
```

**Σημαντικό**: μην το βάλεις σε OneDrive ή σε path με ελληνικούς
χαρακτήρες — το Spark σπάει.

## 6. venv + dependencies

Άνοιξε τον φάκελο στο VS Code (`File → Open Folder`). Άνοιξε terminal
(`` Ctrl+` ``):

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

Αν σκάει σε execution policy:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Έλεγχος:
```powershell
python --version              # 3.11.9
python -c "import pyspark; print(pyspark.__version__)"   # 3.5.8 (ή 3.5.5)
```

## 7. Διάλεξε interpreter στο VS Code

`Ctrl+Shift+P` → **Python: Select Interpreter** → διάλεξε
`.venv\Scripts\python.exe`. Κάτω δεξιά θα γράφει
`3.11.9 ('.venv': venv)`.

## 8. Κατέβασε τα sample δεδομένα

```powershell
python scripts\download_sample.py
```

Αυτό φορτώνει:
- LA Crime Data 2010-2019 & 2020-2025 (100K γραμμές το καθένα)
- LA Census Blocks 2020 (full ~171 MB)
- LA Police Stations
- LA Income 2021

Διάρκεια: 2-5 λεπτά ανάλογα τη σύνδεση.

## 9. Δοκίμασε ότι όλα δουλεύουν

Άνοιξε `verify_data.py` και πάτα **F5**. Αν δεις 100,000 rows και schema
με στήλες `DR_NO`, `DATE OCC`, `TIME OCC`, `Premis Desc` — όλα ΟΚ.

## 10. Τρέξε τα queries

| Αρχείο | Τι κάνει |
|--------|----------|
| `src/q1_day_parts/q1_dataframe.py` | Q1 DataFrame |
| `src/q1_day_parts/q1_dataframe_udf.py` | Q1 με UDF |
| `src/q1_day_parts/q1_rdd.py` | Q1 RDD |
| `src/q2_top_months/q2_dataframe.py` | Q2 DataFrame |
| `src/q2_top_months/q2_sql.py` | Q2 SQL |
| `src/q3_income/q3_dataframe.py` | Q3 DataFrame |
| `src/q3_income/q3_rdd.py` | Q3 RDD |
| `src/q3_income/q3_with_hints.py` | Q3 με join hints |
| `src/q4_nearest_station/q4_dataframe.py` | Q4 DataFrame |
| `src/q4_nearest_station/q4_with_hints.py` | Q4 με join hints |
| `src/conversion/csv_to_parquet.py` | CSV → Parquet + comparison |

Άνοιξε όποιο θες και πάτα **F5**.

---

## Συχνά προβλήματα

### `WslRegisterDistribution failed 0x80080005`
Στις Windows Home εκδόσεις. Λύση: ενεργοποίησε Virtual Machine Platform:
```powershell
Enable-WindowsOptionalFeature -Online -FeatureName VirtualMachinePlatform -All
```
Restart, και μετά `wsl --update`. **Δεν χρειάζεται όμως** για τοπική
ανάπτυξη — αρκεί για το cluster path.

### `ModuleNotFoundError: No module named 'pyspark'`
Δεν είναι ενεργοποιημένο το venv. Στο terminal:
```powershell
.\.venv\Scripts\Activate.ps1
```
Ή το VS Code δεν χρησιμοποιεί το σωστό interpreter (βλ. βήμα 7).

### `Py4JJavaError: Could not find or load main class org.apache.spark.launcher.Main`
Το path του project περιέχει ελληνικούς χαρακτήρες ή είναι σε OneDrive.
Μετακίνησέ το σε `C:\Users\<user>\bigdata_project`.

### `Py4JError: An error occurred while calling z:org...collectAndServe`
Java version mismatch. Σιγουρέψου ότι έχεις Java 11 (όχι 17 ή 21) και
restart το VS Code μετά την αλλαγή `JAVA_HOME`.

### `WslRegisterDistribution failed` / κάθε ώρα WSL crash
Για το **cluster path** (όχι local development) χρειάζεται working WSL.
Δες τον επίσημο οδηγό `04_remote-spark-kubernetes`.

---

## Επόμενο βήμα: cluster (προαιρετικό για τοπική ανάπτυξη)

Αν θες να τρέξεις στο cslab cluster (απαιτείται για το τελικό
παραδοτέο):

1. WSL Ubuntu σύμφωνα με τον preparatory οδηγό
2. OpenVPN με το `.ovpn` από το email
3. kubectl + kubeconfig στο `~/.kube/config`
4. Spark 3.5.8 + Hadoop 3 client στο WSL
5. `core-site.xml` και `spark-defaults.conf`
6. `DATA_MODE=hdfs` στις env vars

Λεπτομέρειες στους οδηγούς `04_remote-spark-kubernetes` του ikons repo.
