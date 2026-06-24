# Διαχείριση Δεδομένων Μεγάλης Κλίμακας — Εξαμηνιαία Εργασία 2025-26

Ανάλυση δεδομένων εγκληματικότητας του Los Angeles με Apache Spark πάνω σε HDFS,
στο πλαίσιο του μαθήματος «Διαχείριση Δεδομένων Μεγάλης Κλίμακας» του ΔΠΜΣ
ΕΔΕΜΜ της ΣΗΜΜΥ (ΕΜΠ).

## Δεδομένα

Τα δεδομένα διατίθενται στο HDFS του εργαστηριακού cluster, στο prefix:

```
hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/
```

| Σύνολο | Path |
|--------|------|
| LA Crime 2010-2019 | `/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv` |
| LA Crime 2020-2025 | `/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv` |
| Census Blocks 2020 | `/data/LA_Census_Blocks_2020.geojson` |
| Census Fields | `/data/LA_Census_Blocks_2020_fields.csv` |
| Median Income 2021 | `/data/LA_income_2021.csv` |
| LA Police Stations | `/data/LA_Police_Stations.csv` |

## Δομή κώδικα

```
src/
├── common/           Helpers (Spark session, paths, timing)
├── conversion/       Μετατροπή csv → parquet (Ζητούμενο 1)
├── q1_day_parts/     Query 1: ποσοστά εγκλημάτων σε δρόμο ανά τμήμα ημέρας
├── q2_top_months/    Query 2: top-3 μήνες με τα περισσότερα εγκλήματα ανά έτος
├── q3_income/        Query 3: μέσο κατά κεφαλήν εισόδημα ανά ZIP
└── q4_nearest_station/ Query 4: εγκλήματα πλησιέστερα σε κάθε αστυνομικό τμήμα

scripts/
├── submit.sh         Generic spark-submit wrapper
└── run_all_configs.sh Πειραματικός driver για Q4 (6 configurations)
```

## Εκτέλεση

Όλα τα queries τρέχουν με `spark-submit` πάνω στο k8s cluster του μαθήματος:

```bash
./scripts/submit.sh \
    --executors 2 --cores 1 --memory 2g \
    src/q1_day_parts/q1_dataframe.py
```

Το `submit.sh` περνάει τις σωστές παραμέτρους σε `spark-submit` και κρατάει
τα defaults για master/namespace.

## Αντιστοίχιση Ζητουμένων → Αρχείων

| Ζητούμενο | Αρχεία |
|-----------|--------|
| 1 (format) | `src/conversion/csv_to_parquet.py` |
| 2 (Q1 ×3) | `src/q1_day_parts/{q1_dataframe,q1_dataframe_udf,q1_rdd}.py` |
| 3 (Q2 ×2) | `src/q2_top_months/{q2_dataframe,q2_sql}.py` |
| 4 (Q3 ×2) | `src/q3_income/{q3_dataframe,q3_rdd}.py` |
| 5 (Q4 scalability) | `src/q4_nearest_station/q4_dataframe.py` + `scripts/run_all_configs.sh` |
| 6 (join strategies) | παραλλαγές των Q3/Q4 με `.hint(...)` |

## Δήλωση χρήσης LLM

Βλ. [`LLM_USAGE.md`](LLM_USAGE.md).
