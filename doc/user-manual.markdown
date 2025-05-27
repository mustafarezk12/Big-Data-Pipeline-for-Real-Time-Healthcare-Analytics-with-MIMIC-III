# User Manual: Processing MIMIC-III Datasets with Hadoop, Hive, and Avro

This user manual provides a step-by-step guide to preprocess, convert, store, and analyze four MIMIC-III datasets (`PATIENTS`, `ADMISSIONS`, `ICUSTAYS`, `DIAGNOSES_ICD`) using a Hadoop-based big data environment. The process involves extracting data from MIMIC-III, cleaning and converting CSV files to Avro format using Jupyter Notebook on a Windows desktop, uploading Avro files to HDFS via WSL, creating external Hive tables, performing SQL-based analytics with HiveQL, and running distributed processing with Hadoop MapReduce. The setup uses WSL (Ubuntu on Windows) with the `docker-hadoop-spark` stack for Hadoop and Hive, and a Windows desktop for initial data processing.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup Environment](#setup-environment)
3. [Extract and Clean MIMIC-III Data](#extract-and-clean-mimic-iii-data)
4. [Convert Datasets to Avro](#convert-datasets-to-avro)
5. [Upload Avro Files to HDFS](#upload-avro-files-to-hdfs)
6. [Create Hive Tables](#create-hive-tables)
7. [Verify Hive Tables](#verify-hive-tables)
8. [Perform Analytics with Hive](#perform-analytics-with-hive)
9. [Perform Distributed Processing with MapReduce](#perform-distributed-processing-with-mapreduce)
10. [Troubleshooting](#troubleshooting)
11. [Conclusion](#conclusion)

## Prerequisites

Before starting, ensure you have the following:

- **Hardware**:
  - Windows PC with WSL2 (Ubuntu distribution recommended).
  - At least 16 GB RAM and 50 GB free disk space for Docker and datasets.

- **Software**:
  - WSL2 installed (`wsl --install` in Windows PowerShell).
  - Docker Desktop for Windows with WSL2 backend.
  - Python 3.8+ with Jupyter Notebook, pandas, and avro-python3 installed on Windows:
    ```bash
    pip install jupyter pandas avro-python3
    ```
  - `docker-hadoop-spark` repository cloned in WSL:
    ```bash
    git clone https://github.com/big-data-europe/docker-hadoop-spark.git
    cd docker-hadoop-spark
    ```

- **Datasets**:
  - MIMIC-III CSV files (`PATIENTS.csv`, `ADMISSIONS.csv`, `ICUSTAYS.csv`, `DIAGNOSES_ICD.csv`) stored at `C:\Users\345d0\Downloads\` on Windows.
  - Access requires MIMIC-III credentials (PhysioNet).

- **Directory Setup**:
  - Create a directory on Windows for Avro files:
    ```cmd
    mkdir "E:\avro_data"
    ```
  - Create a directory in WSL for MapReduce scripts:
    ```bash
    mkdir -p ~/mimic-project/mapreduce
    ```

## Setup Environment

1. **Start Docker Hadoop-Spark Stack in WSL**:
   - Open a WSL terminal and navigate to the `docker-hadoop-spark` directory:
     ```bash
     cd ~/docker-hadoop-spark
     ```
   - Start Docker containers:
     ```bash
     docker-compose up -d
     ```
   - Verify containers (`namenode`, `hive-server`, etc.) are running:
     ```bash
     docker ps
     ```

2. **Launch Jupyter Notebook on Windows**:
   - Open a Windows Command Prompt or PowerShell:
     ```cmd
     jupyter notebook
     ```
   - Access it at `http://localhost:8888` in a browser.
   - Create a new notebook for preprocessing.

3. **Install Dependencies in NameNode**:
   - Ensure `avro-python3` is installed for MapReduce:
     ```bash
     docker exec -it namenode bash
     pip install avro-python3
     exit
     ```

## Extract and Clean MIMIC-III Data

1. **Extract Data**:
   - Download the MIMIC-III dataset from PhysioNet after obtaining access.
   - Extract the CSV files (`PATIENTS.csv`, `ADMISSIONS.csv`, `ICUSTAYS.csv`, `DIAGNOSES_ICD.csv`) to `C:\Users\345d0\Downloads\`.

2. **Clean Data**:
   - In Jupyter Notebook on Windows, clean each dataset:
     - Remove duplicates using `df.drop_duplicates()`.
     - Convert date columns to `datetime64` with `pd.to_datetime()`.
     - Cast integer columns to `int32` for efficiency.
     - Replace `NaN` with `None` for nullable string columns to comply with Avro schemas.
   - Cleaning is integrated into the conversion scripts below.

## Convert Datasets to Avro

Convert each CSV to Avro format using Python in Jupyter Notebook on Windows. Below are scripts for each dataset.

### PATIENTS
```python
import pandas as pd
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json

# Avro schema
avro_schema = {
    "type": "record",
    "name": "Patient",
    "fields": [
        {"name": "row_id", "type": ["int", "null"]},
        {"name": "subject_id", "type": ["int", "null"]},
        {"name": "gender", "type": ["string", "null"]},
        {"name": "dob", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "dod", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "dod_hosp", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "dod_ssn", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "expire_flag", "type": ["int", "null"]}
    ]
}

# Load and clean
df = pd.read_csv(r"C:\Users\345d0\Downloads\PATIENTS.csv")
df = df.drop_duplicates()
date_columns = ["dob", "dod", "dod_hosp", "dod_ssn"]
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')
for col in ['row_id', 'subject_id', 'expire_flag']:
    df[col] = df[col].astype('int32')
df['gender'] = df['gender'].where(pd.notna(df['gender']), None)

# Write to Avro
schema = avro.schema.parse(json.dumps(avro_schema))
with open(r"E:\avro_data\PATIENTS_clean.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    for _, row in df.iterrows():
        writer.append({
            "row_id": row["row_id"],
            "subject_id": row["subject_id"],
            "gender": row["gender"],
            "dob": None if pd.isna(row["dob"]) else row["dob"].value // 10**6,
            "dod": None if pd.isna(row["dod"]) else row["dod"].value // 10**6,
            "dod_hosp": None if pd.isna(row["dod_hosp"]) else row["dod_hosp"].value // 10**6,
            "dod_ssn": None if pd.isna(row["dod_ssn"]) else row["dod_ssn"].value // 10**6,
            "expire_flag": row["expire_flag"]
        })
    writer.close()

# Verify
print("Data Types:", df.dtypes)
print("\nSample Data:", df.head(5))
```

### ADMISSIONS
```python
# Avro schema
avro_schema = {
    "type": "record",
    "name": "Admission",
    "fields": [
        {"name": "row_id", "type": ["int", "null"]},
        {"name": "subject_id", "type": ["int", "null"]},
        {"name": "hadm_id", "type": ["int", "null"]},
        {"name": "admittime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "dischtime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "deathtime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "admission_type", "type": ["string", "null"]},
        {"name": "admission_location", "type": ["string", "null"]},
        {"name": "discharge_location", "type": ["string", "null"]},
        {"name": "insurance", "type": ["string", "null"]},
        {"name": "language", "type": ["string", "null"]},
        {"name": "religion", "type": ["string", "null"]},
        {"name": "marital_status", "type": ["string", "null"]},
        {"name": "ethnicity", "type": ["string", "null"]},
        {"name": "edregtime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "edouttime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "diagnosis", "type": ["string", "null"]},
        {"name": "hospital_expire_flag", "type": ["int", "null"]},
        {"name": "has_chartevents_data", "type": ["int", "null"]}
    ]
}

# Load and clean
df = pd.read_csv(r"C:\Users\345d0\Downloads\ADMISSIONS.csv")
df = df.drop_duplicates()
date_columns = ["admittime", "dischtime", "deathtime", "edregtime", "edouttime"]
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')
for col in ['row_id', 'subject_id', 'hadm_id', 'hospital_expire_flag', 'has_chartevents_data']:
    df[col] = df[col].astype('int32')
string_columns = ["admission_type", "admission_location", "discharge_location", "insurance",
                  "language", "religion", "marital_status", "ethnicity", "diagnosis"]
for col in string_columns:
    df[col] = df[col].where(pd.notna(df[col]), None)

# Write to Avro
schema = avro.schema.parse(json.dumps(avro_schema))
with open(r"E:\avro_data\ADMISSIONS_clean.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    for _, row in df.iterrows():
        writer.append({
            "row_id": row["row_id"],
            "subject_id": row["subject_id"],
            "hadm_id": row["hadm_id"],
            "admittime": None if pd.isna(row["admittime"]) else row["admittime"].value // 10**6,
            "dischtime": None if pd.isna(row["dischtime"]) else row["dischtime"].value // 10**6,
            "deathtime": None if pd.isna(row["deathtime"]) else row["deathtime"].value // 10**6,
            "admission_type": row["admission_type"],
            "admission_location": row["admission_location"],
            "discharge_location": row["discharge_location"],
            "insurance": row["insurance"],
            "language": row["language"],
            "religion": row["religion"],
            "marital_status": row["marital_status"],
            "ethnicity": row["ethnicity"],
            "edregtime": None if pd.isna(row["edregtime"]) else row["edregtime"].value // 10**6,
            "edouttime": None if pd.isna(row["edouttime"]) else row["edouttime"].value // 10**6,
            "diagnosis": row["diagnosis"],
            "hospital_expire_flag": row["hospital_expire_flag"],
            "has_chartevents_data": row["has_chartevents_data"]
        })
    writer.close()

# Verify
print("Data Types:", df.dtypes)
print("\nSample Data:", df.head(5))
```

### ICUSTAYS
```python
# Avro schema
avro_schema = {
    "type": "record",
    "name": "ICUStay",
    "fields": [
        {"name": "row_id", "type": ["int", "null"]},
        {"name": "subject_id", "type": ["int", "null"]},
        {"name": "hadm_id", "type": ["int", "null"]},
        {"name": "icustay_id", "type": ["int", "null"]},
        {"name": "dbsource", "type": ["string", "null"]},
        {"name": "first_careunit", "type": ["string", "null"]},
        {"name": "last_careunit", "type": ["string", "null"]},
        {"name": "first_wardid", "type": ["int", "null"]},
        {"name": "last_wardid", "type": ["int", "null"]},
        {"name": "intime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "outtime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "los", "type": ["double", "null"]}
    ]
}

# Load and clean
df = pd.read_csv(r"C:\Users\345d0\Downloads\ICUSTAYS.csv")
df = df.drop_duplicates()
date_columns = ["intime", "outtime"]
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')
for col in ['row_id', 'subject_id', 'hadm_id', 'icustay_id', 'first_wardid', 'last_wardid']:
    df[col] = df[col].astype('int32')
string_columns = ["dbsource", "first_careunit", "last_careunit"]
for col in string_columns:
    df[col] = df[col].where(pd.notna(df[col]), None)

# Write to Avro
schema = avro.schema.parse(json.dumps(avro_schema))
with open(r"E:\avro_data\ICUSTAYS_clean.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    for _, row in df.iterrows():
        writer.append({
            "row_id": row["row_id"],
            "subject_id": row["subject_id"],
            "hadm_id": row["hadm_id"],
            "icustay_id": row["icustay_id"],
            "dbsource": row["dbsource"],
            "first_careunit": row["first_careunit"],
            "last_careunit": row["last_careunit"],
            "first_wardid": row["first_wardid"],
            "last_wardid": row["last_wardid"],
            "intime": None if pd.isna(row["intime"]) else row["intime"].value // 10**6,
            "outtime": None if pd.isna(row["outtime"]) else row["outtime"].value // 10**6,
            "los": row["los"]
        })
    writer.close()

# Verify
print("Data Types:", df.dtypes)
print("\nSample Data:", df.head(5))
```

### DIAGNOSES_ICD
```python
# Avro schema
avro_schema = {
    "type": "record",
    "name": "DiagnosisICD",
    "fields": [
        {"name": "row_id", "type": ["int", "null"]},
        {"name": "subject_id", "type": ["int", "null"]},
        {"name": "hadm_id", "type": ["int", "null"]},
        {"name": "seq_num", "type": ["int", "null"]},
        {"name": "icd9_code", "type": ["string", "null"]}
    ]
}

# Load and clean
df = pd.read_csv(r"C:\Users\345d0\Downloads\DIAGNOSES_ICD.csv")
df = df.drop_duplicates()
for col in ['row_id', 'subject_id', 'hadm_id', 'seq_num']:
    df[col] = df[col].astype('int32')
df['icd9_code'] = df['icd9_code'].where(pd.notna(df['icd9_code']), None)

# Write to Avro
schema = avro.schema.parse(json.dumps(avro_schema))
with open(r"E:\avro_data\DIAGNOSES_ICD_clean.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    for _, row in df.iterrows():
        writer.append({
            "row_id": row["row_id"],
            "subject_id": row["subject_id"],
            "hadm_id": row["hadm_id"],
            "seq_num": row["seq_num"],
            "icd9_code": row["icd9_code"]
        })
    writer.close()

# Verify
print("Data Types:", df.dtypes)
print("\nSample Data:", df.head(5))
```

**Verify Avro Files**:
- Check the Avro files on Windows:
  ```cmd
  dir "E:\avro_data\"
  ```

## Upload Avro Files to HDFS

Upload each Avro file to HDFS using WSL.

1. **Copy to NameNode**:
   - In WSL, copy files from the Windows filesystem (accessible via `/mnt/`):
     ```bash
     docker cp /mnt/e/avro_data/PATIENTS_clean.avro namenode:/tmp/PATIENTS_clean.avro
     docker cp /mnt/e/avro_data/ADMISSIONS_clean.avro namenode:/tmp/ADMISSIONS_clean.avro
     docker cp /mnt/e/avro_data/ICUSTAYS_clean.avro namenode:/tmp/ICUSTAYS_clean.avro
     docker cp /mnt/e/avro_data/DIAGNOSES_ICD_clean.avro namenode:/tmp/DIAGNOSES_ICD_clean.avro
     ```
   - Verify:
     ```bash
     docker exec namenode ls /tmp/*.avro
     ```

2. **Upload to HDFS**:
   - Access the `namenode`:
     ```bash
     docker exec -it namenode bash
     ```
   - Create directories:
     ```bash
     hdfs dfs -mkdir -p /mimic/patients
     hdfs dfs -mkdir -p /mimic/admissions
     hdfs dfs -mkdir -p /mimic/icustays
     hdfs dfs -mkdir -p /mimic/diagnoses_icd
     ```
   - Upload files:
     ```bash
     hdfs dfs -put /tmp/PATIENTS_clean.avro /mimic/patients/
     hdfs dfs -put /tmp/ADMISSIONS_clean.avro /mimic/admissions/
     hdfs dfs -put /tmp/ICUSTAYS_clean.avro /mimic/icustays/
     hdfs dfs -put /tmp/DIAGNOSES_ICD_clean.avro /mimic/diagnoses_icd/
     ```
   - Verify:
     ```bash
     hdfs dfs -ls /mimic/patients
     hdfs dfs -ls /mimic/admissions
     hdfs dfs -ls /mimic/icustays
     hdfs dfs -ls /mimic/diagnoses_icd
     ```
   - Exit:
     ```bash
     exit
     ```

## Create Hive Tables

Create external Hive tables for each dataset.

1. **Access Beeline**:
   ```bash
   docker exec -it hive-server bash
   beeline -u jdbc:hive2://localhost:10000
   ```

2. **Create Database**:
   ```sql
   CREATE DATABASE IF NOT EXISTS mimic;
   USE mimic;
   ```

3. **Create Tables**:
   - **PATIENTS**:
     ```sql
     CREATE EXTERNAL TABLE patients (
         row_id INT,
         subject_id INT,
         gender STRING,
         dob TIMESTAMP,
         dod TIMESTAMP,
         dod_hosp TIMESTAMP,
         dod_ssn TIMESTAMP,
         expire_flag INT
     )
     STORED AS AVRO
     LOCATION '/mimic/patients';
     ```
   - **ADMISSIONS**:
     ```sql
     CREATE EXTERNAL TABLE admissions (
         row_id INT,
         subject_id INT,
         hadm_id INT,
         admittime TIMESTAMP,
         dischtime TIMESTAMP,
         deathtime TIMESTAMP,
         admission_type STRING,
         admission_location STRING,
         discharge_location STRING,
         insurance STRING,
         language STRING,
         religion STRING,
         marital_status STRING,
         ethnicity STRING,
         edregtime TIMESTAMP,
         edouttime TIMESTAMP,
         diagnosis STRING,
         hospital_expire_flag INT,
         has_chartevents_data INT
     )
     STORED AS AVRO
     LOCATION '/mimic/admissions';
     ```
   - **ICUSTAYS**:
     ```sql
     CREATE EXTERNAL TABLE icustays (
         row_id INT,
         subject_id INT,
         hadm_id INT,
         icustay_id INT,
         dbsource STRING,
         first_careunit STRING,
         last_careunit STRING,
         first_wardid INT,
         last_wardid INT,
         intime TIMESTAMP,
         outtime TIMESTAMP,
         los DOUBLE
     )
     STORED AS AVRO
     LOCATION '/mimic/icustays';
     ```
   - **DIAGNOSES_ICD**:
     ```sql
     CREATE EXTERNAL TABLE diagnoses_icd (
         row_id INT,
         subject_id INT,
         hadm_id INT,
         seq_num INT,
         icd9_code STRING
     )
     STORED AS AVRO
     LOCATION '/mimic/diagnoses_icd';
     ```

## Verify Hive Tables

1. **Check Schemas**:
   ```sql
   DESCRIBE patients;
   DESCRIBE admissions;
   DESCRIBE icustays;
   DESCRIBE diagnoses_icd;
   ```

2. **Test Queries**:
   ```sql
   SELECT * FROM patients LIMIT 5;
   SELECT * FROM admissions LIMIT 5;
   SELECT * FROM icustays LIMIT 5;
   SELECT * FROM diagnoses_icd LIMIT 5;
   ```

## Perform Analytics with Hive

Analyze the data using HiveQL. Below are three example queries to extract meaningful insights from the MIMIC-III datasets.

1. **Average Length of Stay (LOS) in ICU by Care Unit**:
   - **Query**:
     ```sql
     SELECT first_careunit, AVG(los) as avg_los
     FROM icustays
     GROUP BY first_careunit;
     ```
   - **Description**: Calculates the average length of stay (in days) for patients in each ICU care unit.
   - **Result**:
     ```
     +-----------------+---------------------+
     | first_careunit  |       avg_los       |
     +-----------------+---------------------+
     | CCU             | 5.7539              |
     | CSRU            | 3.6313499999999994  |
     | MICU            | 3.9553454545454554  |
     | SICU            | 5.668460869565218   |
     | TSICU           | 3.589609090909091   |
     +-----------------+---------------------+
     5 rows selected (4.673 seconds)
     ```

2. **Distribution of ICU Readmissions**:
   - **Query**:
     ```sql
     SELECT
         icu_admissions,
         COUNT(*) AS num_of_patients
     FROM (
         SELECT subject_id, COUNT(icustay_id) AS icu_admissions
         FROM icustays
         GROUP BY subject_id
     ) t
     GROUP BY icu_admissions
     ORDER BY icu_admissions;
     ```
   - **Description**: Shows the number of patients with different counts of ICU admissions, indicating readmission patterns.
   - **Result**:
     ```
     +-----------------+------------------+
     | icu_admissions  | num_of_patients  |
     +-----------------+------------------+
     | 1               | 81               |
     | 2               | 15               |
     | 3               | 2                |
     | 4               | 1                |
     | 15              | 1                |
     +-----------------+------------------+
     5 rows selected (5.787 seconds)
     ```

3. **Average Length of Stay per Diagnosis**:
   - **Query**:
     ```sql
     SELECT 
         d.icd9_code,
         COUNT(*) as total_cases,
         ROUND(AVG(
             (unix_timestamp(a.dischtime) - unix_timestamp(a.admittime)) / 86400
         ), 2) as avg_los_days
     FROM diagnoses_icd d
     JOIN admissions a 
         ON d.hadm_id = a.hadm_id AND d.subject_id = a.subject_id
     WHERE (unix_timestamp(a.dischtime) - unix_timestamp(a.admittime)) >= 0
     GROUP BY d.icd9_code
     HAVING total_cases >= 5
     ORDER BY avg_los_days DESC
     LIMIT 10;
     ```
   - **Description**: Computes the average hospital length of stay (in days) for each diagnosis code, filtering for diagnoses with at least 5 cases.
   - **Result**:
     ```
     +--------------+--------------+---------------+
     | d.icd9_code  | total_cases  | avg_los_days  |
     +--------------+--------------+---------------+
     | 5845         | 6            | 38.23         |
     | 2767         | 5            | 34.09         |
     | 45829        | 5            | 33.77         |
     | 5180         | 6            | 29.55         |
     | 5712         | 5            | 21.96         |
     | 570          | 6            | 19.66         |
     | 78959        | 5            | 19.38         |
     | 70703        | 15           | 16.15         |
     | 5185         | 5            | 15.87         |
     | 51881        | 31           | 15.14         |
     +--------------+--------------+---------------+
     10 rows selected (20.889 seconds)
     ```

2. **Run Queries**:
   - In Beeline:
     ```sql
     USE mimic;
     -- Paste each query above
     ```

## Perform Distributed Processing with MapReduce

Perform distributed analytics using Hadoop’s MapReduce to calculate the average patient age from the `PATIENTS` dataset.

1. **Create MapReduce Scripts**:
   - **Mapper (`mapper.py`)**:
     ```python
     import sys
     import avro.datafile
     import avro.io
     import io
     import datetime

     # Reference date (May 27, 2025)
     REFERENCE_DATE = datetime.datetime(2025, 5, 27).timestamp() * 1000  # Convert to milliseconds

     def read_avro_from_stdin():
         try:
             # Read Avro data from stdin
             input_data = sys.stdin.buffer.read()
             input_stream = io.BytesIO(input_data)
             reader = avro.datafile.DataFileReader(input_stream, avro.io.DatumReader())
             for record in reader:
                 dob = record.get('dob')
                 if dob is not None:
                     # Calculate age in years
                     age = (REFERENCE_DATE - dob) / (1000 * 60 * 60 * 24 * 365.25)
                     print(f"1\t{age:.2f}")
             reader.close()
         except Exception as e:
             print(f"Error in mapper: {e}", file=sys.stderr)

     if __name__ == "__main__":
         read_avro_from_stdin()
     ```
   - **Reducer (`reducer.py`)**:
     ```python
     import sys

     def reduce_ages():
         total_age = 0.0
         count = 0

         try:
             for line in sys.stdin:
                 key, age = line.strip().split('\t')
                 total_age += float(age)
                 count += 1

             if count > 0:
                 average_age = total_age / count
                 print(f"Average Patient Age: {average_age:.2f} years")
             else:
                 print("No valid ages found", file=sys.stderr)
         except Exception as e:
             print(f"Error in reducer: {e}", file=sys.stderr)

     if __name__ == "__main__":
         reduce_ages()
     ```
   - Save scripts in `~/mimic-project/mapreduce/` in WSL.

2. **Copy Scripts to NameNode**:
   ```bash
   docker cp ~/mimic-project/mapreduce/mapper.py namenode:/tmp/mapper.py
   docker cp ~/mimic-project/mapreduce/reducer.py namenode:/tmp/reducer.py
   ```
   - Verify:
     ```bash
     docker exec namenode ls /tmp/*.py
     ```

3. **Run MapReduce Job**:
   - Access `namenode`:
     ```bash
     docker exec -it namenode bash
     ```
   - Run Hadoop Streaming:
     ```bash
     hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
       -file /tmp/mapper.py \
       -mapper "python3 /tmp/mapper.py" \
       -file /tmp/reducer.py \
       -reducer "python3 /tmp/reducer.py" \
       -input /mimic/patients/PATIENTS_clean.avro \
       -output /mimic/output/avg_age
     ```
   - Ensure output directory doesn’t exist:
     ```bash
     hdfs dfs -rm -r /mimic/output/avg_age
     ```

4. **View Results**:
   - Check output:
     ```bash
     hdfs dfs -cat /mimic/output/avg_age/part-00000
     ```
   - Exit:
     ```bash
     exit
     ```

## Troubleshooting

- **Python Errors in Jupyter**:
  - Ensure correct file paths (use raw strings: `r"C:\path\to\file"`).
  - Check for `NaN` in string columns:
    ```python
    print(df.isna().sum())
    ```

- **Docker cp Errors**:
  - Use correct syntax:
    ```bash
    docker cp /mnt/e/avro_data/file.avro namenode:/tmp/file.avro
    ```

- **Hive Query Fails**:
  - Verify `hive-server`:
    ```bash
    docker ps
    ```
  - Restart:
    ```bash
    cd ~/docker-hadoop-spark
    docker-compose restart hive-server
    ```

- **MapReduce Errors**:
  - Ensure `avro-python3` is installed in `namenode`.
  - Check Hadoop version:
    ```bash
    docker exec namenode hadoop version
    ```
  - Verify input file:
    ```bash
    hdfs dfs -ls /mimic/patients/PATIENTS_clean.avro
    ```

## Conclusion

This manual outlines the process to process, store, and analyze MIMIC-III datasets using Hadoop, Hive, Avro, and MapReduce, with data cleaning and conversion performed on a Windows desktop. By following these steps, you can convert CSVs to Avro, store them in HDFS, create Hive tables, perform SQL queries, and run distributed analytics. For additional datasets or analytics, adapt the provided scripts and commands.