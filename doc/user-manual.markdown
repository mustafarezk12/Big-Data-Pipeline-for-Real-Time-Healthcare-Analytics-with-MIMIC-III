# User Manual: Processing MIMIC-III Datasets with Hadoop, Hive, and Avro

This user manual provides a step-by-step guide to preprocess, convert, store, and analyze four MIMIC-III datasets (`PATIENTS`, `ADMISSIONS`, `ICUSTAYS`, `DIAGNOSES_ICD`) using a Hadoop-based big data environment. The process involves extracting data from MIMIC-III, cleaning and converting CSV files to Avro format using Jupyter Notebook on a Windows desktop, uploading Avro files to HDFS via WSL, creating external Hive tables, and performing SQL-based analytics with HiveQL. The setup uses WSL (Ubuntu on Windows) with the `docker-hadoop-spark` stack for Hadoop and Hive, and a Windows desktop for initial data processing.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup Environment](#setup-environment)
3. [Extract and Clean MIMIC-III Data](#extract-and-clean-mimic-iii-data)
4. [Convert Datasets to Avro](#convert-datasets-to-avro)
5. [Upload Avro Files to HDFS](#upload-avro-files-to-hdfs)
6. [Create Hive Tables](#create-hive-tables)
7. [Verify Hive Tables](#verify-hive-tables)
8. [Perform Analytics](#perform-analytics)
9. [Troubleshooting](#troubleshooting)
10. [Conclusion](#conclusion)

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
    mkdir "E:\iti\Big Data\parquet_data"
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
with open(r"E:\iti\Big Data\parquet_data\PATIENTS_clean.avro", "wb") as f:
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
with open(r"E:\iti\Big Data\parquet_data\ADMISSIONS_clean.avro", "wb") as f:
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
with open(r"E:\iti\Big Data\parquet_data\ICUSTAYS_clean.avro", "wb") as f:
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
with open(r"E:\iti\Big Data\parquet_data\DIAGNOSES_ICD_clean.avro", "wb") as f:
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
  dir "E:\iti\Big Data\parquet_data\"
  ```

## Upload Avro Files to HDFS

Upload each Avro file to HDFS using WSL.

1. **Copy to NameNode**:
   - In WSL, copy files from the Windows filesystem (accessible via `/mnt/`):
     ```bash
     docker cp /mnt/e/iti/Big\ Data/parquet_data/PATIENTS_clean.avro namenode:/tmp/PATIENTS_clean.avro
     docker cp /mnt/e/iti/Big\ Data/parquet_data/ADMISSIONS_clean.avro namenode:/tmp/ADMISSIONS_clean.avro
     docker cp /mnt/e/iti/Big\ Data/parquet_data/ICUSTAYS_clean.avro namenode:/tmp/ICUSTAYS_clean.avro
     docker cp /mnt/e/iti/Big\ Data/parquet_data/DIAGNOSES_ICD_clean.avro namenode:/tmp/DIAGNOSES_ICD_clean.avro
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

## Perform Analytics

Analyze the data using HiveQL. Below is an example query to calculate the average hospital length of stay (LOS) per diagnosis.

1. **Average LOS per Diagnosis**:
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

2. **Run Query**:
   - In Beeline:
     ```sql
     USE mimic;
     -- Paste the query above
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
    docker cp /mnt/e/iti/Big\ Data/parquet_data/file.avro namenode:/tmp/file.avro
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

## Conclusion

This manual outlines the process to process and analyze MIMIC-III datasets using Hadoop, Hive, and Avro, with data cleaning and conversion performed on a Windows desktop. By following these steps, you can convert CSVs to Avro, store them in HDFS, create Hive tables, and perform analytics. For additional datasets or queries, adapt the provided scripts and SQL statements.