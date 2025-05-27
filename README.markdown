<p align="center">
  <img src="images/pipeline.png" alt="MIMIC-III Pipeline Diagram" width="600"/>
</p>

# MIMIC-III Big Data Pipeline: From Raw Data to Actionable Insights

Welcome to the MIMIC-III Big Data Pipeline project! This repository demonstrates a robust pipeline for processing and analyzing medical data from the MIMIC-III database using modern big data technologies. We extract, clean, and transform raw CSV data into Avro format, store it efficiently in HDFS, and perform SQL-based analytics using Hive, all within a Dockerized Hadoop environment. Whether you're a data scientist, researcher, or big data enthusiast, this project provides a reproducible framework for handling large-scale healthcare data.

## Pipeline Overview

The pipeline transforms raw MIMIC-III data into queryable insights through a series of well-defined steps, as shown in the diagram above. The flow includes:
- **MIMIC-III Database**: Extract raw CSVs.
- **Jupyter Notebook**: Clean data and convert to Avro (Windows desktop).
- **Docker (Hadoop)**: Host HDFS for storage.
- **HDFS**: Store Avro files.
- **Hive**: Query data for analytics.

## Project Workflow

### 1. Extract Data from MIMIC-III
- **What Happens**: Download the MIMIC-III dataset from PhysioNet (requires credentials) and extract four CSV files (`PATIENTS.csv`, `ADMISSIONS.csv`, `ICUSTAYS.csv`, `DIAGNOSES_ICD.csv`) to `C:\Users\345d0\Downloads\`.
- **Why**: These files contain patient demographics, hospital admissions, ICU stays, and diagnosis codes, forming the foundation for analysis.

### 2. Clean and Convert to Avro in Jupyter Notebook
- **What Happens**: Using Jupyter Notebook on a Windows desktop, we:
  - Load CSVs with pandas.
  - Clean data by removing duplicates, converting dates to `datetime64`, casting integers to `int32`, and replacing `NaN` with `None` for strings.
  - Convert cleaned data to Avro format, saving files to `E:\avro_data`.
- **Why Avro?**:
  - **Avro** is a compact, schema-based serialization format ideal for big data.
  - It includes a schema in each file, ensuring data integrity and compatibility with Hadoop tools like Hive.
  - Avro supports nullable fields and efficient storage, perfect for MIMIC-III’s mix of integers, strings, and timestamps.
- **Output**: Avro files (`PATIENTS_clean.avro`, `ADMISSIONS_clean.avro`, `ICUSTAYS_clean.avro`, `DIAGNOSES_ICD_clean.avro`).

### 3. Store Data in HDFS
- **What Happens**:
  - Copy Avro files from Windows (`E:\avro_data`) to the `namenode` container in a Dockerized Hadoop environment using WSL.
  - Upload files to HDFS directories (`/mimic/patients`, `/mimic/admissions`, `/mimic/icustays`, `/mimic/diagnoses_icd`).
- **Why HDFS**:
  - HDFS (Hadoop Distributed File System) provides scalable, fault-tolerant storage for large datasets.
  - It integrates seamlessly with Hive, enabling efficient querying.
- **Process**:
  - Avro files are copied to the `namenode/` container’s `/tmp` directory.
  - HDFS commands (`hdfs dfs -put`) store files in designated paths.
  - Result: Data is distributed across HDFS, ready for Hive access.

### 4. Create Hive Tables
- **What Happens**: Define external Hive tables in the `mimic` database, mapping to HDFS paths. Each table (`patients`, `admissions`, `icustays`, `diagnoses_icd`) uses Avro schema for structure.
- **Why**: Hive provides a SQL-like interface to query HDFS data, simplifying analytics without manual data extraction.

### 5. Perform Analytics with Hive
- **What Happens**: Use HiveQL to query tables and extract insights. Example analysis:
  - **Average Length of Stay (LOS) per Diagnosis**:
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
- **Why**: HiveQL enables complex joins and aggregations, revealing patterns like diagnosis-specific hospital stays.

## Getting Started

1. **Clone the Repository**:
   ```bash
   git clone (https://github.com/mustafarezk12/Big-Data-Pipeline-for-Real-Time-Healthcare-Analytics-with-MIMIC-III.git)
   cd     Big-Data-Pipeline-for-Real-Time-Healthcare-Analytics-with-MIMIC-III


   ```

2. **Follow the User Manual**:
   - Detailed instructions are in `[user-manual.md](https://user-manual/diagnoses_icd)` covering:
     - Environment setup (Docker, Jupyter).
     - Data cleaning and Avro conversion.
     - HDFS upload and Hive table creation.
     - Analytics setup.

## Technologies Used
- **MIMIC-III**: Clinical database.
- **Jupyter Notebook**: Data cleaning and Avro conversion (Windows).
- **Avro**: Schema-based data serialization.
- **Docker**: Hosts Hadoop and Hive (`docker-hadoop-spark` stack).
- **HDFS**: Distributed storage.
- **Hive**: SQL querying.
- **WSL2**: Bridges Windows and Dockerized environments.

## Contributing
Fork the repository, submit issues, or contribute enhancements. Suggestions for additional analytics (e.g., mortality rates, ICU readmissions) are welcome!

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
