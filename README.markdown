<p align="center">
  <img src="images/pipeline_diagram.png" alt="MIMIC-III Pipeline Diagram" width="600"/>
</p>

# MIMIC-III Big Data Pipeline: From Raw Data to Actionable Insights

Welcome to the MIMIC-III Big Data Pipeline project! This repository demonstrates a robust pipeline for processing and analyzing medical data from the MIMIC-III database using modern big data technologies. We extract, clean, and transform raw CSV data into Avro format, store it efficiently in HDFS, and perform analytics using Hive SQL and Hadoop MapReduce, all within a Dockerized Hadoop environment. Whether you're a data scientist, researcher, or big data enthusiast, this project provides a reproducible framework for handling large-scale healthcare data.

## Pipeline Overview

The pipeline transforms raw MIMIC-III data into queryable insights through a series of well-defined steps, as illustrated in the diagram above, created using [Draw.io](https://app.diagrams.net/). The flow includes:
- **MIMIC-III Database**: Extract raw CSVs.
- **Jupyter Notebook**: Clean data and convert to Avro (Windows desktop).
- **Docker Container (Hadoop)**: Host HDFS for storage.
- **HDFS**: Store Avro files.
- **Hive & MapReduce**: Query and process data for analytics.

## Project Workflow

### 1. Extract Data from MIMIC-III
- **What Happens**: Download the MIMIC-III dataset from PhysioNet (requires credentials) and extract four CSV files (`PATIENTS.csv`, `ADMISSIONS.csv`, `ICUSTAYS.csv`, `DIAGNOSES_ICD.csv`) to `C:\Users\345d0\Downloads\`.
- **Why**: These files contain patient demographics, hospital admissions, ICU stays, and diagnosis codes, forming the foundation for analysis.

### 2. Clean and Convert to Avro in Jupyter Notebook
- **What Happens**: Using Jupyter Notebook on a Windows desktop, we:
  - Load CSVs with pandas.
  - Clean data by removing duplicates, converting dates to `datetime64`, casting integers to `int32`, and replacing `NaN` with `None` for strings.
  - Convert cleaned data to Avro format, saving files to `E:\avro_data\`.
- **Why Avro?**:
  - **Avro** is a compact, schema-based serialization format ideal for big data.
  - It includes a schema in each file, ensuring data integrity and compatibility with Hadoop tools like Hive and MapReduce.
  - Avro supports nullable fields and efficient storage, perfect for MIMIC-III’s mix of integers, strings, and timestamps.
- **Output**: Avro files (`PATIENTS_clean.avro`, `ADMISSIONS_clean.avro`, `ICUSTAYS_clean.avro`, `DIAGNOSES_ICD_clean.avro`).

### 3. Store Data in HDFS
- **What Happens**:
  - Copy Avro files from Windows (`E:\avro_data\`) to the `namenode` container in a Dockerized Hadoop environment using WSL.
  - Upload files to HDFS directories (`/mimic/patients`, `/mimic/admissions`, `/mimic/icustays`, `/mimic/diagnoses_icd`).
- **Why HDFS**:
  - HDFS (Hadoop Distributed File System) provides scalable, fault-tolerant storage for large datasets.
  - It integrates seamlessly with Hive and MapReduce, enabling efficient querying and processing.
- **Process**:
  - Avro files are copied to the `namenode` container’s `/tmp` directory.
  - HDFS commands (`hdfs dfs -put`) store files in designated paths.
  - Result: Data is distributed across HDFS, ready for Hive and MapReduce access.

### 4. Create Hive Tables
- **What Happens**: Define external Hive tables in the `mimic` database, mapping to HDFS paths. Each table (`patients`, `admissions`, `icustays`, `diagnoses_icd`) uses Avro’s schema for structure.
- **Why**: Hive provides a SQL-like interface to query HDFS data, simplifying analytics without manual data extraction.

### 5. Perform Analytics with Hive and MapReduce
- **Hive Analytics**:
  - Use HiveQL to query tables and extract insights. Examples:
    1. **Average Length of Stay (LOS) in ICU by Care Unit**:
       - **Query**: Calculates the average LOS (in days) for each ICU care unit.
         ```sql
         SELECT first_careunit, AVG(los) as avg_los
         FROM icustays
         GROUP BY first_careunit;
         ```
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
       - **Query**: Shows the number of patients with different counts of ICU admissions.
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
       - **Query**: Computes the average hospital LOS for each diagnosis code (min. 5 cases).
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
- **MapReduce Analytics**:
  - Use Hadoop MapReduce for distributed processing. Example:
    - **Average Patient Age**:
      - Processes `PATIENTS_clean.avro` to calculate ages from `dob`.
      - Uses Python scripts (`mapper.py`, `reducer.py`) via Hadoop Streaming.
      - Outputs the average age to HDFS.
- **Why**: HiveQL is ideal for SQL-based analytics, while MapReduce enables scalable, distributed processing for custom computations like averaging ages across large datasets.

## Getting Started

1. **Clone the Repository**:
   ```bash
   git clone <your-repo-url>
   cd <your-repo-name>
   ```

2. **Follow the User Manual**:
   - Detailed instructions are in [user-manual.md](user-manual.md), covering:
     - Environment setup (Docker, Jupyter).
     - Data cleaning and Avro conversion.
     - HDFS upload and Hive table creation.
     - Hive and MapReduce analytics.

## Technologies Used
- **MIMIC-III**: Clinical database.
- **Jupyter Notebook**: Data cleaning and Avro conversion (Windows).
- **Avro**: Schema-based data serialization.
- **Docker**: Hosts Hadoop and Hive (`docker-hadoop-spark` stack).
- **HDFS**: Distributed storage.
- **Hive**: SQL querying.
- **MapReduce**: Distributed processing.
- **WSL**: Bridges Windows and Hadoop environments.

## Contributing
Fork the repository, submit issues, or contribute enhancements. Suggestions for additional analytics (e.g., mortality rates, readmission patterns) are welcome!

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.