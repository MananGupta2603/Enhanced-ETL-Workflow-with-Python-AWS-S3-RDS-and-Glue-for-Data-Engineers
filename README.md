
# Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue

## 🚀 Project Overview

This project demonstrates a scalable, cloud-based ETL (Extract, Transform, Load) pipeline using Python and AWS services including S3, RDS, and Glue. It handles data from multiple formats (CSV, JSON, XML), transforms and standardizes it, stores it securely, and logs all steps for traceability.

---

## 📂 Technologies Used

- **Python** – for scripting ETL logic.
- **AWS S3** – for storing raw and processed data.
- **AWS RDS** – for loading transformed data into a relational database.
- **AWS Glue (Optional)** – for schema inference and transformation.
- **Pandas** – for data manipulation.
- **SQLAlchemy** – for database operations.
- **Logging** – for monitoring pipeline activities.

---

## 📦 Dataset

Download dataset:
```bash
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
```


## 🔁 ETL Flow Overview

### 📤 Upload Raw Files to S3
- Upload **CSV**, **JSON**, and **XML** files to the `raw/` folder of your S3 bucket.

### 🔍 Extract and Transform with Python
- Read and merge files using **Pandas**, `json`, and `xml.etree`.
- Standardize column names and data types.
- Perform unit conversions:
  - **Inches → Meters**
  - **Pounds → Kilograms**
- Handle missing data, remove duplicates, and normalize categorical values.

### 💾 Load to AWS
- Save the transformed data as CSV to the `processed/` folder in S3.
- Use **SQLAlchemy** and `pandas.to_sql()` to load data into an **AWS RDS** database (PostgreSQL/MySQL).

### 📜 Logging
- Log all ETL steps including data counts, timestamps, and exceptions.
- Save logs both **locally** and in the `logs/` folder on **S3**.
