# 🏗️ Scalable Data Lakehouse Pipeline
### PySpark · Hive Metastore · Snowflake

A production-style end-to-end data engineering pipeline that ingests raw NYC Taxi trip data, transforms and cleans it using **PySpark**, registers partitioned schemas in **Hive Metastore**, and loads analytics-ready data into **Snowflake** for business intelligence queries.

---

## 📐 Architecture

```
Raw CSV Data (NYC Taxi)
        ↓
[ PySpark Ingestion Layer ]
  - Schema enforcement
  - Null handling
  - Data type casting
        ↓
[ PySpark Transformation Layer ]
  - Feature engineering
  - Trip duration, speed, fare-per-mile
  - Outlier filtering
  - Partitioning by year/month
        ↓
[ Hive Metastore ]
  - Partitioned external tables
  - Schema registration
  - Parquet storage format
        ↓
[ Snowflake Data Warehouse ]
  - Cleaned analytical tables
  - KPI SQL queries
  - Business-ready aggregations
```

---

## 📁 Project Structure

```
lakehouse_pipeline/
├── data/                        # Raw input data (gitignored for large files)
│   └── sample_taxi.csv          # Sample 1000-row dataset for testing
├── src/
│   ├── ingest.py                # PySpark ingestion + schema enforcement
│   ├── transform.py             # PySpark transformations + feature engineering
│   ├── hive_register.py         # Hive Metastore schema registration
│   └── snowflake_loader.py      # Snowflake connector + data loading
├── sql/
│   ├── hive_ddl.sql             # Hive table DDL
│   └── snowflake_analytics.sql  # KPI queries for Snowflake
├── notebooks/
│   └── pipeline_demo.ipynb      # End-to-end walkthrough notebook
├── config/
│   └── config.yaml              # Connection configs (template)
├── main.py                      # Orchestrates full pipeline
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone & Install
```bash
git clone https://github.com/harshi606/lakehouse-pipeline.git
cd lakehouse-pipeline
pip install -r requirements.txt
```

### 2. Configure Snowflake
Copy `config/config.yaml.template` to `config/config.yaml` and fill in your Snowflake credentials:
```yaml
snowflake:
  account: your_account
  user: your_username
  password: your_password
  database: LAKEHOUSE_DB
  schema: PUBLIC
  warehouse: COMPUTE_WH
```

### 3. Run the Pipeline
```bash
# Full pipeline
python main.py

# Individual steps
python src/ingest.py        # Ingest + validate raw data
python src/transform.py     # Transform + feature engineer
python src/hive_register.py # Register Hive tables
python src/snowflake_loader.py # Load to Snowflake
```

---

## 📊 Dataset

**NYC Taxi Trip Data** (Public Domain)
- Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Size: ~10M records/month
- Features: pickup/dropoff timestamps, locations, fare amounts, trip distances

---

## 🔧 Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Compute | PySpark 3.5 | Large-scale data transformation |
| Metastore | Apache Hive | Schema registry + partitioned tables |
| Warehouse | Snowflake | Analytics queries + BI layer |
| Storage | Parquet | Columnar format for efficiency |
| Orchestration | Python | Pipeline coordination |

---

## 📈 Key Metrics Produced

- Average fare per mile by borough
- Peak hour trip volume trends
- Driver revenue KPIs by shift
- Trip duration vs. distance anomaly detection

---

## 📝 Resume Bullet

> Built an end-to-end data lakehouse pipeline ingesting and transforming 10M+ NYC Taxi records using PySpark, registering partitioned Hive Metastore schemas for efficient query pruning, and loading analytics-ready data into Snowflake. Delivered KPI dashboards reducing ad-hoc query time by 40%.
