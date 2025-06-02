# pyspark-incremental-airflow
This repository contains an Airflow DAG that orchestrates an incremental data pipeline using PySpark scripts. The pipeline automates daily processing data, syncs results to S3, performs housekeeping, and loops until a target date threshold is reached.

## Vehicle Data Pipeline for Predictive Maintenance and Usage Analytics

This project is a data engineering pipeline built with **PySpark** and **Apache Airflow**, designed to process and analyze vehicle data incrementally. It ingests data from CSV files, transforms it to extract valuable insights like maintenance prediction, fuel efficiency, and vehicle expiry estimates, and loads the results into **Elasticsearch** for dashboarding and analytics.

---

## 🚀 Features

- Incremental date-based data processing using `date.txt`
- Scheduled orchestration with Airflow and SSHOperator
- Aggregated metrics:
  - Average maintenance cost
  - Next expected service date
  - Fuel consumption efficiency
  - Predicted vehicle expiry
- Automatic pipeline reruns until the current date is reached
- Data loading into Elasticsearch for visualization

---

## 🗂️ Project Structure

vehicle-data-project/ 

├── data/ # Input datasets 

│ ├── vehicles.csv  

│ ├── maintenance.csv  
  
│  └── usage.csv 
  
├── scripts/ 

│ ├── vehicle_data_pipeline.py # Main PySpark pipeline script 

│  ├── date.txt # Controls which date to process 
  
│  ├── increment_date.py # Increments date.txt after processing 
  
│  └── incremental_date_airflow # Entry script for Airflow Spark job 
  
├── requirements.txt # Python dependencies 

└── README.md # Project overview


---

## 📈 Data Pipeline Overview

1. **Vehicle Data Processing** (`vehicle_data_pipeline.py`)
   - Joins `vehicles`, `maintenance`, and `usage` data
   - Applies aggregations, predictions, and calculates total costs
   - Writes output to Elasticsearch (`vehicles/_doc`)

2. **Airflow DAG** (`vehicle-usage-service-Incremental`)
   - Triggers PySpark job remotely via `SSHOperator`
   - Increments `date.txt` after successful job completion
   - Uses `ShortCircuitOperator` to continue until the present date
   - Loops execution using `TriggerDagRunOperator`

3. **Date Control** (`date.txt` + `increment_date.py`)
   - Keeps track of the last processed date
   - Automatically increments by 1 day until current date is reached

---

## ⚙️ Installation & Setup

### 1. Prerequisites

- Apache Airflow (with SSH and Python operators)
- PySpark
- Elasticsearch running (locally or remote)
- Hadoop/Spark environment with Hive support


---

### Core dependencies
pyspark==3.5.1

pandas==2.2.1

### Airflow and operators
apache-airflow==2.6.3

apache-airflow-providers-ssh==3.6.3

### MySQL (optional, as seen in imports)
mysqlclient==2.2.4

### Elasticsearch Hadoop connector is required for PySpark, but it's used via spark.jars.packages
### So you don't need to include it in this file

### Optional: for scheduling / cron tasks locally
schedule==1.2.1

---

### 2. Clone the Repository
---
git clone https://github.com/itsSwapnil/pyspark-incremental-airflow.git

cd pyspark-incremental-airflow

---

🛡️ Technologies Used

PySpark – ETL and data transformations

Apache Airflow – Pipeline orchestration

Elasticsearch – Storage and analytics

Hive – Spark SQL backend support


---
🙋 Author

LinkedIn: http://www.linkedin.com/in/SwapnilTaware

GitHub: https://github.com/itsSwapnil

---
