# FlashPulse — Real-Time Flash Sale Monitoring System

## Overview
FlashPulse is a real-time flash sale monitoring system using Kafka, PySpark, and Streamlit to process high-volume e-commerce transactions and provide live business insights.


---

## About

**FlashPulse** is a real-time data engineering pipeline designed to simulate and monitor high-traffic e-commerce flash sales.

During flash sale events, thousands of users attempt to place orders simultaneously, often causing database bottlenecks, slow response times, and system failures. FlashPulse addresses this challenge using an event-driven architecture that decouples data ingestion from processing and storage.

The system follows the **Medallion Data Architecture** (Bronze → Silver → Gold) to ensure scalable, structured, and reliable data flow.

---

## Problem Statement

Traditional e-commerce systems face major issues during flash sales:

- **Database Bottlenecks** — Direct writes to relational databases cause locking and performance degradation under heavy load
- **Lack of Real-Time Insights** — Batch processing delays decision-making
- **Fraud Risk** — High traffic increases the chances of fraudulent transactions

FlashPulse is designed to mitigate these challenges using streaming and distributed processing.

---

## Architecture

```
Web App / Order Events
         ↓
    Apache Kafka                 ←  Bronze Layer  (Ingestion)
         ↓
  PySpark Structured Streaming   ←  Silver Layer  (Validation & Filtering)
         ↓
    Python Batch ETL             ←  Gold Layer    (Aggregation)
         ↓
    Streamlit Dashboard          ←  Visualization Layer
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Apache Kafka | Event streaming & buffering |
| Processing | Apache Spark / PySpark | Real-time data processing |
| Orchestration | Python | Automated ETL pipeline |
| Dashboard | Streamlit | Data visualization |
| Storage | MySQL / PostgreSQL | Data storage |
| ML (Optional) | Scikit-Learn | Fraud detection (extendable) |
| Data Format | JSON, CSV | Data exchange |

---

## Data Pipeline Layers

### Bronze Layer — Data Ingestion

- Order events are produced as JSON messages
- Events are ingested into Kafka topics
- Ensures high-throughput and fault-tolerant data buffering

### Silver Layer — Data Processing

- PySpark Structured Streaming consumes Kafka data
- Performs data validation, filtering, and rule-based fraud detection
- Suspicious transactions are stored separately in `fraud_alerts/`

### Gold Layer — Data Aggregation

- Python-based ETL runs every 60 seconds
- Performs aggregations such as revenue per minute, order count, and product-level metrics
- Outputs processed data for analytics

---

## Features

- Real-time event streaming pipeline
- Live dashboard for monitoring sales performance
- Rule-based fraud detection (extensible to ML models)
- Automated ETL pipeline running every 60 seconds
- Sales velocity tracking and trend analysis
- Event logging for system monitoring

---


## Performance & Assumptions

- Simulated high-throughput event streaming system
- Batch processing interval: 60 seconds
- Near real-time dashboard updates
- Fraud detection based on configurable thresholds
- Designed for scalability (can be extended to cloud environments)

---

## Project Structure

```
flashpulse/
├── orchestrator.py          # Controls ETL execution loop
├── batch_etl.py             # Gold layer aggregation logic
├── kafka_producer.py        # Simulates order events
├── pyspark_streaming.py     # Real-time processing layer
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── data_lake/
│   ├── silver/              # Clean processed data
│   └── fraud_alerts/        # Suspicious transactions
├── data_warehouse/
│   └── gold/                # Aggregated output
├── requirements.txt
└── README.md
```

---

## Installation & Setup

### Prerequisites

- Python 3.10
- Apache Kafka
- Apache Spark / PySpark
- Streamlit
- MySQL or PostgreSQL

### Steps

```bash
# Clone repository
git clone https://github.com/aryan-code14/Flash-sale-monitoring.git
cd Flash-sale-monitoring

# Install dependencies
pip install -r requirements.txt

## 🚀 How to Run the Project

### 1. Start Apache Kafka (KRaft Mode)

Open a terminal, navigate to your Kafka installation directory, and format the storage (run once):

```bash
.\bin\windows\kafka-storage.bat format -t <YOUR_UUID> -c .\config\server.properties --standalone
```

Start the Kafka Server:

```bash
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 2. Launch the Pipeline

Open 4 separate terminal windows in your project directory and execute the following commands in order:

**Terminal 1 — Data Ingestion:**

```bash
python producer.py
```

**Terminal 2 — Stream Processing:**

```bash
python processor.py
```

**Terminal 3 — Automated ETL Orchestrator:**

```bash
python orchestrator.py
```

**Terminal 4 — Executive Dashboard:**

```bash
streamlit run app.py
```

---

## Future Improvements

- [ ] Integrate machine learning-based fraud detection
- [ ] Deploy pipeline on cloud (AWS / GCP)
- [ ] Use Parquet / Delta Lake instead of CSV for scalability
- [ ] Add real-time WebSocket-based dashboard updates
- [ ] Implement role-based access control
- [ ] Containerization using Docker

---

## Key Learnings

- Event-driven system design
- Real-time data streaming concepts
- Distributed data processing with Spark
- Designing scalable data pipelines
- Building interactive dashboards for analytics

---

## Academic Context


> Aryan Marodia | Roll No: 2306387 | Course: Data Engineering 2025–2026 | KIIT University


