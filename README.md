# FlashPulse — Real-Time Flash Sale Monitoring System

> Task 80 · Bachelor of Technology · Information Technology · Data Engineering
> KIIT Deemed to be University · Academic Session 2025–2026


## About

--FlashPulse-- is an enterprise-grade, event-driven Big Data streaming pipeline built to handle the extreme technical pressures of e-commerce flash sales. It ingests thousands of concurrent order events per secondand also validates and secures them in real time environment and delivers live business intelligence to an executive Streamlit dashboard which is fully automated, zero manual intervention is allowed.

The system is built on the Medallion Data Architecture (Bronze → Silver → Gold), replacing direct database writes with the fault-tolerant Kafka event stream — solving the bottleneck, lock, and crash problems that exist in the traditional monolithic systems that face during high-traffic events.

When a flash sale begins, thousands of users simultaneously browse inventory, add items to cart, and attempt checkout at the exact same millisecond. Traditional monolithic systems relying on MySQL or PostgreSQL immediately create table locks, server timeouts, and crashed web pages — resulting in millions of dollars in lost revenue. FlashPulse was designed specifically to eliminate this problem at every layer of the stack used here.


## The Problem It Solves

Standard e-commerce infrastructure breaks under flash sale load for three core reasons:

**1. Database Bottleneck** — Direct transactional writes to a relational database cause table locks and severe slowdowns when thousands of concurrent users hit the system at once.

**2. No Real-Time Analytics** — Standard analytical processing runs on End-of-Day batch jobs. By the time reports are generated, the sale is already over — leaving zero opportunity to adjust pricing, inventory, or marketing strategy mid-sale.

**3. Fraud Vulnerability** — High-traffic events attract automated botnets that push through large fraudulent transactions, contaminating business metrics and skewing analytics.

FlashPulse solves all three simultaneously through its layered streaming architecture.


## Architecture

Web App / Order Events
         ↓
    Apache Kafka          ←  Bronze Layer  (Ingestion)
         ↓
  PySpark Streaming       ←  Silver Layer  (Validation & Fraud Detection)
         ↓
  Python Batch ETL        ←  Gold Layer    (Aggregation every 60s)
         ↓
  Streamlit Dashboard     ←  BI Layer      (Executive Dashboard)

### Bronze Layer — Apache Kafka
Instead of writing transactions directly to a database, the web application publishes all incoming order events as JSON payloads to a Kafka message broker. Kafka acts as a highly scalable, fault-tolerant shock absorber — appending records to an immutable ledger called a topic. Even at ten thousand orders per second, Kafka safely buffers the data with zero data loss regardless of downstream processing speed.

### Silver Layer — PySpark Structured Streaming
A continuously running PySpark Structured Streaming job consumes the Kafka topic and acts as the system's active security firewall and data validator. Every single transaction is parsed and evaluated in real time. Normal transactions are appended to the primary Data Lake. Orders exceeding the $2,500 fraud threshold are instantly intercepted and routed to an isolated `fraud_alerts/` storage path — ensuring fraudulent anomalies never contaminate downstream analytics.

### Gold Layer — Python Batch ETL
A custom Python orchestrator executes an automated Batch ETL process at 60-second intervals. It reads the validated Silver Data Lake, extracts timestamp data down to the minute, and performs complex mathematical aggregations. The resulting business-ready data is stored as lightweight CSV files in the Gold Data Warehouse, optimised for instantaneous read access by the Streamlit dashboard.


## Getting Started
## Prerequisites

- Python 3.10
- Apache Kafka
- Apache Spark / PySpark
- Streamlit
- MySQL or PostgreSQL

### Installation

bash
# Clone the repository
git clone https://github.com/aryan-code14/Flash-sale-monitoring.git
cd Flash-sale-monitoring

# Install dependencies
pip install -r requirements.txt

# Start the PySpark streaming job
python pyspark_streaming.py

# In a separate terminal, start the orchestrator
python orchestrator.py

# In another terminal, launch the dashboard
streamlit run dashboard/app.py


## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Apache Kafka | Event streaming & fault-tolerant buffering |
| Processing | Apache Spark / PySpark | Real-time structured streaming & fraud detection |
| Orchestration | Python | Automated 60-second Batch ETL loop |
| Dashboard | Streamlit | Executive business intelligence UI |
| Storage | MySQL, PostgreSQL | Relational data management |
| ML / Security | Scikit-Learn | Anomaly detection & fraud flagging |
| Big Data | Databricks | Scalable distributed data processing |
| Data Format | JSON, CSV | Event payloads & warehouse output |



## Project Structure

flashpulse/
├── orchestrator.py          # Core automation engine — 60s ETL loop
├── batch_etl.py             # Gold layer transformation job
├── kafka_producer.py        # Order event simulator / producer
├── pyspark_streaming.py     # Silver layer — validation & fraud detection
├── dashboard/
│   └── app.py               # Streamlit executive dashboard
├── data_lake/
│   ├── silver/              # Validated clean order records
│   └── fraud_alerts/        # Intercepted suspicious transactions
├── data_warehouse/
│   └── gold/                # Aggregated CSV files for dashboard
├── requirements.txt
└── README.md



## Features

- **Real-Time Fraud Detection** — PySpark flags and isolates any order above the $2,500 threshold into a separate `fraud_alerts/` path at the Silver layer. Fraudulent anomalies are completely prevented from reaching downstream analytics.

- **Sales Velocity Monitoring** — The Sales Velocity line chart tracks revenue intensity strictly on a minute-by-minute basis. A sudden drop signals a potential front-end crash to IT teams; a sudden spike confirms the success of a marketing push to the marketing team.

- **AI Insights Engine** — Automatically scans the Gold Data Warehouse and generates plain-English actionable directives. Calculates a Velocity Insight — taking current orders-per-minute and projecting total hourly volume — allowing supply chain managers to call in extra staff ahead of time.

- **Revenue Intensity Heatmap** — Dynamically adjusts shading intensity based on each product's percentage share of total revenue. Dominant products command darker, more prominent visual space — no need to read raw numbers.

- **Efficiency Scatter Plot** — Plots total revenue against total orders per product category. Data points high on the Y-axis but low on the X-axis represent high-margin sales; points far right but low on Y-axis flag products requiring massive logistical effort for little financial return.

- **Event Notification Center** — Every time the orchestrator runs its 60-second batch job, it calculates the percentage delta — whether revenue increased or decreased compared to the previous minute — and logs it directly to the dashboard, providing an undeniable audit trail of system health.

- **Zero-Touch Automation** — The Python orchestrator entirely eliminates the need for manual cron job configuration. The pipeline runs continuously, self-monitors, and self-reports.


## Core Orchestrator

python
# orchestrator.py — Core Automation Engine
import time, subprocess, glob
from datetime import datetime

RUN_INTERVAL_SECONDS = 60

while True:
    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"[{current_time}] System active. Triggering Batch ETL Job...")

    result = subprocess.run(["python", "batch_etl.py"], capture_output=True)

    if result.returncode == 0:
        fraud_files = glob.glob('./data_lake/fraud_alerts/*.json')
        if len(fraud_files) > 0:
            print(f"SECURITY ALERT: {len(fraud_files)} Suspicious Transactions Blocked!")

    time.sleep(RUN_INTERVAL_SECONDS)

## Dashboard Visualizations

| Figure | Visualization | Business Value |
|---|---|---|
| Fig 1 | Security breach banner + KPI header | Real-time threat awareness |
| Fig 2 | Revenue ring + countdown timer + velocity gauge | Live sale pulse |
| Fig 3 | Sales Velocity line chart | Minute-by-minute momentum tracking |
| Fig 4 | Revenue vs Orders bar charts | Fulfillment logistics planning |
| Fig 5 | AI Insights panel | Auto-generated actionable directives |
| Fig 6 | Revenue Intensity Heatmap | Market share at a glance |
| Fig 7 | Efficiency Scatter Plot | Margin analysis & pricing strategy |
| Fig 8 | Event Notification Center | System health audit trail |

## Why This Architecture Works

Traditional dashboards display total aggregate sums — helpful, but lacking context. In a flash sale, the most critical metric is momentum. If total sales are $100,000 but velocity drops to zero, the system has likely experienced a front-end crash. FlashPulse treats the data pipeline not as a passive reporting tool but as an active operational nervous system.

Understanding the balance between order volume and revenue generation is essential for fulfillment logistics. A product category might generate the highest overall revenue — such as Laptops — while a completely different category is responsible for the largest volume of physical boxes the warehouse team must package and ship — such as Smartwatches. This dual-axis insight is critical for allocating warehouse labour efficiently during the fulfillment phase of the flash sale.

For a data pipeline to be trusted, it must be transparent. The Event Notification Center provides continuous assurance that backend systems are actively processing data — not displaying a frozen screen.

## Future Roadmap

- [ ] Migrate Data Lake storage to Amazon S3 / Google Cloud Storage
- [ ] Move Kafka cluster to AWS MSK managed service
- [ ] Scale processing layer with AWS EMR / Databricks
- [ ] Replace hardcoded fraud threshold with a Scikit-Learn ML model that learns baseline customer behaviour and dynamically flags anomalous transactions based on multi-dimensional feature analysis
- [ ] Add WebSocket support for true sub-second dashboard refresh
- [ ] Implement role-based access control for the executive dashboard
- [ ] Build a mobile-responsive version of the monitoring interface

## Academic Context

> Task 80 — Data Engineering Project
> Bachelor of Technology · Information Technology
> KIIT Deemed to be University, Bhubaneswar · 2025–2026
