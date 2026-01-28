# NBA-Data-Engineering-Pipeline

## 🎯 Overview

This project builds a modern, end‑to‑end data pipeline that ingests NBA data from a public API, processes it through a Medallion Architecture (Bronze → Silver → Gold), and produces analytics‑ready datasets for dashboards and insights. The entire system is orchestrated with Airflow, transformed with dbt, stored in Parquet/DuckDB, and fully containerized with Docker.

## 🏀 Data Ingestion (NBA API → Bronze Layer)

- Extract raw NBA data (games, players, teams, stats) from a public API
- Handle pagination, rate limits, retries, and ingestion failures
- Store raw JSON/Parquet files in the Bronze layer
- Automate ingestion using Airflow DAGs

## 🧹 Data Cleaning & Normalization (Bronze → Silver)

- Convert raw API responses into structured, relational tables (Parquet/Delta)
- Clean and normalize nested JSON fields
- Enforce data types and schema consistency
- Implement dbt tests (unique, not null, relationships)
- Store cleaned datasets in the Silver layer

## 📊 Data Modeling & Analytics (Silver → Gold)

- Build dimensional models (players, teams)
- Create fact tables (games, player stats)
- Develop aggregated analytics models (win rates, performance metrics)
- Store analytics‑ready tables in the Gold layer

## ⚙️ Orchestration & Automation

- Use Airflow to orchestrate ingestion and dbt transformations
- Implement DAG dependencies (Bronze → Silver → Gold)
- Add retries, logging, and monitoring
- Schedule the pipeline for automated execution

## 📈 Analytics & Visualization

- Query Gold layer using DuckDB
- Build dashboards (Power BI/Tableau) for:
- Player performance trends
- Team analytics
- Season summaries
- Export visuals for documentation

## 🚫 Out of Scope (for this project)

To keep the project focused and maintainable, the following are excluded:

- Real‑time streaming (Kafka, Kinesis)
- Cloud deployment (Databricks, AWS, GCP, Azure)
- Machine learning models
- Real‑time dashboards
- API serving layers

(These will be part of a separate Databricks cloud pipeline project.)

## ✅ Success Criteria

The project is successful when:

- The pipeline runs end‑to‑end via Airflow

- Bronze, Silver, and Gold layers are populated correctly

- dbt tests pass consistently

- DuckDB queries return expected results

- Dashboards visualize meaningful NBA insights

- The entire system runs reproducibly through Docker
