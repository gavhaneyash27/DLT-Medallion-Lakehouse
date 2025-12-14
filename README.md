# Lakeflow Declarative Pipeline using Asset Bundles and Autoloader
## Project Overview

This project demonstrates an end-to-end data engineering pipeline built using **Lakeflow Declarative Pipelines (Delta Live Tables)**.  
It uses **Databricks Autoloader** for streaming ingestion, applies transformations, and organizes data using the **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline is designed to handle **incremental data ingestion**, enforce **data quality using DLT expectations**, and implement **CDC / SCD Type 2** logic for curated, analytics-ready datasets.

The project is packaged and deployed using **Databricks Asset Bundles**, enabling environment-specific deployments (dev and prod).  
The entire solution is built using the **Databricks Free Edition**, with **Unity Catalog Volumes** used as the data source.
## Architecture
<img width="1797" height="626" alt="dlt_diagram" src="https://github.com/user-attachments/assets/81a30de9-72df-4957-b052-6a26536d6189" />

### Data Source

- CSV files stored in Unity Catalog Volumes
- Used to simulate cloud-based streaming data

### Data Ingestion

- Databricks Autoloader

- Ingests incremental files as a streaming source

### Data Processing Platform

- Lakeflow Declarative Pipelines / Delta Live Tables (DLT)

### Bronze Layer

- Streaming Delta tables

- Raw schema ingestion

### Silver Layer

- Cleaned and enriched Delta tables

- Implemented CDC / SCD Type 2 using Lakeflow auto CDC flows

- Applied DLT expectations for data quality validation

### Gold Layer

- Materialized views

- Business-ready datasets optimized for analytics and reporting

## Lakeflow Declarative Pipeline
<img width="1901" height="801" alt="final pipeline dag mat view" src="https://github.com/user-attachments/assets/baac8a5f-5476-4a0f-904f-917c9ff098b7" />

The pipeline consists of multiple DLT components:

### Bronze Tables

- Streaming tables created using Autoloader

- One bronze table per source entity

### Silver Views

- Streaming views used for transformation and enrichment

### Silver Tables

- Streaming Delta tables

- Implement CDC / SCD Type 2 logic

- Enforce data quality using DLT expectations

### Gold Tables

- Materialized views

- Represent the final curated datasets

## Data Quality (DLT Expectations)

- Enforced in the Silver layer

- Used to validate primary key presence


## CI/CD & Deployment

- Project deployed using Databricks Asset Bundles

- Separate dev and prod targets configured

- Logical separation handled via Asset Bundles
- Due to Free Edition limitations:

  - Dev and Prod are deployed in the same Databricks workspace

## Tech Stack

- Lakeflow Declarative Pipelines / Delta Live Tables

- Databricks Autoloader

- Apache Spark (Structured Streaming)

- Delta Lake

- Unity Catalog

- Databricks Asset Bundles (CI/CD)

## Key Features

- Streaming ingestion using Autoloader

- Declarative pipeline design using Lakeflow Declarative Pipelines

- Medallion Architecture (Bronze, Silver, Gold)

- CDC / SCD Type 2 implementation

- Data quality enforcement using DLT expectations

- Governed ingestion using Unity Catalog Volumes

- CI/CD-ready pipeline using Asset Bundles

- Fully implemented using Databricks Free Edition
