# London Housing Prices – Databricks Project

## Overview
End-to-end data engineering project using **Databricks & PySpark** to process
UK Land Registry housing data for London.

The pipeline follows a **Medallion Architecture**:
- Bronze: Raw ingestion
- Silver: Cleaning & enrichment
- Gold: Aggregated analytics

## Tech Stack
- Databricks
- PySpark
- Delta Lake
- DBFS / Mounted Storage
- GitHub

## Data Source
UK Land Registry – Price Paid Data (2025 sample)

## Pipeline Architecture
CSV → Bronze (Delta) → Silver (Cleaned) → Gold (Aggregated)


## Notebooks
| Notebook | Description |
|--------|------------|
| 01_bronze_ingestion.py | Reads CSV with explicit schema |
| 02_silver_transformation.py | Cleans data & derives date columns |
| 03_gold_aggregation.py | Aggregates avg price & sales count |

## Sample Outputs

### Bronze Table
![Bronze](bronze_table_preview.png)

### Silver Table
![Silver](silver_table_preview.png)

### Gold Table
![Gold](gold_table_preview.png)

### Job Execution
![Job](job_success.png)

## Key Features
- Explicit schema definition
- Delta Lake with overwriteSchema handling
- Production-ready joins & transformations
- Enterprise-safe storage paths

## Author
Jagadeesh Jothi Selvaraj
London, UK
