# FMCG Data Integration & Analytics Platform

## Overview

This project demonstrates the design and implementation of a cloud-based data platform for a Fast-Moving Consumer Goods (FMCG) organization following the acquisition of a nutrition brand.

The objective was to consolidate data from both businesses into a unified reporting environment, enabling consistent analytics across customers, products, pricing, and sales while supporting business decision-making through centralized dashboards.

---

## Business Problem

Following the acquisition, the parent FMCG company and the nutrition brand operated with separate datasets, reporting structures, and business processes.

This created several challenges:

* Data was distributed across multiple sources and formats.
* Reporting was performed independently for each business.
* Product, customer, and pricing information lacked standardization.
* Leadership lacked a consolidated view of revenue and performance.
* Generating business insights required significant manual effort.

The organization required a modern analytics platform capable of integrating data across both businesses and providing a single source of truth for reporting and analysis.

---

## Solution

A Lakehouse-based data platform was developed using **Databricks**, **PySpark**, **Delta Lake**, and **AWS S3**.

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** to support scalable data ingestion, transformation, and analytics.

The platform:

* Ingests raw operational data from multiple business entities.
* Applies cleansing, standardization, and transformation rules.
* Creates analytics-ready datasets for reporting.
* Supports incremental processing using Delta Lake.
* Delivers business insights through interactive Power BI dashboards.

---

## Architecture

```text
Raw Data Sources
       │
       ▼
AWS S3 Storage
       │
       ▼
Bronze Layer
(Raw Ingestion)
       │
       ▼
Silver Layer
(Data Cleansing & Transformation)
       │
       ▼
Gold Layer
(Business Metrics & Reporting)
       │
       ▼
Power BI Dashboard
```

---

## Data Scope

The platform integrates and processes:

| Metric            | Value                                          |
| ----------------- | ---------------------------------------------- |
| Orders            | 155K+                                          |
| Products          | 417                                            |
| Customers         | 57                                             |
| Business Entities | Parent FMCG Company + Acquired Nutrition Brand |

---

## Key Features

### Data Integration

* Consolidated datasets from both businesses into a unified reporting model.
* Standardized customer, product, pricing, and sales information.

### ETL & Data Processing

* Developed automated ETL pipelines using PySpark.
* Implemented incremental loading using Delta Lake MERGE operations.
* Reduced dependency on full-table reprocessing.

### Data Quality & Transformation

* Applied cleansing and validation rules.
* Resolved inconsistencies across reporting grains and source systems.
* Improved consistency of business reporting metrics.

### Analytics & Reporting

* Built Power BI dashboards for revenue, customer, channel, and product analysis.
* Enabled consolidated reporting across both businesses.
* Delivered visibility into revenue performance and business trends.

---

## Business Insights

Analysis of the consolidated dataset revealed:

* Retailer channels contributed approximately **78% of total revenue**.
* Revenue demonstrated significant growth during peak sales periods.
* Product and customer concentration analysis highlighted key revenue drivers.
* Unified reporting enabled performance comparison across both businesses.

---

## Technology Stack

| Category        | Technologies           |
| --------------- | ---------------------- |
| Data Platform   | Databricks             |
| Programming     | Python, PySpark        |
| Storage         | AWS S3                 |
| Data Lake       | Delta Lake             |
| Data Processing | ETL Pipelines          |
| Analytics       | SQL                    |
| Visualization   | Power BI               |
| Architecture    | Medallion Architecture |

---

## Repository Structure

```text
0_data/
├── Parent Company Data
├── Nutrition Brand Data

1_setup/
├── Environment Setup
├── Configuration Scripts

2_dimension_data_processing/
├── Customer Processing
├── Product Processing
├── Pricing Processing

3_fact_data_processing/
├── Orders Processing
├── Revenue Processing
├── Gold Layer Outputs
```

---

## Dashboard

The Power BI dashboard provides:

* Revenue performance monitoring
* Customer performance analysis
* Product performance tracking
* Channel-wise revenue contribution
* Trend analysis across reporting periods

---

## Learning Outcomes

This project provided hands-on experience with:

* Data Integration
* Data Warehousing Concepts
* Lakehouse Architecture
* ETL Development
* Incremental Data Processing
* Data Modeling
* Business Intelligence Reporting
* End-to-End Analytics Delivery

---

## Author

Anusha Dhiman

Data Analytics | Business Analysis | Data Engineering
