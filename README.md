# Modern Data Warehouse with SQL Server

## 📖 Description
This project focuses on building a modern data warehouse using SQL Server. It implements a robust ETL pipeline, dimensional data modeling, and analytics capabilities to transform raw operational data into business-ready insights.

## 🏗 Architecture Overview
This data warehouse follows the **Medallion Architecture** pattern (Bronze, Silver, Gold) to ensure data quality, lineage, and performance.

### 1. Sources
Data is ingested from external operational systems via CSV files.
*   **Systems:** CRM and ERP.
*   **Interface:** Files In Folder.
*   **Object Type:** CSV Files.

### 2. Data Warehouse Layers

#### 🥉 Bronze Layer (Raw Data)
The ingestion layer where data is loaded "as-is" from the source systems.
*   **Objective:** Store raw data with minimal latency.
*   **Object Type:** Tables.
*   **Load Strategy:** Batch Processing, Full Load, Truncate & Insert.
*   **Transformations:** None (Data Model: None).

#### 🥈 Silver Layer (Cleaned & Standardized)
The refinement layer where data is cleansed, standardized, and conformed.
*   **Objective:** Create a single version of the truth.
*   **Object Type:** Tables.
*   **Load Strategy:** Batch Processing, Full Load, Truncate & Insert.
*   **Transformations:**
    *   Data Cleansing
    *   Data Standardization
    *   Data Normalization
    *   Derived Columns
    *   Data Enrichment

#### 🥇 Gold Layer (Business-Ready Data)
The presentation layer optimized for analytics and reporting.
*   **Objective:** Support business logic and high-performance querying.
*   **Object Type:** Tables.
*   **Transformations:** Aggregations, business logic application, final cleansing.
*   **Data Model:**
    *   Star Schema (Dimension and Fact tables)
    *   Flat Tables
    *   Aggregated Tables

### 3. Consumption
The final layer where data is consumed by end-users and applications.
*   **BI & Reporting:** Dashboards and visualizations.
*   **Ad-Hoc SQL Queries:** Direct querying for analysis.
*   **Machine Learning:** Feeding models with clean, aggregated data.

## 🛠 Tech Stack
*   **Database Engine:** Microsoft SQL Server
*   **ETL/ELT:** Stored Procedures / SSIS / Azure Data Factory (depending on implementation)
*   **Data Modeling:** Dimensional Modeling (Star Schema)

## 📂 Project Structure
*(Suggested structure based on the architecture)*

```text
.
├── /ddl                 # Database definition scripts (Create DB, Schemas)
├── /bronze              # Scripts for Bronze layer tables (Raw)
├── /silver              # Scripts for Silver layer tables (Cleaned)
├── /gold                # Scripts for Gold layer tables (Dimensions/Facts)
├── /etl                 # Stored procedures for data loading
│   ├── load_bronze.sql
│   ├── load_silver.sql
│   └── load_gold.sql
└── README.md
```

## Getting Started 🚀
### Prerequisites
- SQL Server instance installed and running.
- Access to the source CSV files (CRM/ERP exports).
### Installation
1. Clone the repository.
2. Run the scripts in the /ddl folder to set up the database schemas.
3. Execute the table creation scripts in /bronze, /silver, and /gold in order.
4. Configure the connection strings for the source file locations.
5. Execute the ETL stored procedures to populate the warehouse.

## Naming Conventions 📝

This project adheres to strict naming conventions to ensure maintainability:
- Style: snake_case (lowercase with underscores).
- Language: English.
- Bronze/Silver Tables: <sourcesystem>_<entity> (e.g., crm_customer_info).
- Gold Tables: <category>_<entity> (e.g., dim_customer, fact_sales).
- Keys: Surrogate keys use the suffix _key (e.g., customer_key).
- Technical Columns: Prefixed with dwh_ (e.g., dwh_load_date).
