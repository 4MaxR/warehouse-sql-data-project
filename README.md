# 🏛️ Enterprise Data Warehouse — SQL Server (Bronze → Silver → Gold)

A production-grade data warehouse built on **Microsoft SQL Server**, implementing a full **Medallion Architecture** (Bronze / Silver / Gold) to integrate and transform raw CRM and ERP data into business-ready analytics.

---

<div align="center">
  <img src="docs/Questions_Analyze.drawio.svg" alt="Data Warehouse Architecture" />
</div>

## 📌 Project Overview

This project demonstrates an end-to-end data engineering pipeline — from raw source ingestion all the way to a Star Schema reporting layer — using only **T-SQL**, with no external orchestration tools.


| Layer | Role | Output |
|---|---|---|
| **Bronze** | Raw ingestion, no transformations | Staging tables mirroring source schemas |
| **Silver** | Cleansed, standardized, deduplicated | Conformed tables with DWH audit columns |
| **Gold** | Business-ready, analytics-optimized | Star Schema Views (Dimensions + Fact) |

---
<div align="center">
  <img src="docs/Dataset_layers_Diagram.drawio.svg" alt="Dataset layers Diagram" />
</div>

## 🗂️ Repository Structure

```
data_warehouse_project/
│
├── docs/
│   ├── architecture_WH.drawio.svg        # Full warehouse architecture diagram
│   ├── data_catalog.md                   # Gold layer column-level business dictionary
│   ├── data_flow.drawio.svg              # End-to-end data flow diagram
│   ├── Dataset_layers_Diagram.drawio.svg # Dataset → layer mapping
│   ├── Diagram_layers_steps.drawio.svg   # Step-by-step layer pipeline
│   ├── Keys Diagram.drawio.svg           # Surrogate & natural key relationships
│   ├── metadata_columns.drawio.svg       # DWH metadata column usage
│   └── Questions_Analyze.drawio.svg      # Business questions the model answers
│
├── scripts/
│   ├── bronze/
│   │   ├── init_database.sql             # Creates DataWarehouse DB + 3 schemas
│   │   ├── ddl_bronze.sql                # Bronze table definitions
│   │   └── create_tables.sql             # Alternate DDL script
│   │
│   ├── silver/
│   │   ├── ddl_silver.sql                # Silver table definitions (with audit columns)
│   │   ├── proce_load_silver.sql         # Stored procedure: Bronze → Silver ETL
│   │   ├── silver_crm_cust_info.sql
│   │   ├── silver_crm_prod_info.sql
│   │   ├── silver_crm_sales_details.sql
│   │   ├── silver.erp_cust_az12.sql
│   │   ├── silver.erp_loc_a101.sql
│   │   └── silver_erp_px_cat_g1v2.sql
│   │
│   ├── gold/
│   │   ├── ddl_gold.sql                  # Gold views: dim_customers, dim_products, fact_sales
│   │   ├── gold_dim_customers.sql
│   │   ├── gold_dim_products.sql
│   │   ├── gold_fact_sales.sql
│   │   ├── gold_clear_gndr.sql           # MDM gender backfill logic
│   │   └── gold_check_fact_sales.sql     # Orphaned sales record audit
│   │
│   └── datasets/
│       ├── source_crm/                   # CRM source files (customers, products, sales)
│       └── source_erp/                   # ERP source files (customers, locations, categories)
│
└── tests/
    ├── silver/                           # Silver-layer data quality checks (per table)
    └── gold/                             # Gold-layer integrity & MDM validation checks
```

---

## 🏗️ Architecture

<div align="center">
  <img src="docs/architecture_WH.drawio.svg" alt="Data Warehouse Architecture" />
</div>

### Medallion Layers

```
[CRM Source]   [ERP Source]
     │               │
     └──────┬─────────┘
            ▼
      ┌───────────┐
      │  BRONZE   │  Raw staging tables — exact copy of source schemas
      └─────┬─────┘
            │  EXEC silver.load_silver
            ▼
      ┌───────────┐
      │  SILVER   │  Cleansed, deduplicated, standardized, with DWH audit timestamps
      └─────┬─────┘
            │  CREATE OR ALTER VIEW
            ▼
      ┌───────────┐
      │   GOLD    │  Star Schema: dim_customers, dim_products, fact_sales
      └───────────┘
```

### Gold Layer — Star Schema

```
         dim_customers
               │
               │ customer_key
               │
fact_sales ────┤
               │
               │ product_key
               │
         dim_products
```
<div align="center">
  <img src="docs/data_flow.drawio.svg" alt="Data Warehouse Architecture" />
</div>

---

## ⚙️ Data Sources

| System | Files | Contents |
|---|---|---|
| **CRM** | `cust_info.csv`, `prd_info.csv`, `sales_details.csv` | Customer master, product catalog, order transactions |
| **ERP** | `CUST_AZ12.csv`, `LOC_A101.csv`, `PX_CAT_G1V2.csv` | Customer demographics, geographic locations, product categories |

---

## 🔄 ETL Pipeline

### Bronze → Raw Ingestion
Tables are created with `DROP TABLE IF EXISTS` / `CREATE TABLE` to support idempotent re-runs. Source CSV data is bulk-loaded directly into Bronze with no transformations.

### Silver → Cleansing & Standardization
Executed via a single stored procedure:
```sql
EXEC silver.load_silver;
```
Key transformations applied per table:

- **Deduplication** — `ROW_NUMBER()` window function to retain only the latest record per natural key
- **Standardization** — Gender (`'M'` → `'Male'`), marital status (`'S'` → `'Single'`), product line (`'R'` → `'Road'`)
- **Date casting** — Integer-format dates (`YYYYMMDD`) cast to proper `DATE` type; invalid dates set to `NULL`
- **String cleansing** — `TRIM()` applied to all text fields
- **Null handling** — Missing costs defaulted to `0`; sales metrics recalculated from `quantity × price` where invalid
- **Audit columns** — `dwh_create_date DATETIME2 DEFAULT GETDATE()` added to all Silver tables

### Gold → Star Schema Views
Gold objects are implemented as **views** (not physical tables) using `CREATE OR ALTER VIEW`, ensuring the layer always reflects the latest Silver state without manual refresh.

**MDM Business Rule — Gender Backfill:**
```sql
CASE
    WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the master source
    ELSE COALESCE(ca.gen, 'n/a')                 -- ERP as fallback
END AS gender
```

**Surrogate Key Generation:**
```sql
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key
```
<div align="center">
  <img src="docs/Keys Diagram.drawio.svg" alt="Data Warehouse Architecture" />
</div>

---

## 📊 Gold Layer — [Data Catalog](docs/data_catalog.md)

### `gold.dim_customers`
Conformed customer dimension enriched from both CRM and ERP.

| Column | Type | Description |
|---|---|---|
| `customer_key` | INT | Surrogate key (auto-generated) |
| `customer_id` | INT | CRM source identifier |
| `customer_number` | NVARCHAR(50) | Natural key for system mapping |
| `first_name` | NVARCHAR(50) | Trimmed from CRM |
| `last_name` | NVARCHAR(50) | Trimmed from CRM |
| `country` | NVARCHAR(50) | Joined from ERP location |
| `marital_status` | NVARCHAR(50) | Standardized (S/M → Single/Married) |
| `gender` | NVARCHAR(50) | CRM master + ERP fallback via COALESCE |
| `birthdate` | DATE | Joined from ERP |
| `create_date` | DATE | From CRM |

### `gold.dim_products`
Active product dimension — historical records (`prd_end_dt IS NOT NULL`) excluded.

| Column | Type | Description |
|---|---|---|
| `product_key` | INT | Surrogate key (auto-generated) |
| `product_number` | NVARCHAR(50) | Natural key for sales join |
| `product_name` | NVARCHAR(50) | Trimmed product label |
| `category` / `subcategory` | NVARCHAR(50) | Joined from ERP category master |
| `product_line` | NVARCHAR(50) | Decoded (M/R/T/S → Mountain/Road/Touring/Standard) |
| `cost` | INT | Defaulted to 0 if NULL |
| `start_date` | DATE | Cast from source DATETIME |

### `gold.fact_sales`
Central fact table for all transactional sales data.

| Column | Type | Description |
|---|---|---|
| `order_number` | NVARCHAR(50) | Natural key (e.g., `SO54496`) |
| `product_key` | INT | FK → `dim_products` |
| `customer_key` | INT | FK → `dim_customers` |
| `order_date` | DATE | Cast from YYYYMMDD integer |
| `shipping_date` | DATE | Cast from YYYYMMDD integer |
| `due_date` | DATE | Cast from YYYYMMDD integer |
| `sales_amount` | INT | Recalculated if NULL or negative |
| `quantity` | INT | Cleansed of negatives |
| `price` | INT | Recalculated if NULL or negative |

---

## 🧪 Data Quality Tests

Quality checks are organized per layer under `tests/`.

### Silver Checks (per table)
- No duplicate natural keys
- No NULL values in required columns
- Valid date ranges (no future dates, no impossible values)
- Standardized lookup values (no raw codes remaining)

### Gold Checks
```sql
-- Orphaned sales records (no matching product)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
WHERE p.product_key IS NULL
OPTION (HASH JOIN);

-- MDM gender validation (CRM vs ERP backfill audit)
SELECT DISTINCT ci.cst_gndr, ca.gen,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
         ELSE COALESCE(ca.gen, 'n/a') END AS new_gndr
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
ORDER BY 1, 2;
```

> **Performance Note:** All Gold-layer queries that join across views should use `OPTION (HASH JOIN)` to prevent nested loop execution plans caused by `ROW_NUMBER()` window functions in the underlying views.

---

## 🚀 How to Run

### Prerequisites
- Microsoft SQL Server 2016+ (or Azure SQL)
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Source CSV files placed in an accessible path for bulk insert

### Execution Order

```sql
-- Step 1: Initialize the database and schemas
-- Run: scripts/bronze/init_database.sql

-- Step 2: Create Bronze tables
-- Run: scripts/bronze/ddl_bronze.sql

-- Step 3: Bulk load source CSVs into Bronze
-- (Configure bulk insert paths per your environment)

-- Step 4: Create Silver tables
-- Run: scripts/silver/ddl_silver.sql

-- Step 5: Run Bronze → Silver ETL
EXEC silver.load_silver;

-- Step 6: Create Gold views
-- Run: scripts/gold/ddl_gold.sql

-- Step 7: Validate
-- Run all scripts under: tests/silver/ and tests/gold/
```

---

## 🛠️ Tech Stack

| Tool | Usage |
|---|---|
| **Microsoft SQL Server** | Core database engine |
| **T-SQL** | DDL, DML, stored procedures, views, window functions |
| **SQL Server Stored Procedures** | Encapsulated ETL pipeline (Bronze → Silver) |
| **draw.io** | Architecture and data flow diagrams |
| **CSV** | Source data format (CRM + ERP) |

---

## 📎 Key Design Decisions

- **Views over physical tables at Gold layer** — ensures no stale data; always reflects latest Silver state
- **Idempotent ETL** — `TRUNCATE + INSERT` pattern in `silver.load_silver` makes every run safe to re-execute
- **MDM hierarchy** — CRM data treated as the master system; ERP used only as a fallback via `COALESCE`
- **OPTION (HASH JOIN)** — applied to complex Gold queries to prevent query optimizer issues with windowed views
- **Surrogate keys via ROW_NUMBER()** — eliminates dependency on identity columns; view-compatible

---

### 👤 Author

* **Name:** Al Rouby
* **Specialization:** Data Engineering | SQL Server | ETL & Data Modeling
* **Credits:** Inspired by [Mr.Baraa's Channel](https://www.youtube.com/@DataWithBaraa)
* **Connect with me:** [LinkedIn](https://www.linkedin.com/in/mustafa-al-rouby-20218b171/) | [GitHub](https://github.com/4MaxR)
---

*Built as a portfolio project demonstrating end-to-end data warehouse design and implementation using T-SQL.*
