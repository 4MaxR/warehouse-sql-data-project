# 📚 Enterprise Data Catalog: Gold Layer (Business Analytics)

## 🏛️ Architecture Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It is modeled using a **Star Schema** architecture, optimized for high-performance querying in Business Intelligence (BI) tools.

---

### 1. **gold.dim_customers**
- **Purpose:** Stores customer details enriched with demographic and geographic data. Conformed from CRM and ERP sources.

| Column Name | Data Type | Description | Business Logic / Transformations |
| :--- | :--- | :--- | :--- |
| `customer_key` | INT | Surrogate key uniquely identifying each customer record in the dimension table. | Auto-generated using `ROW_NUMBER()`. |
| `customer_id` | INT | Unique numerical identifier assigned to each customer. | Retained for system mapping to CRM. |
| `customer_number` | NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tracking and referencing. | Natural Key mapped from Bronze layer. |
| `first_name` | NVARCHAR(50) | The customer's first name, as recorded in the system. | Cleansed of formatting spaces via `TRIM()`. |
| `last_name` | NVARCHAR(50) | The customer's last name or family name. | Cleansed of formatting spaces via `TRIM()`. |
| `country` | NVARCHAR(50) | The country of residence for the customer (e.g., 'Australia'). | Joined from the ERP location dimension. |
| `marital_status`| NVARCHAR(50) | The marital status of the customer (e.g., 'Married', 'Single'). | Standardized (e.g., 'S' -> 'Single', 'M' -> 'Married'). |
| `gender` | NVARCHAR(50) | The gender of the customer (e.g., 'Male', 'Female', 'n/a'). | CRM data prioritized as the master source. Missing CRM data is backfilled using ERP data via `COALESCE()`. |
| `birthdate` | DATE | The date of birth of the customer, formatted as YYYY-MM-DD. | Joined from ERP system. |
| `create_date` | DATE | The date and time when the customer record was created in the system. | Extracted directly from CRM. |

---

### 2. **gold.dim_products**
- **Purpose:** Provides information about the products and their attributes. Filters out historical records to only show currently active products.

| Column Name | Data Type | Description | Business Logic / Transformations |
| :--- | :--- | :--- | :--- |
| `product_key` | INT | Surrogate key uniquely identifying each product record in the product dimension table. | Auto-generated using `ROW_NUMBER()`. |
| `product_id` | INT | A unique identifier assigned to the product for internal tracking and referencing. | Retained for reverse-auditing. |
| `product_number` | NVARCHAR(50) | A structured alphanumeric code representing the product, often used for categorization or inventory. | Extracted from complex Bronze string patterns. |
| `product_name` | NVARCHAR(50) | Descriptive name of the product, including key details such as type, color, and size. | Trimmed of trailing/leading whitespace. |
| `category_id` | NVARCHAR(50) | A unique identifier for the product's category, linking to its high-level classification. | Extracted using `SUBSTRING()` logic. |
| `category` | NVARCHAR(50) | The broader classification of the product (e.g., Bikes, Components) to group related items. | Joined from ERP master data. |
| `subcategory` | NVARCHAR(50) | A more detailed classification of the product within the category, such as product type. | Joined from ERP master data. |
| `maintenance_required`| NVARCHAR(50)| Indicates whether the product requires maintenance (e.g., 'Yes', 'No'). | Joined from ERP master data. |
| `cost` | INT | The cost or base price of the product, measured in monetary units. | Missing values defaulted to 0. |
| `product_line` | NVARCHAR(50) | The specific product line or series to which the product belongs (e.g., Road, Mountain). | Mapped abbreviations (e.g., 'M' -> 'Mountain', 'R' -> 'Road'). |
| `start_date` | DATE | The date when the product became available for sale or use. | Cast to standard DATE type. |

---

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes. Acts as the central fact table.

| Column Name | Data Type | Description | Business Logic / Transformations |
| :--- | :--- | :--- | :--- |
| `order_number` | NVARCHAR(50) | A unique alphanumeric identifier for each sales order (e.g., 'SO54496'). | Natural Key mapped from Bronze layer. |
| `product_key` | INT | Surrogate key linking the order to the product dimension table. | Mapped via string-to-string join on `product_number`. |
| `customer_key` | INT | Surrogate key linking the order to the customer dimension table. | Mapped via integer-to-integer join on `customer_id`. |
| `order_date` | DATE | The date when the order was placed. | Cast from YYYYMMDD integer; invalid dates set to NULL. |
| `shipping_date` | DATE | The date when the order was shipped to the customer. | Cast from YYYYMMDD integer; invalid dates set to NULL. |
| `due_date` | DATE | The date when the order payment was due. | Cast from YYYYMMDD integer; invalid dates set to NULL. |
| `sales_amount` | INT | The total monetary value of the sale for the line item, in whole currency units (e.g., 25). | Recalculated mathematically if original value was NULL or negative (`quantity * price`). |
| `quantity` | INT | The number of units of the product ordered for the line item (e.g., 1). | Cleansed of negatives. |
| `price` | INT | The price per unit of the product for the line item, in whole currency units (e.g., 25). | Recalculated mathematically if original value was NULL or negative (`sales_amount / quantity`). |

---

## 🔗 ER Diagram & Join Logic
When querying the Gold Layer, business users and BI developers should always start with `fact_sales` and use `LEFT JOIN` to attach the required dimensions using the integer surrogate keys:

* `fact_sales.customer_key` ➔ `dim_customers.customer_key`
* `fact_sales.product_key` ➔ `dim_products.product_key`

> **⚠️ Performance Tuning Note for Analysts:** > Due to the dynamic window functions (`ROW_NUMBER()`) inside the underlying dimension views, complex analytical queries joining all three tables may cause "Nested Loop" execution plan bottlenecks in SQL Server. It is highly recommended to append the `OPTION (HASH JOIN)` query hint to enforce memory-optimized execution plans.
> 
> **Example:**
> ```sql
> SELECT * FROM gold.fact_sales f
> LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
> LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
> OPTION (HASH JOIN);
> ```
