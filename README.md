# Retail-Q: Retail Analytics Platform on Databricks

Retail-Q is a retail analytics data engineering platform built on Databricks using PySpark, Delta Lake, Structured Streaming and the medallion architecture. It brings together data from Salesforce (accounts and opportunities), PostgreSQL (product catalog and inventory) and cloud storage (sales transactions), applies data quality rules with declarative pipeline expectations, and builds fact and dimension models plus a semantic metric view for analytics and reporting.

## Architecture

```text
Salesforce ───────► retail_q.salesforce_bronze (account, opportunity)  ─┐
PostgreSQL ───────► retail_q.postgres_bronze  (product_catalog,        │
                                               inventory)               ├─► Silver (expectations,
Cloud storage CSV ─► Auto Loader ─► retail_q.blob_bronze.transactions  ─┘     standardisation)
                                                                                    │
                                                                                    ▼
                                         Gold: fact_sales, dim_customer, dim_product,
                                               fact_inventory, calendar
                                                                                    │
                                                                                    ▼
                                         Semantic layer: retail_semantic.retail_metrics
```

The Salesforce and PostgreSQL bronze tables are created outside this repository (the code only reads them). The PostgreSQL bronze tables carry an `__END_AT` column, i.e. they are kept with SCD Type 2 history, and the silver step keeps only current rows.

## Tech stack

- Databricks, Unity Catalog (catalog `retail_q`, volumes)
- Lakeflow / Spark Declarative Pipelines (`pyspark.pipelines`) with expectations
- Auto Loader (`cloudFiles`) and Structured Streaming
- Delta Lake
- Databricks SQL views and Unity Catalog metric views (YAML)

## Repository structure

```text
.
├── Notebooks/
│   ├── blob_to_bronze.py         # Auto Loader: CSV transactions → blob_bronze.transactions
│   ├── 02_Gold_Views.py          # Gold views: dim_customer, dim_product, fact_inventory
│   ├── 03_calendar.py            # Parameterised calendar (date dimension) table
│   └── 04_Metric View.py         # Metric view retail_semantic.retail_metrics
└── Pipelines/
    ├── bronze_to_silver/         # Declarative pipeline: one file per silver table
    │   ├── account.py
    │   ├── opportunity.py
    │   ├── product_catalog.py
    │   ├── inventory.py
    │   └── transactions.py
    └── silver_to_gold/
        └── fact_sales.py         # Transactions joined to opportunities
```

## What each stage does

**Bronze – transactions (`Notebooks/blob_to_bronze.py`)** – streams CSV files from `/Volumes/retail_q/volumes/blob_source/transactions_source/` with Auto Loader (`addNewColumns` schema evolution) into `retail_q.blob_bronze.transactions`, using an `availableNow` trigger and a checkpoint.

**Silver (`Pipelines/bronze_to_silver/`)** – each table trims and standardises text (upper-case IDs, `initcap` names, lower-case websites), adds `silver_processed_at`, and enforces expectations:

| Table | Fail on | Drop rows where | Warn |
| --- | --- | --- | --- |
| `account` | null `Id` or `Name` | negative revenue or employee count | deleted accounts |
| `opportunity` | null `Id`, `Name`, `AccountId`, `CloseDate` | negative amount, probability outside 0–100 | won but not closed |
| `product_catalog` | null `product_id` or `product_name` | negative price, null category | active product without price |
| `inventory` | null inventory, product or store ID | negative stock or reorder level | stock at or below reorder level |
| `transactions` | – | null IDs, quantity ≤ 0, negative price or discount | discount above line value |

`transactions` also casts quantity, prices (`decimal(10,2)`) and the timestamp.

**Gold**
- `fact_sales` (`Pipelines/silver_to_gold/fact_sales.py`) left-joins transactions to opportunities on the normalised opportunity name and exposes transaction measures, `transaction_date`, payment mode, sales channel, opportunity stage, owner, amount and the account ID as `customer_id`.
- `02_Gold_Views.py` creates `dim_customer` (active, non-deleted accounts), `dim_product` (active products) and `fact_inventory`.
- `03_calendar.py` builds a calendar table between the `:start_date` and `:end_date` parameters with year, quarter, month, week, day names, weekend/weekday flags and month boundary flags.

**Semantic layer (`Notebooks/04_Metric View.py`)** – a metric view over `fact_sales` joined to product, calendar and customer. It defines dimensions (date, year, quarter, month, category, brand, payment mode, sales channel, opportunity stage, customer type/name, billing location, industry) and measures (transaction count, total revenue, total quantity, total discount, average transaction value, unique customers), with display names, formats and synonyms for natural-language querying.

## How to run

Prerequisites: a Databricks workspace with Unity Catalog, a `retail_q` catalog with the required schemas, a volume at `/Volumes/retail_q/volumes/blob_source/`, and the Salesforce and PostgreSQL data already ingested into `salesforce_bronze` and `postgres_bronze`.

1. Upload transaction CSV files to `transactions_source/` and run `Notebooks/blob_to_bronze.py`.
2. Create a declarative pipeline from `Pipelines/bronze_to_silver/` and run it.
3. Create a pipeline from `Pipelines/silver_to_gold/` and run it.
4. Run `02_Gold_Views.py`, then `03_calendar.py` with `start_date` and `end_date`, then `04_Metric View.py`.

## Notes and possible improvements

The code is not fully consistent yet, so some objects may need aligning before an end-to-end run:

- Schema names differ between files: silver tables are written to `retail_q.silver.*`, but some readers use `retail_q.retail_silver.*`; gold objects are created in both `retail_q.gold` and `retail_q.retail_gold`.
- Silver keeps source column names such as `Name`, `StageName`, `BillingCity`, while the gold code reads snake_case names such as `name`, `stage_name`, `billing_city`, `is_active`.
- The metric view joins `retail_gold.dim_calendar`, while the calendar notebook creates `retail_gold.calendar`.
- `dim_product` selects `product_segment`, which the silver product table does not produce.
