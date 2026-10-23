# Market Demand Forecasting Pipeline

An educational end-to-end Big Data Analytics project using the UCI Online Retail dataset. The full local stack uses Apache Hadoop HDFS, PySpark, Parquet, and the existing Streamlit dashboard. The software and local pipeline are free; no paid cloud services are used.

## Run the full pipeline (Windows)

Prerequisites: Docker Desktop with its engine running, an internet connection for the first build and UCI download, and enough free disk space for the Hadoop image and local data.

From this project folder, run:

```powershell
docker compose up --build
```

The first run builds a small Hadoop image and downloads the UCI dataset if it is not cached. The pipeline then validates and cleans the normalized sales in Spark, writes Parquet datasets to HDFS, fits and evaluates a forecasting model, saves a local results summary, and starts Streamlit.

- Streamlit dashboard: <http://localhost:8501>
- HDFS NameNode status: <http://localhost:9870>

Stop the stack with `Ctrl+C`, then run `docker compose down` to stop its containers. Named HDFS volumes are retained between runs. Do not remove volumes if you want to keep the HDFS files.

## Pipeline stages

1. **Source and ingestion:** downloads the same UCI workbook used by the existing app. The current Python loader normalizes the source into product and sales-line CSV files.
2. **Spark validation and ETL:** PySpark reads the normalized transaction CSV, checks required fields, filters invalid sales, removes exact duplicate rows, and creates daily aggregates by product and country.
3. **Storage:** writes line-level, daily-market, engineered-feature, and forecast datasets in Parquet format to HDFS. HDFS replication is configured across two DataNode containers.
4. **Feature engineering:** builds daily product series, fills dates without recorded sales with zero, and calculates 1/7/14/21/28-day lags, 7/28-day rolling means, same-weekday four-week average, weekday, month, and observed median price.
5. **Forecasting and evaluation:** trains a Spark Gradient-Boosted Trees regressor and compares it with recent 7-day average, previous-week same-day, and four-week same-weekday averages on the final 30 historical days. It reports overall MAE/RMSE, errors by actual demand band, and per-product results for the 20 highest-volume validation products. A flat seven-day projection is provided for demonstration.
6. **Inventory decision signals:** displays demand cover and a reorder-point proxy using an assumed 7-day lead time. These are not actual order recommendations because UCI has no stock levels, supplier lead times, or store IDs.
7. **Dashboard:** the **End-to-End Pipeline** tab shows model and baseline metrics, validation error slices, top-product validation results, side-by-side forecast comparisons, and HDFS output locations.

## HDFS note

This uses real Hadoop HDFS services: one NameNode and two DataNode processes in Docker containers. They all run on the same computer. This demonstrates HDFS storage and replication, but does not provide machine-level fault tolerance or production security. The older storage tab in the original workbench remains an illustrative local-storage simulation.

## Data limits

The dataset describes UK online retail transactions from December 2010 through December 2011. It contains product descriptions, quantities, prices, invoice timestamps, customer IDs, and countries. Country is used only as a rough market grouping; it is not a store identifier. There are no inventory balances, costs, supplier lead times, promotions, or holiday calendar. Consequently, the forecast is historical and educational, and inventory outputs are demand-only proxies.

## Existing demonstrations

The original tabs remain available for the local HDFS concept simulation, MapReduce-style aggregation and join, stream-mining algorithms, product similarity and clustering, and the per-product forecasting walkthrough.

## Main project files

- `pipeline_runner.py` — PySpark ETL, features, model evaluation, and HDFS Parquet writes.
- `docker-compose.yml` — local NameNode, two DataNodes, and Streamlit pipeline services.
- `docker/hadoop/` — Hadoop image, HDFS configuration, and service entrypoint.
- `storage_and_data.py` — UCI download, source normalization, local cache, and original storage adapter.
- `app.py` — Streamlit dashboards and analytics demonstrations.
- `bigdata_algorithms.py` — original algorithm demonstrations and simple forecasting helper.
