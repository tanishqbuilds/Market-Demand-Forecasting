# Market Demand Forecasting Workbench

An educational Big Data Analytics dashboard built with Python and Streamlit. It uses real historical retail sales from the UCI Machine Learning Repository to demonstrate data preparation, aggregation, stream algorithms, product similarity, clustering, and simple demand forecasting.

## Run locally

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

On first launch, the app downloads the UCI dataset, cleans it, and stores normalized CSV files under `hdfs_sim/`. This requires an internet connection. Later launches use the local cached CSVs; use **Reload dataset from UCI** in the sidebar to download and prepare the source again. Open the URL printed by Streamlit, usually <http://localhost:8501>.

## Data source and preparation

The app uses the [UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail), containing transactions from a UK-based online retailer between December 2010 and December 2011. It records invoices, product codes and descriptions, quantities, timestamps, unit prices, customer IDs, and countries. Prices are in British pounds (GBP).

The app excludes cancellation invoices, non-product stock codes, rows with missing required sales fields, invalid dates, and rows with non-positive quantities or prices. It does not fabricate product categories or costs, which are not present in the source. Catalog prices are observed median unit prices; product tags are words taken from the source descriptions. Some customer IDs are missing in the original data.

The source workbook is read from the official UCI archive and is not committed to the repository. The cleaned local cache and JSON results store are ignored by Git.

## What the dashboard demonstrates

- **Decision dashboard:** headline revenue, invoice, customer, and average invoice metrics; monthly revenue and unit trends; top products by revenue and volume; and revenue by country. These summarize historical activity and do not account for inventory, cost, or margin.
- **Storage:** local CSV and JSON files demonstrate storage concepts. The HDFS block metadata, NameNode, replicas, and rack layout are visual simulations, not a Hadoop cluster.
- **MapReduce-style aggregation and join:** product sales are grouped and joined in one Python process. This illustrates Map, partitioning, and Reduce; it does not distribute work across machines.
- **Stream mining:** Bloom filter, DGIM, and Flajolet–Martin examples operate on bounded samples from the real dataset, not a live event feed.
- **Product analysis:** Jaccard similarity compares words in product descriptions for the 30 best-selling products. K-Means groups products using scaled median price and total quantity sold. The representative-point display is illustrative; this is not a CURE implementation.
- **Forecasting:** a moving average and linear trend are shown for a selected product. MAE is measured on the final held-out historical days before the trend is fit to all history for the example projection.

## Limitations

This is a classroom demonstration, not a production inventory or forecasting service. The source data is historical and should not be used as a current sales forecast. The simulated storage, processing, and streaming components are local; there is no distributed cluster, real NoSQL server, or live sales connection. The simple forecast is not benchmarked against robust forecasting models or business costs.

## Project files

- `app.py` — Streamlit dashboard
- `storage_and_data.py` — UCI download, cleaning, local storage adapters
- `bigdata_algorithms.py` — analytics demonstrations
- `requirements.txt` — Python dependencies, including the Excel reader used for the source workbook
