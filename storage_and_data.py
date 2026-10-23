"""
storage_and_data.py
===================
Storage Layer & Data Ingestion for Big Data Analytics.

Big Data Architecture Concepts:
1. HDFS (Hadoop Distributed File System):
   - NameNode: Holds metadata (namespace, file-to-block mapping).
   - DataNode: Stores physical blocks (128 MB default block size).
   - Rack Awareness: 2 replicas on the local rack, 1 replica on a remote rack for cross-switch fault tolerance.
   - In this project: `HDFSAdapter` stores files under `hdfs_sim/` with detailed block metadata simulation.

2. NoSQL Store (MongoDB / Document Store):
   - A local JSON document-store simulation caches dataset metadata and forecasts.
"""

import json
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.request import Request, urlopen
from zipfile import ZipFile
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
HDFS_DIR = BASE_DIR / "hdfs_sim"
DATA_DIR = HDFS_DIR / "online_retail"
NOSQL_FILE = BASE_DIR / "nosql_sim.json"
DATASET_METADATA_FILE = DATA_DIR / "dataset_metadata.json"
ONLINE_RETAIL_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
ONLINE_RETAIL_PAGE = "https://archive.ics.uci.edu/dataset/352/online+retail"
NORMALIZATION_VERSION = 2

# ==============================================================================
# 1. HDFS Storage Adapter
# ==============================================================================
class HDFSAdapter:
    """Stores CSV files locally and exposes illustrative HDFS metadata."""
    def __init__(self, sim_dir: Path = HDFS_DIR, use_real_hdfs: bool = False):
        self.sim_dir = sim_dir
        self.use_real_hdfs = use_real_hdfs
        self.sim_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, filename: str, df: pd.DataFrame) -> str:
        filepath = self.sim_dir / filename
        df.to_csv(filepath, index=False)
        return str(filepath)

    def read_csv(self, filename: str, dtype=None) -> pd.DataFrame:
        filepath = self.sim_dir / filename
        if not filepath.exists():
            return pd.DataFrame()
        return pd.read_csv(filepath, dtype=dtype)

    def list_files(self):
        return [f.name for f in self.sim_dir.glob("*.csv")]

    def get_file_stats(self, filename: str):
        filepath = self.sim_dir / filename
        if not filepath.exists():
            return {}
        size_bytes = filepath.stat().st_size
        block_size_bytes = 128 * 1024 * 1024
        return {
            "file": filename,
            "size_bytes": size_bytes,
            "size_kb": round(size_bytes / 1024, 2),
            "logical_block_size": "128 MB",
            "required_blocks": max(1, (size_bytes + block_size_bytes - 1) // block_size_bytes)
        }

    def get_hdfs_block_metadata(self) -> list:
        """
        Emulates NameNode fsimage/editlog block mapping.
        Demonstrates Hadoop Rack-Awareness:
        Replica 1 & 2 on Rack /rack-01 (same rack, different DataNodes)
        Replica 3 on Rack /rack-02 (different rack to survive switch failures)
        """
        blocks = []
        for filename in self.list_files():
            stats = self.get_file_stats(filename)
            num_blocks = stats.get("required_blocks", 1)
            for b_idx in range(num_blocks):
                block_id = f"blk_{1073741825 + len(blocks)}"
                blocks.append({
                    "file_path": f"/datalake/raw/{filename}",
                    "block_id": block_id,
                    "block_index": b_idx,
                    "size_bytes": stats.get("size_bytes", 0),
                    "replication_factor": 3,
                    "datanode_replicas": [
                        "dn-01.rack1.internal:9866 (Rack 1)",
                        "dn-02.rack1.internal:9866 (Rack 1)",
                        "dn-03.rack2.internal:9866 (Rack 2 - Remote)"
                    ],
                    "status": "HEALTHY"
                })
        return blocks

    def get_info(self):
        return {
            "storage_mode": "Simulated HDFS (Local Storage Engine)",
            "block_size": "128 MB (Apache Hadoop Standard)",
            "replication_policy": "Rack-Aware 3x (2 local, 1 remote)",
            "storage_root": str(self.sim_dir),
            "files": self.list_files()
        }

# ==============================================================================
# 2. NoSQL Store (Document Store)
# ==============================================================================
class NoSQLStore:
    """Local JSON document-store simulation for forecasts and dataset metadata."""
    def __init__(self, json_path: Path = NOSQL_FILE):
        self.json_path = json_path
        self.data = self._load()

    def _load(self):
        if self.json_path.exists():
            try:
                with open(self.json_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save(self):
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, default=str)

    def save(self, collection: str, key: str, record: dict):
        if collection not in self.data:
            self.data[collection] = {}
        self.data[collection][key] = record
        self._save()

    def get(self, collection: str, key: str = None):
        col = self.data.get(collection, {})
        if key:
            return col.get(key)
        return col

# ==============================================================================
# 3. Real Retail Data Ingestion
# ==============================================================================
def load_online_retail_dataset(force_download: bool = False):
    """Load and normalize the UCI Online Retail dataset, caching it as local CSV files."""
    hdfs = HDFSAdapter(DATA_DIR)
    nosql = NoSQLStore()

    if not force_download and DATASET_METADATA_FILE.exists():
        metadata = json.loads(DATASET_METADATA_FILE.read_text(encoding="utf-8"))
        if (
            metadata.get("source_url") == ONLINE_RETAIL_URL
            and metadata.get("normalization_version") == NORMALIZATION_VERSION
        ):
            df_products = hdfs.read_csv(
                "products.csv", dtype={"product_id": str}
            )
            df_transactions = hdfs.read_csv(
                "sales_transactions.csv",
                dtype={"tx_id": str, "country": str, "product_id": str, "customer_id": str},
            )
            if not df_products.empty and not df_transactions.empty:
                return df_products, df_transactions, metadata

    request = Request(ONLINE_RETAIL_URL, headers={"User-Agent": "Market-Demand-Forecasting/1.0"})
    try:
        with urlopen(request, timeout=120) as response:
            archive_bytes = response.read()
    except Exception as error:
        raise RuntimeError(f"Could not download the UCI dataset from {ONLINE_RETAIL_URL}: {error}") from error

    try:
        with ZipFile(BytesIO(archive_bytes)) as archive:
            workbook_name = next(
                name for name in archive.namelist()
                if name.lower().endswith(".xlsx")
            )
            raw = pd.read_excel(
                BytesIO(archive.read(workbook_name)),
                engine="openpyxl",
            )
    except Exception as error:
        raise RuntimeError(f"Could not read the downloaded UCI dataset: {error}") from error

    required_columns = {
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country",
    }
    missing_columns = required_columns.difference(raw.columns)
    if missing_columns:
        raise ValueError(f"The UCI dataset is missing expected columns: {sorted(missing_columns)}")

    source_rows = len(raw)
    cleaned = raw.dropna(
        subset=["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "Country"]
    ).copy()
    cleaned["InvoiceNo"] = cleaned["InvoiceNo"].astype(str).str.replace(r"\.0$", "", regex=True)
    cleaned["StockCode"] = cleaned["StockCode"].astype(str).str.strip()
    cleaned["Description"] = cleaned["Description"].astype(str).str.strip()
    cleaned["Country"] = cleaned["Country"].astype(str).str.strip()
    cleaned["Quantity"] = pd.to_numeric(cleaned["Quantity"], errors="coerce")
    cleaned["UnitPrice"] = pd.to_numeric(cleaned["UnitPrice"], errors="coerce")
    cleaned["InvoiceDate"] = pd.to_datetime(cleaned["InvoiceDate"], errors="coerce")
    valid_sales = (
        ~cleaned["InvoiceNo"].str.upper().str.startswith("C")
        & cleaned["InvoiceDate"].notna()
        & cleaned["Quantity"].gt(0)
        & cleaned["UnitPrice"].gt(0)
        & cleaned["StockCode"].str.fullmatch(r"\d+")
        & cleaned["Description"].ne("")
        & cleaned["Country"].ne("")
    )
    cleaned = cleaned.loc[valid_sales].copy()
    if cleaned.empty:
        raise ValueError("No valid sales rows remained after cleaning the UCI dataset.")

    cleaned["product_id"] = cleaned["StockCode"]
    cleaned["name"] = cleaned["Description"]
    products = (
        cleaned.groupby("product_id", as_index=False)
        .agg(
            name=("name", lambda values: values.mode().iloc[0]),
            price=("UnitPrice", "median"),
            units_sold=("Quantity", "sum"),
        )
        .sort_values("product_id")
    )
    products["tags"] = products["name"].map(
        lambda name: " ".join(dict.fromkeys(re.findall(r"[a-z0-9]+", name.lower())))
    )
    products = products[["product_id", "name", "price", "units_sold", "tags"]]

    cleaned = cleaned.sort_values("InvoiceDate")
    transactions = pd.DataFrame({
        "tx_id": cleaned["InvoiceNo"].astype(str),
        "date": cleaned["InvoiceDate"].dt.strftime("%Y-%m-%d"),
        "timestamp": cleaned["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "country": cleaned["Country"],
        "product_id": cleaned["product_id"],
        "quantity": cleaned["Quantity"].astype("int64"),
        "unit_price": cleaned["UnitPrice"].astype(float),
        "total_revenue": (cleaned["Quantity"] * cleaned["UnitPrice"]).round(2),
        "customer_id": cleaned["CustomerID"].astype("Int64").astype("string"),
    })

    metadata = {
        "dataset": "UCI Online Retail",
        "normalization_version": NORMALIZATION_VERSION,
        "source_url": ONLINE_RETAIL_URL,
        "source_page": ONLINE_RETAIL_PAGE,
        "currency": "GBP",
        "source_rows": source_rows,
        "cleaned_sales_rows": len(transactions),
        "excluded_rows": source_rows - len(transactions),
        "products": int(products["product_id"].nunique()),
        "invoices": int(transactions["tx_id"].nunique()),
        "countries": int(transactions["country"].nunique()),
        "customers_with_id": int(transactions["customer_id"].nunique()),
        "start_date": transactions["date"].min(),
        "end_date": transactions["date"].max(),
        "cleaning": "Removed rows with missing required sales fields, cancellations, non-product stock codes, invalid dates, non-positive quantities or prices.",
        "loaded_at": datetime.now().isoformat(timespec="seconds"),
    }

    hdfs.write_csv("products.csv", products)
    hdfs.write_csv("sales_transactions.csv", transactions)
    DATASET_METADATA_FILE.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    nosql.save("metadata", "lake_summary", {
        "dataset": metadata["dataset"],
        "source_page": ONLINE_RETAIL_PAGE,
        "currency": metadata["currency"],
        "total_products": metadata["products"],
        "total_sales_lines": metadata["cleaned_sales_rows"],
        "unique_invoices": metadata["invoices"],
        "total_revenue": float(transactions["total_revenue"].sum()),
        "date_range": f'{metadata["start_date"]} to {metadata["end_date"]}',
    })

    return products, transactions, metadata
