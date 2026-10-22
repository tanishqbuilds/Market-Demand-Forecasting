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
   - Used for serving analytical results, forecasts, and stream aggregates with sub-millisecond point queries.
   - Falls back to an in-memory/JSON dictionary (`nosql_sim.json`) if MongoDB is not running.
"""

import os
import json
import random
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent
HDFS_DIR = BASE_DIR / "hdfs_sim"
NOSQL_FILE = BASE_DIR / "nosql_sim.json"

# ==============================================================================
# 1. HDFS Storage Adapter
# ==============================================================================
class HDFSAdapter:
    """Simulates HDFS storage on local disk, with real HDFS configuration hooks."""
    def __init__(self, sim_dir: Path = HDFS_DIR, use_real_hdfs: bool = False):
        self.sim_dir = sim_dir
        self.use_real_hdfs = use_real_hdfs
        self.sim_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, filename: str, df: pd.DataFrame) -> str:
        filepath = self.sim_dir / filename
        df.to_csv(filepath, index=False)
        return str(filepath)

    def read_csv(self, filename: str) -> pd.DataFrame:
        filepath = self.sim_dir / filename
        if not filepath.exists():
            return pd.DataFrame()
        return pd.read_csv(filepath)

    def list_files(self):
        return [f.name for f in self.sim_dir.glob("*.csv")]

    def get_file_stats(self, filename: str):
        filepath = self.sim_dir / filename
        if not filepath.exists():
            return {}
        size_bytes = filepath.stat().st_size
        return {
            "file": filename,
            "size_bytes": size_bytes,
            "size_kb": round(size_bytes / 1024, 2),
            "logical_block_size": "128 MB",
            "required_blocks": max(1, (size_bytes // (128 * 1024 * 1024)) + 1)
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
    """Document store for fast queries of forecasts and stream metrics."""
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
# 3. Synthetic Retail Data Ingestion
# ==============================================================================
def generate_dataset(num_transactions: int = 1500):
    """Generates synthetic products and sales transactions into the HDFS lake."""
    hdfs = HDFSAdapter()
    nosql = NoSQLStore()

    products_data = [
        {"product_id": "P101", "name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "price": 120.0, "cost": 70.0, "tags": "audio bluetooth travel audio"},
        {"product_id": "P102", "name": "Mechanical Gaming Keyboard", "category": "Electronics", "price": 85.0, "cost": 50.0, "tags": "gaming pc accessories keyboard"},
        {"product_id": "P103", "name": "4K Ultra HD Action Camera", "category": "Electronics", "price": 160.0, "cost": 95.0, "tags": "video sports camera outdoor action"},
        {"product_id": "P104", "name": "Smart Fitness Tracker Band", "category": "Electronics", "price": 49.0, "cost": 28.0, "tags": "fitness wearable health sports tracker"},
        {"product_id": "P105", "name": "Organic Arabica Coffee Beans 1kg", "category": "Groceries", "price": 22.0, "cost": 12.0, "tags": "coffee organic beverage breakfast beans"},
        {"product_id": "P106", "name": "Cold-Pressed Extra Virgin Olive Oil", "category": "Groceries", "price": 15.0, "cost": 8.0, "tags": "cooking oil organic kitchen food"},
        {"product_id": "P107", "name": "Dark Chocolate Sea Salt Bar 85%", "category": "Groceries", "price": 4.5, "cost": 2.0, "tags": "snack chocolate sweet food confectionery"},
        {"product_id": "P108", "name": "Breathable Mesh Running Shoes", "category": "Apparel", "price": 75.0, "cost": 40.0, "tags": "footwear sports running shoes fitness"},
        {"product_id": "P109", "name": "All-Weather Waterproof Jacket", "category": "Apparel", "price": 110.0, "cost": 60.0, "tags": "outerwear winter jacket apparel outdoor"},
        {"product_id": "P110", "name": "Ergonomic Memory Foam Pillow", "category": "Home & Kitchen", "price": 35.0, "cost": 18.0, "tags": "bedding sleep comfort home wellness"}
    ]
    df_products = pd.DataFrame(products_data)
    hdfs.write_csv("products.csv", df_products)

    stores = ["Store_NY_01", "Store_CA_02", "Store_TX_03", "Store_Online_04"]
    start_date = datetime.now() - timedelta(days=60)
    
    transactions = []
    for i in range(1, num_transactions + 1):
        prod = random.choice(products_data)
        random_days = random.randint(0, 59)
        random_hour = random.randint(8, 21)
        tx_time = start_date + timedelta(days=random_days, hours=random_hour, minutes=random.randint(0, 59))
        qty = random.choices([1, 2, 3, 4, 5, 8], weights=[55, 25, 10, 5, 3, 2])[0]
        discount = random.choices([0.0, 0.05, 0.1, 0.15], weights=[75, 10, 10, 5])[0]
        total = round(qty * prod["price"] * (1 - discount), 2)

        transactions.append({
            "tx_id": f"TX_{i:06d}",
            "date": tx_time.strftime("%Y-%m-%d"),
            "timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
            "store_id": random.choice(stores),
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": prod["price"],
            "discount": discount,
            "total_revenue": total
        })

    transactions.sort(key=lambda x: x["timestamp"])
    df_transactions = pd.DataFrame(transactions)
    hdfs.write_csv("sales_transactions.csv", df_transactions)

    nosql.save("metadata", "lake_summary", {
        "generated_at": datetime.now().isoformat(),
        "total_products": len(df_products),
        "total_transactions": len(df_transactions),
        "total_revenue": float(df_transactions["total_revenue"].sum())
    })

    return df_products, df_transactions
