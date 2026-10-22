"""
bigdata_algorithms.py
=====================
Core Big Data Analytics Algorithms for Undergraduates:
1. MapReduce (Demand Counter & Reduce-side Join)
2. Stream Mining (Bloom Filter, DGIM, Flajolet-Martin)
3. Product Similarity & Clustering (Jaccard, Cosine, K-Means/CURE)
4. Time-Series Demand Forecasting (Moving Average & Linear Trend)
"""

import math
import hashlib
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

# ==============================================================================
# 1. MapReduce: Batch Analytics
# ==============================================================================
def mapreduce_product_demand(df_transactions: pd.DataFrame):
    """
    Simulates a 2-stage MapReduce job:
    - Map: reads each row and emits (product_id, (quantity, revenue))
    - Shuffle & Sort: groups emissions by product_id
    - Reduce: aggregates total quantity sold and revenue per product
    """
    # 1. MAP PHASE
    intermediate = []
    for _, row in df_transactions.iterrows():
        key = str(row["product_id"])
        val = (int(row["quantity"]), float(row["total_revenue"]))
        intermediate.append((key, val))

    # 2. SHUFFLE & SORT
    grouped = defaultdict(list)
    for key, val in intermediate:
        grouped[key].append(val)

    # 3. REDUCE PHASE
    reduced = []
    for product_id, values in grouped.items():
        total_qty = sum(v[0] for v in values)
        total_rev = sum(v[1] for v in values)
        tx_count = len(values)
        reduced.append({
            "product_id": product_id,
            "units_sold": total_qty,
            "total_revenue": round(total_rev, 2),
            "order_count": tx_count
        })

    return pd.DataFrame(reduced).sort_values(by="units_sold", ascending=False)

def mapreduce_reduce_side_join(df_transactions: pd.DataFrame, df_products: pd.DataFrame):
    """
    Simulates a Reduce-Side Join in MapReduce:
    - Mapper tags records with origin: ('PROD', product_meta) or ('TX', transaction_meta)
    - Reducer receives all records for product_id and joins them.
    """
    tagged = []
    for _, p in df_products.iterrows():
        tagged.append((p["product_id"], ("P", {"name": p["name"], "category": p["category"], "price": p["price"]})))
    for _, t in df_transactions.iterrows():
        tagged.append((t["product_id"], ("T", {"tx_id": t["tx_id"], "quantity": t["quantity"], "revenue": t["total_revenue"]})))

    grouped = defaultdict(list)
    for key, val in tagged:
        grouped[key].append(val)

    joined = []
    for prod_id, items in grouped.items():
        prod_info = next((v[1] for v in items if v[0] == "P"), None)
        if not prod_info:
            continue
        for tag, data in items:
            if tag == "T":
                joined.append({
                    "tx_id": data["tx_id"],
                    "product_id": prod_id,
                    "product_name": prod_info["name"],
                    "category": prod_info["category"],
                    "quantity": data["quantity"],
                    "revenue": data["revenue"]
                })
    return pd.DataFrame(joined)

# ==============================================================================
# 2. Stream Mining Algorithms
# ==============================================================================
class BloomFilter:
    """
    Probabilistic data structure for stream membership testing.
    Zero False Negatives; small, tunable False Positive rate.
    """
    def __init__(self, size: int = 256, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [0] * size

    def _hashes(self, item: str):
        hashes = []
        for i in range(self.num_hashes):
            h = int(hashlib.md5(f"{item}_{i}".encode()).hexdigest(), 16)
            hashes.append(h % self.size)
        return hashes

    def add(self, item: str):
        for idx in self._hashes(item):
            self.bit_array[idx] = 1

    def contains(self, item: str) -> bool:
        return all(self.bit_array[idx] == 1 for idx in self._hashes(item))

class DGIM:
    """
    Datar-Gionis-Indyk-Motwani (DGIM) algorithm.
    Counts number of 1s (e.g. surge sales) in a sliding window N with logarithmic memory.
    Maintains at most 2 buckets of any power-of-2 size.
    """
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.timestamp = 0
        # Buckets: list of (size, end_timestamp)
        self.buckets = []

    def update(self, bit: int):
        self.timestamp += 1
        # Expire old buckets beyond sliding window
        self.buckets = [b for b in self.buckets if (self.timestamp - b[1]) <= self.window_size]

        if bit == 1:
            # Create new bucket of size 1
            self.buckets.insert(0, (1, self.timestamp))
            self._compact()

    def _compact(self):
        # Merge if more than 2 buckets of the same size exist
        i = 0
        while i < len(self.buckets) - 2:
            s1, s2, s3 = self.buckets[i][0], self.buckets[i+1][0], self.buckets[i+2][0]
            if s1 == s2 == s3:
                # Merge oldest two into size 2*s2
                merged = (s2 * 2, self.buckets[i+1][1])
                self.buckets.pop(i+2)
                self.buckets[i+1] = merged
            else:
                i += 1

    def count_ones(self) -> int:
        """Estimates total 1s: sum all buckets except the oldest (which contributes half)."""
        if not self.buckets:
            return 0
        if len(self.buckets) == 1:
            return self.buckets[0][0]
        full_sum = sum(b[0] for b in self.buckets[:-1])
        oldest_half = self.buckets[-1][0] // 2
        return full_sum + oldest_half

def flajolet_martin_distinct_count(stream_items: list) -> int:
    """
    Flajolet-Martin Algorithm for estimating distinct elements in a stream using bit hashes.
    Estimate = 2^R / 0.77351 (phi correction factor).
    """
    max_zeros = 0
    for item in stream_items:
        h = int(hashlib.sha256(str(item).encode()).hexdigest(), 16)
        binary = bin(h)[2:]
        trailing_zeros = len(binary) - len(binary.rstrip('0'))
        if trailing_zeros > max_zeros:
            max_zeros = trailing_zeros
    estimate = int((2 ** max_zeros) / 0.77351)
    return min(estimate, len(stream_items))

# ==============================================================================
# 3. Product Similarity & Clustering
# ==============================================================================
def calculate_jaccard_similarity(set_a: set, set_b: set) -> float:
    union = set_a.union(set_b)
    if not union:
        return 0.0
    return round(len(set_a.intersection(set_b)) / len(union), 3)

def compute_product_similarities(df_products: pd.DataFrame):
    """Computes pairwise Jaccard tag similarity for retail cross-selling recommendations."""
    products = df_products.to_dict(orient="records")
    matrix = []
    for p1 in products:
        tags1 = set(p1["tags"].split())
        row = {"product": p1["name"]}
        for p2 in products:
            tags2 = set(p2["tags"].split())
            row[p2["product_id"]] = calculate_jaccard_similarity(tags1, tags2)
        matrix.append(row)
    return pd.DataFrame(matrix)

def cluster_products_by_market(df_products: pd.DataFrame, df_transactions: pd.DataFrame, n_clusters: int = 3):
    """
    Groups products by Price vs Total Demand using K-Means (CURE/Centroid concept).
    Segments into: e.g. Budget Essentials, Mid-Tier Favorites, Premium Luxury.
    """
    demand = df_transactions.groupby("product_id")["quantity"].sum().reset_index()
    merged = pd.merge(df_products, demand, on="product_id", how="left").fillna(0)

    X = merged[["price", "quantity"]].values
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    merged["cluster"] = kmeans.fit_predict(X)

    # Assign descriptive segment names
    cluster_labels = {
        0: "Segment A (High Volume)",
        1: "Segment B (Premium/Niche)",
        2: "Segment C (Balanced Everyday)"
    }
    merged["segment"] = merged["cluster"].map(lambda c: cluster_labels.get(c, f"Cluster {c}"))
    return merged

# ==============================================================================
# 4. Time-Series Demand Forecasting
# ==============================================================================
def forecast_product_demand(df_transactions: pd.DataFrame, product_id: str = "P101", forecast_days: int = 7):
    """
    Calculates 7-day Moving Average and Linear Trend extrapolation for future retail demand.
    """
    prod_tx = df_transactions[df_transactions["product_id"] == product_id].copy()
    if prod_tx.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily = prod_tx.groupby("date")["quantity"].sum().reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values(by="date")

    # 7-day Moving Average
    daily["moving_avg_7d"] = daily["quantity"].rolling(window=7, min_periods=1).mean().round(2)

    # Simple Linear Regression (Trend)
    x = np.arange(len(daily))
    y = daily["quantity"].values
    if len(x) > 1:
        slope, intercept = np.polyfit(x, y, 1)
    else:
        slope, intercept = 0, y[0] if len(y) > 0 else 0

    # Project next N days
    last_date = daily["date"].max()
    future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, forecast_days + 1)]
    future_x = np.arange(len(daily), len(daily) + forecast_days)
    future_y = np.maximum(0, np.round(slope * future_x + intercept, 2))

    df_forecast = pd.DataFrame({
        "date": future_dates,
        "forecasted_demand": future_y
    })

    return daily, df_forecast
