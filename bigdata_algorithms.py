"""
bigdata_algorithms.py
=====================
Core Big Data Analytics Algorithms for Undergraduates:
1. MapReduce Execution Engine:
   - Input Split -> Map Phase -> Partition & Shuffle/Sort -> Reduce Phase
   - Reduce-Side Join (Tagging and Merging)
2. Mining Streaming Data:
   - Bloom Filter (Sub-linear membership query, zero false negatives)
   - DGIM Algorithm (Sliding-window counter with logarithmic space)
   - Flajolet-Martin Algorithm (Distinct item estimation via trailing hash zeros)
3. High-Dimensional Similarity & Clustering:
   - Jaccard Similarity on Set Characteristics
   - K-Means product segmentation (Price vs Demand)
4. Distributed Demand Forecasting:
   - Moving Average, Linear Trend Projection, and MAE Metric
"""

import math
import hashlib
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ==============================================================================
# 1. MapReduce Execution Engine
# ==============================================================================
def mapreduce_product_demand_trace(df_transactions: pd.DataFrame, num_reducers: int = 3):
    """
    Demonstrates a local, non-distributed MapReduce-style pipeline:
    1. Input Splits
    2. Map Phase: emits key-value pairs (K2, V2)
    3. Partitioner: hash(key) mod num_reducers
    4. Shuffle & Sort: network exchange and key collation
    5. Reduce Phase: aggregation per key
    """
    if num_reducers < 1:
        raise ValueError("num_reducers must be at least 1.")

    # These are illustrative local splits, not work dispatched to cluster workers.
    base_rows, extra_rows = divmod(len(df_transactions), 3)
    sample_splits = [
        {"split_id": f"Split-0{i+1}", "rows": base_rows + (i < extra_rows)}
        for i in range(3)
    ]

    def reducer_for(product_id):
        digest = hashlib.sha256(str(product_id).encode()).digest()
        return int.from_bytes(digest[:8], "big") % num_reducers

    sample_emissions = []
    for row in df_transactions.head(12).itertuples(index=False):
        key = str(row.product_id)
        sample_emissions.append({
            "key": key,
            "value": {
                "tx_id": str(row.tx_id),
                "quantity": int(row.quantity),
                "revenue": float(row.total_revenue),
            },
            "partition": reducer_for(key),
        })

    # Vectorized aggregation keeps the real, large dataset responsive.
    df_reduced = (
        df_transactions.groupby("product_id", as_index=False)
        .agg(
            units_sold=("quantity", "sum"),
            total_revenue=("total_revenue", "sum"),
            sales_line_count=("tx_id", "size"),
        )
    )
    df_reduced["total_revenue"] = df_reduced["total_revenue"].round(2)
    df_reduced["assigned_reducer"] = df_reduced["product_id"].map(
        lambda product_id: f"Reducer-{reducer_for(product_id)}"
    )
    df_reduced = df_reduced.sort_values(by="units_sold", ascending=False)

    return {
        "splits": sample_splits,
        "sample_map_emissions": sample_emissions,
        "total_emissions_count": len(df_transactions),
        "reduced_df": df_reduced
    }

def mapreduce_reduce_side_join_trace(df_transactions: pd.DataFrame, df_products: pd.DataFrame):
    """
    Simulates Hadoop Reduce-Side Join:
    - Mappers tag product records as 'P' and transaction records as 'T'
    - Shuffle groups all records sharing product_id onto the same Reducer
    - Reducer loops through 'T' records and joins the single 'P' metadata
    """
    tagged_records = []
    for _, p in df_products.iterrows():
        tagged_records.append({
            "key": p["product_id"],
            "tag": "P (Catalog)",
            "payload": {"name": p["name"], "price": p["price"]}
        })
    for _, t in df_transactions.head(20).iterrows():
        tagged_records.append({
            "key": t["product_id"],
            "tag": "T (Transaction)",
            "payload": {"tx_id": t["tx_id"], "quantity": t["quantity"], "revenue": t["total_revenue"]}
        })

    grouped = defaultdict(list)
    for item in tagged_records:
        grouped[item["key"]].append((item["tag"], item["payload"]))

    joined = []
    for prod_id, items in grouped.items():
        prod_info = next((v[1] for v in items if "P" in v[0]), None)
        if not prod_info:
            continue
        for tag, data in items:
            if "T" in tag:
                joined.append({
                    "tx_id": data["tx_id"],
                    "product_id": prod_id,
                    "product_name": prod_info["name"],
                    "unit_price": prod_info["price"],
                    "quantity": data["quantity"],
                    "revenue": data["revenue"]
                })

    return {
        "tagged_sample": tagged_records[:10],
        "joined_df": pd.DataFrame(joined)
    }

# ==============================================================================
# 2. Mining Streaming Data
# ==============================================================================
class BloomFilter:
    """
    Probabilistic space-efficient data structure.
    - m: size of bit array
    - k: number of independent hash functions
    - Zero False Negatives: if an item was added, bits are guaranteed to be 1.
    - False Positive Probability p ≈ (1 - e^(-kn/m))^k
    """
    def __init__(self, size: int = 128, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [0] * size
        self.inserted_count = 0

    def get_hashes(self, item: str):
        hashes = []
        for i in range(self.num_hashes):
            digest = hashlib.md5(f"{item}_salt_{i}".encode()).hexdigest()
            pos = int(digest, 16) % self.size
            hashes.append((f"Hash-{i+1}", pos))
        return hashes

    def add(self, item: str):
        for _, pos in self.get_hashes(item):
            self.bit_array[pos] = 1
        self.inserted_count += 1

    def contains(self, item: str) -> bool:
        return all(self.bit_array[pos] == 1 for _, pos in self.get_hashes(item))

    def false_positive_probability(self) -> float:
        if self.inserted_count == 0:
            return 0.0
        exponent = - (self.num_hashes * self.inserted_count) / self.size
        p = (1.0 - math.exp(exponent)) ** self.num_hashes
        return round(p, 4)

class DGIM:
    """
    Datar-Gionis-Indyk-Motwani (DGIM) Algorithm.
    Counts number of 1s (surge purchases) in sliding window N.
    Memory: O(log^2 N) bits instead of N bits.
    Max error bounded by at most 50%.
    """
    def __init__(self, window_size: int = 60):
        self.window_size = window_size
        self.timestamp = 0
        self.buckets = []  # List of dicts: {"size": int, "end_time": int}

    def update(self, bit: int):
        self.timestamp += 1
        # 1. Expire buckets older than window N
        self.buckets = [b for b in self.buckets if (self.timestamp - b["end_time"]) <= self.window_size]

        # 2. If new bit is 1, create bucket of size 1
        if bit == 1:
            self.buckets.insert(0, {"size": 1, "end_time": self.timestamp})
            self._compact()

    def _compact(self):
        # Enforce invariant: at most 2 buckets of any given size 2^j
        i = 0
        while i < len(self.buckets) - 2:
            s1 = self.buckets[i]["size"]
            s2 = self.buckets[i+1]["size"]
            s3 = self.buckets[i+2]["size"]
            if s1 == s2 == s3:
                # Merge oldest two into size 2 * s2
                self.buckets[i+1]["size"] = s2 * 2
                self.buckets.pop(i+2)
            else:
                i += 1

    def count_ones(self) -> int:
        if not self.buckets:
            return 0
        if len(self.buckets) == 1:
            return self.buckets[0]["size"]
        # Sum all buckets except oldest, add half of oldest
        total = sum(b["size"] for b in self.buckets[:-1])
        total += self.buckets[-1]["size"] // 2
        return total

def flajolet_martin_trace(stream_items: list):
    """
    Flajolet-Martin Algorithm for distinct element counting.
    Hashes each element, finds trailing zeros R in binary representation.
    Distinct elements estimate = 2^R / 0.77351.
    """
    max_r = 0
    item_traces = []
    for item in stream_items:
        h = int(hashlib.sha256(str(item).encode()).hexdigest(), 16)
        binary_str = bin(h)[2:]
        trailing_zeros = len(binary_str) - len(binary_str.rstrip('0'))
        if trailing_zeros > max_r:
            max_r = trailing_zeros
        if len(item_traces) < 10:
            item_traces.append({
                "item": str(item),
                "binary_suffix": "..." + binary_str[-8:],
                "trailing_zeros_R": trailing_zeros
            })

    estimate = int((2 ** max_r) / 0.77351)
    return {
        "max_trailing_zeros_R": max_r,
        "raw_estimate": estimate,
        "sample_traces": item_traces,
        "exact_distinct_count": len(set(stream_items))
    }

# ==============================================================================
# 3. High-Dimensional Similarity & Clustering
# ==============================================================================
def compute_jaccard_matrix(df_products: pd.DataFrame, top_n: int = 30):
    """
    Jaccard Similarity J(A, B) = |A ∩ B| / |A ∪ B|.
    Measures overlap of product characteristic tag sets.
    """
    if top_n < 1:
        raise ValueError("top_n must be at least 1.")
    products = df_products
    if "units_sold" in products.columns:
        products = products.nlargest(top_n, "units_sold")
    else:
        products = products.head(top_n)
    records = products.to_dict(orient="records")
    data = []
    for p1 in records:
        set1 = set(p1["tags"].split())
        row = {"Product": p1["name"]}
        for p2 in records:
            set2 = set(p2["tags"].split())
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            jaccard = round(intersection / union, 2) if union > 0 else 0.0
            row[p2["product_id"]] = jaccard
        data.append(row)
    return pd.DataFrame(data)

def cluster_products_kmeans(df_products: pd.DataFrame, df_transactions: pd.DataFrame, n_clusters: int = 3):
    """
    Groups products by scaled price and sales quantity using K-Means.
    Representative points are included only as a visual illustration, not as CURE.
    """
    demand = df_transactions.groupby("product_id")["quantity"].sum().reset_index()
    merged = pd.merge(df_products, demand, on="product_id", how="left").fillna(0)

    X = merged[["price", "quantity"]].values
    n_distinct = len(np.unique(X, axis=0))
    effective_clusters = min(n_clusters, n_distinct)
    if effective_clusters < 1:
        raise ValueError("At least one product is required for clustering.")
    scaler = StandardScaler()
    scaled_X = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=effective_clusters, random_state=42, n_init=10)
    merged["cluster_id"] = kmeans.fit_predict(scaled_X)

    centroids = scaler.inverse_transform(kmeans.cluster_centers_)

    # Calculate representative points shrunk by alpha = 0.2 toward centroid
    alpha = 0.2
    rep_points = []
    for c_id, centroid in enumerate(centroids):
        pts = X[merged["cluster_id"] == c_id]
        if len(pts) > 0:
            # Pick extreme point and shrink towards centroid
            farthest_idx = np.argmax(np.linalg.norm(pts - centroid, axis=1))
            shrunk_pt = pts[farthest_idx] + alpha * (centroid - pts[farthest_idx])
            rep_points.append({
                "cluster_id": c_id,
                "centroid_price": round(centroid[0], 2),
                "centroid_demand": round(centroid[1], 2),
                "rep_point_price": round(shrunk_pt[0], 2),
                "rep_point_demand": round(shrunk_pt[1], 2)
            })

    merged["segment_label"] = merged["cluster_id"].map(lambda c: f"Segment {c + 1}")
    return merged, pd.DataFrame(rep_points)

# ==============================================================================
# 4. Distributed Demand Forecasting
# ==============================================================================
def forecast_product_demand(df_transactions: pd.DataFrame, product_id: str, forecast_days: int = 7):
    """
    Evaluates 7-Day Rolling Moving Average and Linear Extrapolation.
    Measures MAE on a held-out tail of historical observations.
    """
    prod_tx = df_transactions[df_transactions["product_id"] == product_id].copy()
    if prod_tx.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    daily = prod_tx.groupby("date")["quantity"].sum().reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values(by="date").reset_index(drop=True)

    # Include zero-sales days so the time series has a consistent daily interval.
    date_range = pd.date_range(daily["date"].min(), daily["date"].max(), freq="D")
    daily = (
        daily.set_index("date")
        .reindex(date_range, fill_value=0)
        .rename_axis("date")
        .reset_index()
    )
    daily["moving_avg_7d"] = daily["quantity"].rolling(window=7, min_periods=1).mean().round(2)

    y = daily["quantity"].to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    if len(y) > 1:
        validation_days = min(7, max(1, len(y) // 5))
        split_at = len(y) - validation_days
        if split_at >= 2:
            validation_slope, validation_intercept = np.polyfit(
                x[:split_at], y[:split_at], 1
            )
            validation_prediction = np.maximum(
                0, validation_slope * x[split_at:] + validation_intercept
            )
            mae = float(np.mean(np.abs(y[split_at:] - validation_prediction)))
        else:
            mae = 0.0
        slope, intercept = np.polyfit(x, y, 1)
    else:
        slope, intercept = 0.0, float(y[0]) if len(y) > 0 else 0.0
        mae = 0.0

    last_date = daily["date"].max()
    future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, forecast_days + 1)]
    future_x = np.arange(len(daily), len(daily) + forecast_days)
    future_y = np.maximum(0, np.round(slope * future_x + intercept, 2))

    df_forecast = pd.DataFrame({
        "date": future_dates,
        "forecasted_demand": future_y
    })

    metrics = {
        "model": "OLS Linear Trend + 7D Rolling Average",
        "trend_slope": round(float(slope), 4),
        "mean_absolute_error_mae": round(mae, 2),
        "validation_days": min(7, max(1, len(daily) // 5)) if len(daily) > 1 else 0,
        "historical_days_evaluated": len(daily),
    }

    return daily, df_forecast, metrics
