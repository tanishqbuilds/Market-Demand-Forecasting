"""
app.py
======
Market Demand Forecasting System
Interactive Big Data Analytics & Distributed Systems Architecture Workbench.

Tech Stack: Python 3.10+, Streamlit, Scikit-Learn, Pandas, NumPy.
Zero frontend build overhead; runs entirely from Python backend.
"""

import streamlit as st
import pandas as pd
import numpy as np
from storage_and_data import HDFSAdapter, NoSQLStore, generate_dataset
from bigdata_algorithms import (
    mapreduce_product_demand_trace,
    mapreduce_reduce_side_join_trace,
    BloomFilter,
    DGIM,
    flajolet_martin_trace,
    compute_jaccard_matrix,
    cluster_products_cure_concept,
    forecast_product_demand
)

# Page configuration
st.set_page_config(
    page_title="Big Data Analytics Workbench | Demand Forecasting",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Clean CSS (No gimmicks, enterprise data platform typography)
st.markdown("""
<style>
    .main-header {
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
        font-size: 26px;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 2px;
    }
    .sub-header {
        font-size: 13px;
        color: #64748B;
        margin-bottom: 20px;
        letter-spacing: 0.3px;
    }
    .metric-badge {
        display: inline-block;
        padding: 3px 8px;
        background-color: #F1F5F9;
        border: 1px solid #CBD5E1;
        border-radius: 4px;
        font-family: monospace;
        font-size: 11px;
        color: #334155;
    }
    .pipeline-step {
        background-color: #F8FAFC;
        border-left: 3px solid #3B82F6;
        padding: 10px 14px;
        margin-bottom: 8px;
        font-size: 13px;
    }
    .bit-cell-1 {
        background-color: #2563EB;
        color: white;
        padding: 4px;
        text-align: center;
        font-family: monospace;
        font-size: 11px;
        border-radius: 2px;
    }
    .bit-cell-0 {
        background-color: #E2E8F0;
        color: #64748B;
        padding: 4px;
        text-align: center;
        font-family: monospace;
        font-size: 11px;
        border-radius: 2px;
    }
</style>
""", unsafe_allow_html=True)

# Application Header
st.markdown("<div class='main-header'>Market Demand Forecasting Engine</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>Distributed Storage Architecture • MapReduce Execution Pipeline • Stream Mining • High-Dimensional Clustering • Demand Forecasting</div>", unsafe_allow_html=True)

# Initialize Storage Adapters
hdfs = HDFSAdapter()
nosql = NoSQLStore()

# Load Datasets from HDFS lake
df_products = hdfs.read_csv("products.csv")
df_transactions = hdfs.read_csv("sales_transactions.csv")

if df_products.empty or df_transactions.empty:
    with st.spinner("Ingesting baseline datasets into HDFS lake..."):
        df_products, df_transactions = generate_dataset()

# Sidebar: Cluster & Storage Status
with st.sidebar:
    st.markdown("### Cluster Infrastructure")
    st.markdown(f"""
    **Storage Layer:** HDFS (Local Simulated Adapter)  
    **NameNode Namespace:** `/datalake/raw/`  
    **Replication Policy:** 3x Rack-Aware  
    **Block Size:** 128 MB (Hadoop Spec)  
    **Serving Database:** NoSQL Document Store  
    """)

    st.markdown("---")
    st.markdown("### Lake Ingestion Control")
    num_tx_input = st.slider("Transaction Ingestion Batch", min_value=500, max_value=3000, value=1500, step=250)

    if st.button("Trigger Ingestion Job", type="primary"):
        df_products, df_transactions = generate_dataset(num_transactions=num_tx_input)
        st.success(f"Ingested {num_tx_input} transactions into HDFS.")
        st.rerun()

    st.markdown("---")
    st.metric(label="Active Master Catalog", value=f"{len(df_products)} SKUs")
    st.metric(label="Total POS Records", value=f"{len(df_transactions):,} Records")
    st.metric(label="Aggregate Lake Revenue", value=f"${df_transactions['total_revenue'].sum():,.2f}")

# Main Navigation Tabs
tab_hdfs, tab_mr, tab_stream, tab_cluster, tab_forecast = st.tabs([
    "[1] Storage & HDFS Topology",
    "[2] MapReduce Execution",
    "[3] Stream Mining Engine",
    "[4] Similarity & Clustering",
    "[5] Demand Forecasting"
])

# ==============================================================================
# TAB 1: Storage & HDFS Topology
# ==============================================================================
with tab_hdfs:
    st.markdown("#### HDFS Cluster Topology & NameNode Block Mapping")
    st.caption("Demonstrating NameNode metadata management, 128 MB logical chunking, and cross-rack replication.")

    col_meta, col_diag = st.columns([1, 1])

    with col_meta:
        st.markdown("##### NameNode In-Memory File System Image (fsimage)")
        block_metadata = hdfs.get_hdfs_block_metadata()
        df_blocks = pd.DataFrame(block_metadata)
        if not df_blocks.empty:
            st.dataframe(df_blocks[["file_path", "block_id", "size_bytes", "replication_factor", "status"]])

        st.markdown("##### NoSQL Serving Tier Cache (JSON / MongoDB State)")
        nosql_meta = nosql.get("metadata", "lake_summary")
        st.json(nosql_meta or {"status": "Active"})

    with col_diag:
        st.markdown("##### Distributed Rack Placement Architecture")
        st.markdown("""
        ```
        [Client Ingestion] 
              |
              v (RPC Request)
        [NameNode:50070] ---> Returns Block IDs + DataNode Pipeline
              |
              v (Direct Streaming Pipeline: DN1 -> DN2 -> DN3)
        ===========================================================
        RACK /rack-01:
          ├─ DataNode 01: blk_1073741825 (Replica 1 - Primary)
          └─ DataNode 02: blk_1073741825 (Replica 2 - Local Rack)
        -----------------------------------------------------------
        RACK /rack-02 (Remote Switch):
          └─ DataNode 03: blk_1073741825 (Replica 3 - Fault Isolation)
        ===========================================================
        ```
        """)
        st.markdown("##### Raw Ingestion Preview: `sales_transactions.csv`")
        st.dataframe(df_transactions[["tx_id", "date", "store_id", "product_id", "quantity", "unit_price", "total_revenue"]].head(8))

# ==============================================================================
# TAB 2: MapReduce Execution
# ==============================================================================
with tab_mr:
    st.markdown("#### MapReduce Distributed Execution Pipeline")
    st.caption("Step-by-step trace of Map, Shuffle & Sort, and Reduce stages computing product demand aggregates.")

    mr_trace = mapreduce_product_demand_trace(df_transactions, num_reducers=3)

    c_step1, c_step2, c_step3 = st.columns(3)
    with c_step1:
        st.markdown("<div class='pipeline-step'><b>Stage 1: Input Splits & Map</b><br>Emits: <code>(product_id, {qty, rev})</code></div>", unsafe_allow_html=True)
        st.caption("Sample Mapper emissions:")
        sample_emissions = pd.DataFrame([
            {"Key (K2)": e["key"], "Value (V2)": f"qty={e['value']['quantity']}, rev=${e['value']['revenue']}", "Partition": f"P-{e['partition']}"}
            for e in mr_trace["sample_map_emissions"]
        ])
        st.dataframe(sample_emissions.head(6))

    with c_step2:
        st.markdown("<div class='pipeline-step'><b>Stage 2: Shuffle & Sort</b><br>Network partition via <code>hash(key) % 3</code></div>", unsafe_allow_html=True)
        st.caption("Collation by key across Reducer nodes:")
        st.code("""
Reducer-0 <= Hash(Key)%3 == 0 (P101, P104, P107)
Reducer-1 <= Hash(Key)%3 == 1 (P102, P105, P108)
Reducer-2 <= Hash(Key)%3 == 2 (P103, P106, P109)
        """, language="text")

    with c_step3:
        st.markdown("<div class='pipeline-step'><b>Stage 3: Reduce Phase</b><br>Reduces list of values into final totals</div>", unsafe_allow_html=True)
        st.caption("Aggregated demand summary:")
        st.dataframe(mr_trace["reduced_df"][["product_id", "units_sold", "total_revenue", "assigned_reducer"]].head(6))

    st.markdown("---")
    st.markdown("##### MapReduce Reduce-Side Join Implementation")
    st.caption("Joining transactions with master catalog metadata using composite secondary keys.")
    join_trace = mapreduce_reduce_side_join_trace(df_transactions, df_products)
    st.dataframe(join_trace["joined_df"].head(8))

# ==============================================================================
# TAB 3: Stream Mining Engine
# ==============================================================================
with tab_stream:
    st.markdown("#### Sub-linear Space Stream Mining Algorithms")
    st.caption("Processing high-velocity streaming data where memory capacity M is significantly smaller than stream length N.")

    col_bf, col_dgim, col_fm = st.columns(3)

    # 1. Bloom Filter
    with col_bf:
        st.markdown("##### Bloom Filter (Membership)")
        st.caption("Zero false negatives. Approximate membership query.")
        bf = BloomFilter(size=64, num_hashes=3)
        sample_ids = df_transactions["tx_id"].head(50).tolist()
        for s in sample_ids:
            bf.add(s)

        test_item = st.text_input("Query Transaction ID", value="TX_000005")
        hashes = bf.get_hashes(test_item)
        is_member = bf.contains(test_item)

        st.write("**Hash Traces:**")
        for h_name, bit_pos in hashes:
            st.code(f"{h_name}('{test_item}') -> Bit Index {bit_pos}", language="text")

        if is_member:
            st.info(f"Result: '{test_item}' is PROBABLY PRESENT in stream.")
        else:
            st.error(f"Result: '{test_item}' is DEFINITIVELY NOT in stream.")

        st.caption(f"Theoretical False Positive Rate: {bf.false_positive_probability() * 100:.2f}%")

        st.write("**64-Bit Array State:**")
        bits_str = "".join(str(b) for b in bf.bit_array[:32]) + "..."
        st.code(bits_str, language="text")

    # 2. DGIM Algorithm
    with col_dgim:
        st.markdown("##### DGIM Algorithm (Sliding Window)")
        st.caption("Logarithmic-memory counting of surge sales (qty >= 3).")
        window_size = st.slider("Window Size N", min_value=30, max_value=120, value=60, step=10)
        dgim = DGIM(window_size=window_size)

        stream_bits = [1 if q >= 3 else 0 for q in df_transactions["quantity"].head(window_size)]
        for b in stream_bits:
            dgim.update(b)

        exact_count = sum(stream_bits)
        approx_count = dgim.count_ones()
        error_pct = round(abs(approx_count - exact_count) / max(1, exact_count) * 100, 1)

        st.metric(label="Exact 1s in Window", value=exact_count)
        st.metric(label="DGIM Estimated 1s", value=approx_count, delta=f"{error_pct}% Error (Guaranteed <= 50%)")

        st.write(f"**Active Exponential Buckets ({len(dgim.buckets)}):**")
        df_buckets = pd.DataFrame([
            {"Size (2^j)": b["size"], "End Timestamp": b["end_time"], "Age (T - t)": dgim.timestamp - b["end_time"]}
            for b in dgim.buckets
        ])
        st.dataframe(df_buckets)

    # 3. Flajolet-Martin Algorithm
    with col_fm:
        st.markdown("##### Flajolet-Martin (Distinct Count)")
        st.caption("Estimating distinct stream keys via hash trailing zeros.")
        stream_keys = df_transactions["product_id"].tolist() + df_transactions["store_id"].tolist()
        fm_result = flajolet_martin_trace(stream_keys)

        st.metric(label="Exact Distinct Count", value=fm_result["exact_distinct_count"])
        st.metric(label="Flajolet-Martin Estimate", value=fm_result["raw_estimate"], delta=f"{fm_result['raw_estimate'] - fm_result['exact_distinct_count']} Deviation")

        st.markdown(f"**Max Trailing Zeros (R):** `{fm_result['max_trailing_zeros_R']}`")
        st.caption("Formula: $Estimate = 2^R / 0.77351$")
        st.write("**Sample Bit Hash Suffixes:**")
        st.dataframe(pd.DataFrame(fm_result["sample_traces"]).head(6))

# ==============================================================================
# TAB 4: Similarity & Clustering
# ==============================================================================
with tab_cluster:
    st.markdown("#### High-Dimensional Similarity & CURE Clustering")
    st.caption("Identifying cross-sell product affinities and segmenting inventory by demand elasticity.")

    col_jacc, col_cure = st.columns([1, 1])

    with col_jacc:
        st.markdown("##### Pairwise Jaccard Tag Similarity Matrix")
        st.caption("Metric: $J(A, B) = |A \cap B| / |A \cup B|$ computed over product feature sets.")
        df_jaccard = compute_jaccard_matrix(df_products)
        st.dataframe(df_jaccard.set_index("Product"))

    with col_cure:
        st.markdown("##### Market Segmentation (CURE / K-Means)")
        st.caption("Clustering SKUs by Unit Price ($) vs Total Units Demanded.")
        df_clustered, df_reps = cluster_products_cure_concept(df_products, df_transactions, n_clusters=3)
        st.scatter_chart(df_clustered, x="price", y="quantity", color="segment_label")

        st.write("**CURE Representative Points (Shrunk by α = 0.2 toward centroid):**")
        st.dataframe(df_reps)

# ==============================================================================
# TAB 5: Demand Forecasting
# ==============================================================================
with tab_forecast:
    st.markdown("#### Distributed Time-Series Demand Forecasting")
    st.caption("Rolling-window moving averages and autoregressive trend projection.")

    catalog_dict = {row["product_id"]: row["name"] for _, row in df_products.iterrows()}
    sel_pid = st.selectbox(
        "Select Product SKU",
        options=list(catalog_dict.keys()),
        format_func=lambda pid: f"{pid} - {catalog_dict[pid]}"
    )
    horizon = st.slider("Forecast Horizon (Days Ahead)", min_value=3, max_value=14, value=7)

    daily_hist, df_fc, metrics = forecast_product_demand(df_transactions, product_id=sel_pid, forecast_days=horizon)

    if not daily_hist.empty:
        col_chart, col_stats = st.columns([2, 1])

        with col_chart:
            st.markdown(f"##### Historical vs Forecast Demand: `{catalog_dict[sel_pid]}`")
            plot_df = daily_hist[["date", "quantity", "moving_avg_7d"]].rename(columns={
                "quantity": "Actual Daily Demand",
                "moving_avg_7d": "7-Day Rolling Average"
            }).set_index("date")
            st.line_chart(plot_df)

        with col_stats:
            st.markdown("##### Model Performance Metrics")
            st.metric(label="Mean Absolute Error (MAE)", value=f"{metrics['mean_absolute_error_mae']} units")
            st.metric(label="Trend Growth Slope", value=f"{metrics['trend_slope']} / day")
            st.metric(label="Historical Days Sampled", value=f"{metrics['historical_days_evaluated']} Days")

        st.markdown("##### Projected Daily Demand Quantities")
        f_cols = st.columns(len(df_fc))
        for idx, (_, row) in enumerate(df_fc.iterrows()):
            with f_cols[idx]:
                st.metric(
                    label=row["date"].strftime("%a, %b %d"),
                    value=f"{int(row['forecasted_demand'])} units"
                )

        # Cache forecast into NoSQL document store
        nosql.save("forecasts", sel_pid, {
            "product_id": sel_pid,
            "product_name": catalog_dict[sel_pid],
            "predictions": df_fc.to_dict(orient="records"),
            "metrics": metrics
        })
    else:
        st.warning("Insufficient transaction history to construct forecast.")
