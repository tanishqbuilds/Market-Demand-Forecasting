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
from storage_and_data import (
    DATA_DIR,
    HDFSAdapter,
    NoSQLStore,
    ONLINE_RETAIL_PAGE,
    load_online_retail_dataset,
)
from bigdata_algorithms import (
    mapreduce_product_demand_trace,
    mapreduce_reduce_side_join_trace,
    BloomFilter,
    DGIM,
    flajolet_martin_trace,
    compute_jaccard_matrix,
    cluster_products_kmeans,
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
st.markdown("<div class='sub-header'>Real historical retail data • Local storage simulations • MapReduce-style analytics • Stream Mining • Product Clustering • Demand Forecasting</div>", unsafe_allow_html=True)

# Initialize Storage Adapters
hdfs = HDFSAdapter(DATA_DIR)
nosql = NoSQLStore()

# Load the real UCI dataset, downloading and normalizing it on first run.
try:
    with st.spinner("Loading the UCI Online Retail dataset..."):
        df_products, df_transactions, dataset_metadata = load_online_retail_dataset()
except (RuntimeError, ValueError) as error:
    st.error(f"Unable to load the real retail dataset. {error}")
    st.stop()

# Sidebar: Cluster & Storage Status
with st.sidebar:
    st.markdown("### Dataset")
    st.markdown(f"""
    **Source:** [UCI Online Retail]({ONLINE_RETAIL_PAGE})<br>
    **Period:** {dataset_metadata['start_date']} to {dataset_metadata['end_date']}<br>
    **Currency:** GBP (£)<br>
    **Storage:** Local CSV files (HDFS simulation)
    """)

    st.markdown("---")
    st.markdown("### Dataset Ingestion")
    st.caption(
        f"{dataset_metadata['cleaned_sales_rows']:,} valid sales lines from "
        f"{dataset_metadata['source_rows']:,} source rows. Cancellations and invalid "
        "or non-positive sales are excluded."
    )
    if st.button("Reload dataset from UCI"):
        try:
            with st.spinner("Downloading and preparing the official UCI dataset..."):
                load_online_retail_dataset(force_download=True)
            st.success("Dataset refreshed from UCI.")
            st.rerun()
        except (RuntimeError, ValueError) as error:
            st.error(f"Dataset refresh failed. {error}")

    st.markdown("---")
    st.metric(label="Products", value=f"{len(df_products):,}")
    st.metric(label="Clean sales lines", value=f"{len(df_transactions):,}")
    st.metric(label="Recorded sales", value=f"£{df_transactions['total_revenue'].sum():,.2f}")

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
    st.markdown("#### Local Storage & Simulated HDFS Topology")
    st.caption("The sales are real historical records; HDFS blocks, replicas, racks, and NameNode metadata below are illustrative only.")

    col_meta, col_diag = st.columns([1, 1])

    with col_meta:
        st.markdown("##### NameNode In-Memory File System Image (fsimage)")
        block_metadata = hdfs.get_hdfs_block_metadata()
        df_blocks = pd.DataFrame(block_metadata)
        if not df_blocks.empty:
            st.dataframe(df_blocks[["file_path", "block_id", "size_bytes", "replication_factor", "status"]])

        st.markdown("##### Local JSON Results Cache (NoSQL concept demo)")
        nosql_meta = nosql.get("metadata", "lake_summary")
        st.json(nosql_meta or {"status": "Active"})

    with col_diag:
        st.markdown("##### Illustrative Rack Placement (not a live cluster)")
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
        st.markdown("##### Cleaned UCI Sales Records: `sales_transactions.csv`")
        st.dataframe(df_transactions[["tx_id", "date", "country", "product_id", "quantity", "unit_price", "total_revenue"]].head(8))

# ==============================================================================
# TAB 2: MapReduce Execution
# ==============================================================================
with tab_mr:
    st.markdown("#### MapReduce-Style Sales Aggregation")
    st.caption("A single-process demonstration of Map, Shuffle/Sort, and Reduce over the real sales lines.")

    mr_trace = mapreduce_product_demand_trace(df_transactions, num_reducers=3)

    c_step1, c_step2, c_step3 = st.columns(3)
    with c_step1:
        st.markdown("<div class='pipeline-step'><b>Stage 1: Input Splits & Map</b><br>Emits: <code>(product_id, {qty, rev})</code></div>", unsafe_allow_html=True)
        st.caption("Sample Mapper emissions:")
        sample_emissions = pd.DataFrame([
            {"Key (K2)": e["key"], "Value (V2)": f"qty={e['value']['quantity']}, rev=£{e['value']['revenue']:.2f}", "Reducer": f"Reducer-{e['partition']}"}
            for e in mr_trace["sample_map_emissions"]
        ])
        st.dataframe(sample_emissions.head(6))

    with c_step2:
        st.markdown("<div class='pipeline-step'><b>Stage 2: Shuffle & Sort</b><br>Stable partition via <code>SHA-256(product_id) % 3</code></div>", unsafe_allow_html=True)
        st.caption("Example product keys grouped by simulated reducer:")
        partition_examples = []
        for reducer_id in range(3):
            product_keys = mr_trace["reduced_df"].loc[
                mr_trace["reduced_df"]["assigned_reducer"] == f"Reducer-{reducer_id}",
                "product_id",
            ].head(5)
            partition_examples.append(
                f"Reducer-{reducer_id}: {', '.join(product_keys.astype(str)) or '(no products)'}"
            )
        st.code("\n".join(partition_examples), language="text")

    with c_step3:
        st.markdown("<div class='pipeline-step'><b>Stage 3: Reduce Phase</b><br>Reduces list of values into final totals</div>", unsafe_allow_html=True)
        st.caption("Aggregated demand summary:")
        st.dataframe(mr_trace["reduced_df"][["product_id", "units_sold", "total_revenue", "assigned_reducer"]].head(6))

    st.markdown("---")
    st.markdown("##### MapReduce Reduce-Side Join Implementation")
    st.caption("Illustrative local join of the first 20 cleaned sales lines to catalog names and observed median prices.")
    join_trace = mapreduce_reduce_side_join_trace(df_transactions, df_products)
    st.dataframe(join_trace["joined_df"].head(8))

# ==============================================================================
# TAB 3: Stream Mining Engine
# ==============================================================================
with tab_stream:
    st.markdown("#### Sub-linear Space Stream Mining Algorithms")
    st.caption("Demonstrations of compact stream algorithms using real records; this app is not a live event stream.")

    col_bf, col_dgim, col_fm = st.columns(3)

    # 1. Bloom Filter
    with col_bf:
        st.markdown("##### Bloom Filter (Membership)")
        st.caption("Zero false negatives. Approximate membership query.")
        bf = BloomFilter(size=64, num_hashes=3)
        sample_ids = df_transactions["tx_id"].head(50).tolist()
        for s in sample_ids:
            bf.add(s)

        sample_invoice = str(df_transactions["tx_id"].iloc[min(4, len(df_transactions) - 1)])
        test_item = st.text_input("Query Invoice ID", value=sample_invoice)
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
        st.caption("Logarithmic-memory count of large-quantity purchases in the first N chronological sales lines.")
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
        st.caption("Estimating distinct customer IDs from a bounded sample of actual sales.")
        stream_keys = (
            df_transactions.head(10_000)["customer_id"]
            .dropna()
            .astype(str)
            .tolist()
        )
        fm_result = flajolet_martin_trace(stream_keys)

        st.metric(label="Exact distinct customers in sample", value=fm_result["exact_distinct_count"])
        st.metric(label="Flajolet-Martin sample estimate", value=fm_result["raw_estimate"], delta=f"{fm_result['raw_estimate'] - fm_result['exact_distinct_count']} Deviation")

        st.markdown(f"**Max Trailing Zeros (R):** `{fm_result['max_trailing_zeros_R']}`")
        st.caption("Formula: $Estimate = 2^R / 0.77351$")
        st.caption("Uses customer IDs from the first 10,000 cleaned sales lines; missing IDs are omitted.")
        st.write("**Sample Bit Hash Suffixes:**")
        st.dataframe(pd.DataFrame(fm_result["sample_traces"]).head(6))

# ==============================================================================
# TAB 4: Similarity & Clustering
# ==============================================================================
with tab_cluster:
    st.markdown("#### Product Similarity & K-Means Segmentation")
    st.caption("Comparing product descriptions and grouping products by observed price and sales quantity.")

    col_jacc, col_kmeans = st.columns([1, 1])

    with col_jacc:
        st.markdown("##### Jaccard Similarity of Product Descriptions")
        st.caption("Limited to the 30 best-selling products so the matrix stays readable.")
        st.caption(r"Metric: $J(A, B) = |A \cap B| / |A \cup B|$ computed over product feature sets.")
        df_jaccard = compute_jaccard_matrix(df_products)
        st.dataframe(df_jaccard.set_index("Product"))

    with col_kmeans:
        st.markdown("##### Product Segmentation (K-Means)")
        st.caption("Clusters use scaled observed median price (£) and total units sold. This is K-Means, not CURE.")
        df_clustered, df_reps = cluster_products_kmeans(df_products, df_transactions, n_clusters=3)
        st.scatter_chart(df_clustered, x="price", y="quantity", color="segment_label")

        st.write("**Illustrative centroid-shrunk representative points (not a CURE implementation):**")
        st.dataframe(df_reps)

# ==============================================================================
# TAB 5: Demand Forecasting
# ==============================================================================
with tab_forecast:
    st.markdown("#### Historical Demand Forecast Demonstration")
    st.caption("Simple forecasting methods applied to old retail data; this is not current business advice.")

    active_days = df_transactions.groupby("product_id")["date"].nunique()
    forecastable_ids = set(active_days[active_days >= 30].index)
    forecast_products = df_products[df_products["product_id"].isin(forecastable_ids)]
    forecast_products = forecast_products.sort_values("units_sold", ascending=False)
    catalog_dict = {
        row["product_id"]: row["name"]
        for _, row in forecast_products.iterrows()
    }
    st.caption(
        f"Products with at least 30 days containing sales are available "
        f"({len(catalog_dict):,} of {len(df_products):,} products)."
    )
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
            st.markdown("##### Holdout Validation")
            st.metric(label=f"MAE on {metrics['validation_days']} held-out days", value=f"{metrics['mean_absolute_error_mae']} units")
            st.metric(label="Trend Growth Slope", value=f"{metrics['trend_slope']} / day")
            st.metric(label="Historical Days Sampled", value=f"{metrics['historical_days_evaluated']} Days")
            st.caption("After measuring error on the final held-out days, the trend is refit using all available history for this illustrative projection.")

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
