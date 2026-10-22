"""
app.py
======
Market Demand Forecasting System – Interactive Big Data Analytics Dashboard.
Built with Streamlit (100% Python, Zero frontend code required).

To run:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from storage_and_data import HDFSAdapter, NoSQLStore, generate_dataset
from bigdata_algorithms import (
    mapreduce_product_demand,
    mapreduce_reduce_side_join,
    BloomFilter,
    DGIM,
    flajolet_martin_distinct_count,
    compute_product_similarities,
    cluster_products_by_market,
    forecast_product_demand
)

st.set_page_config(page_title="Market Demand Forecasting", layout="wide", page_icon="📈")

# Title and header
st.title("📈 Market Demand Forecasting System")
st.caption("Undergraduate Big Data Analytics Platform: HDFS Lake • MapReduce • Stream Mining • Clustering • Forecasting")

# Initialize storage singletons
hdfs = HDFSAdapter()
nosql = NoSQLStore()

# Load or generate initial data
df_products = hdfs.read_csv("products.csv")
df_transactions = hdfs.read_csv("sales_transactions.csv")

if df_products.empty or df_transactions.empty:
    with st.spinner("Initializing HDFS Lake with synthetic retail datasets..."):
        df_products, df_transactions = generate_dataset()

# Sidebar: Controls & Storage Status
with st.sidebar:
    st.header("⚙️ Big Data Engine")
    st.info(f"**Storage Mode:** Simulated HDFS\n\n**HDFS Directory:** `hdfs_sim/`\n\n**Block Concept:** 128 MB blocks, 3x replication\n\n**NoSQL Tier:** Document JSON/Mongo store")

    if st.button("🔄 Regenerate Lake Data", use_container_width=True):
        df_products, df_transactions = generate_dataset(num_transactions=1500)
        st.success("New transactions ingested into HDFS!")
        st.rerun()

    st.divider()
    st.metric("Total Catalog Products", len(df_products))
    st.metric("Total Ingested Transactions", len(df_transactions))
    st.metric("Total Lake Revenue", f"${df_transactions['total_revenue'].sum():,.2f}")

# Main Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 1. HDFS & Data Ingestion",
    "⚙️ 2. MapReduce Analytics",
    "🌊 3. Stream Mining",
    "🎯 4. Similarity & Clustering",
    "🔮 5. Demand Forecasting"
])

# ==============================================================================
# TAB 1: Storage Layer
# ==============================================================================
with tab1:
    st.subheader("HDFS Lake & Storage Overview")
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("#### 📁 Files in HDFS Data Lake (`hdfs_sim/`)")
        for f in hdfs.list_files():
            st.code(f"hdfs://localhost:9870/data/{f}", language="bash")
        st.dataframe(df_products, use_container_width=True)

    with col2:
        st.markdown("#### ⚡ Real-Time NoSQL Cache (`nosql_sim.json`)")
        metadata = nosql.get("metadata", "lake_summary")
        st.json(metadata or {"status": "Active"})
        st.markdown("#### Preview Ingested POS Sales Transactions (HDFS)")
        st.dataframe(df_transactions.head(10), use_container_width=True)

# ==============================================================================
# TAB 2: MapReduce
# ==============================================================================
with tab2:
    st.subheader("Distributed MapReduce Processing")
    st.markdown("""
    **Map Phase:** Emits `(product_id, (quantity, revenue))`  
    **Shuffle & Sort:** Groups emissions across partitions by key  
    **Reduce Phase:** Aggregates total demand and revenue per product
    """)

    df_demand = mapreduce_product_demand(df_transactions)
    df_joined_summary = pd.merge(df_demand, df_products[["product_id", "name", "category", "price"]], on="product_id")

    col_chart, col_table = st.columns([1, 1])
    with col_chart:
        st.markdown("#### Product Demand (Units Sold)")
        st.bar_chart(df_joined_summary.set_index("name")["units_sold"])

    with col_table:
        st.markdown("#### MapReduce Aggregated Output")
        st.dataframe(df_joined_summary[["product_id", "name", "category", "units_sold", "total_revenue", "order_count"]], use_container_width=True)

    with st.expander("🔍 View MapReduce Reduce-Side Join Sample"):
        df_joined = mapreduce_reduce_side_join(df_transactions.head(15), df_products)
        st.dataframe(df_joined, use_container_width=True)

# ==============================================================================
# TAB 3: Stream Mining
# ==============================================================================
with tab3:
    st.subheader("Mining Real-Time Data Streams")
    st.write("Stream algorithms trade a tiny known error margin for massive memory reduction.")

    c1, c2, c3 = st.columns(3)

    # 1. Bloom Filter
    with c1:
        st.markdown("### 🌸 Bloom Filter")
        st.caption("Check if a transaction ID was already seen (zero false negatives)")
        sample_ids = df_transactions["tx_id"].head(100).tolist()
        bf = BloomFilter(size=128, num_hashes=3)
        for tid in sample_ids:
            bf.add(tid)

        test_id = st.text_input("Test Transaction ID", value="TX_00005")
        if bf.contains(test_id):
            st.success(f"'{test_id}' is PROBABLY IN the stream (Bit match: Yes)")
        else:
            st.error(f"'{test_id}' is DEFINITIVELY NOT in the stream")
        st.caption(f"Bit array saturation: {sum(bf.bit_array)} / {bf.size} bits set")

    # 2. DGIM Algorithm
    with c2:
        st.markdown("### ⏱️ DGIM Algorithm")
        st.caption("Sliding-window count of surge sales (qty > 2) using log buckets")
        window_size = 50
        dgim = DGIM(window_size=window_size)
        stream_bits = [1 if q > 2 else 0 for q in df_transactions["quantity"].head(window_size)]
        for b in stream_bits:
            dgim.update(b)

        exact_ones = sum(stream_bits)
        approx_ones = dgim.count_ones()
        st.metric("Exact Surge Sales", exact_ones)
        st.metric("DGIM Estimated Surge Sales", approx_ones, delta=f"{approx_ones - exact_ones} error")
        st.caption(f"Active DGIM Buckets: {len(dgim.buckets)} (Sizes: {[b[0] for b in dgim.buckets]})")

    # 3. Flajolet-Martin
    with c3:
        st.markdown("### 🎲 Flajolet-Martin")
        st.caption("Distinct element estimation using trailing hash zeros")
        sample_stream = df_transactions["store_id"].tolist() + df_transactions["product_id"].tolist()
        exact_distinct = len(set(sample_stream))
        fm_distinct = flajolet_martin_distinct_count(sample_stream)

        st.metric("Exact Distinct Entities", exact_distinct)
        st.metric("Flajolet-Martin Estimate", fm_distinct, delta=f"{fm_distinct - exact_distinct} error")
        st.caption("Equation: Estimate = $2^R / 0.77351$")

# ==============================================================================
# TAB 4: Similarity & Clustering
# ==============================================================================
with tab4:
    st.subheader("Product Similarity & Market Segmentation")

    col_sim, col_clust = st.columns([1, 1])

    with col_sim:
        st.markdown("#### Jaccard Tag Similarity Matrix")
        df_sim = compute_product_similarities(df_products)
        st.dataframe(df_sim.set_index("product"), use_container_width=True)

    with col_clust:
        st.markdown("#### Market Clusters (Price vs Demand Volume)")
        df_clustered = cluster_products_by_market(df_products, df_transactions)
        st.scatter_chart(df_clustered, x="price", y="quantity", color="segment")
        st.dataframe(df_clustered[["name", "category", "price", "quantity", "segment"]], use_container_width=True)

# ==============================================================================
# TAB 5: Forecasting
# ==============================================================================
with tab5:
    st.subheader("Time-Series Demand Forecasting")

    prod_names = {row["product_id"]: row["name"] for _, row in df_products.iterrows()}
    selected_prod = st.selectbox(
        "Select Product to Forecast",
        options=list(prod_names.keys()),
        format_func=lambda pid: f"{pid} - {prod_names[pid]}"
    )
    forecast_days = st.slider("Forecast Horizon (Days)", min_value=3, max_value=14, value=7)

    daily_hist, df_forecast = forecast_product_demand(df_transactions, product_id=selected_prod, forecast_days=forecast_days)

    if not daily_hist.empty:
        # Combine historical and forecasted for interactive charting
        chart_data = daily_hist[["date", "quantity", "moving_avg_7d"]].rename(columns={
            "quantity": "Actual Daily Demand",
            "moving_avg_7d": "7-Day Moving Avg"
        }).set_index("date")

        st.line_chart(chart_data)

        st.markdown(f"#### 🔮 Next {forecast_days}-Day Projected Demand")
        cols = st.columns(len(df_forecast))
        for i, (_, row) in enumerate(df_forecast.iterrows()):
            with cols[i]:
                date_str = row["date"].strftime("%b %d")
                st.metric(date_str, f"{row['forecasted_demand']} units")

        # Save latest forecast into NoSQL store
        nosql.save("forecasts", selected_prod, {
            "product_id": selected_prod,
            "forecast_generated_at": pd.Timestamp.now().isoformat(),
            "predictions": df_forecast.to_dict(orient="records")
        })
        st.caption("Forecast successfully cached in NoSQL store for API serving.")
    else:
        st.warning("Insufficient transaction history for selected product.")
