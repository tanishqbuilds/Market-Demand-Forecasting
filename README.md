# Market Demand Forecasting System

A clean, enterprise-grade educational project for undergraduate **Big Data Analytics (BDA)** courses. Demonstrates core distributed data storage, parallel processing, sub-linear streaming algorithms, high-dimensional clustering, and time-series demand forecasting with **zero frontend code** using Streamlit.

## Quick Start (1 Command)

```bash
cd market-demand-forecasting
streamlit run app.py
```
Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Big Data Analytics (BDA) Concept Mapping

### 1. Storage & Distributed Topology (`storage_and_data.py`)
- **HDFS Lake Simulation (`hdfs_sim/`)**:
  - **NameNode**: Maintains in-memory metadata (`fsimage`) mapping file paths to Block IDs.
  - **DataNode**: Stores physical data chunks partitioned into standard **128 MB logical blocks**.
  - **Rack Awareness Policy**: Enforces standard 3x replication (2 replicas on local Rack 1, 1 replica on remote Rack 2 across the core switch for cross-rack fault isolation).
- **NoSQL Serving Tier (`nosql_sim.json`)**: Document store for low-latency queries and cached analytical forecasts.

### 2. Distributed MapReduce Execution Engine (`bigdata_algorithms.py`)
- **Stage 1 (Input Split & Map)**: Mappers parse raw CSV blocks and emit intermediate key-value pairs:
  $$\text{Map}: (K_1, V_1) \to \text{list}(K_2, V_2) = (product\_id, (quantity, revenue))$$
- **Stage 2 (Partitioning & Shuffle/Sort)**: Hash partitioner ($\text{hash}(key) \pmod{\text{NumReducers}}$) collates keys across worker nodes.
- **Stage 3 (Reduce)**: Aggregates total demand and revenue per SKU.
- **Reduce-Side Join**: Tags heterogeneous datasets (`'P'` for catalog metadata and `'T'` for POS transactions) and joins them on the Reducer.

### 3. Mining Data Streams
- **Bloom Filter (Membership Testing)**:
  - Sub-linear bit array ($m$ bits) with $k$ independent hash functions.
  - Guarantees **zero false negatives** with theoretical false positive bound:
    $$p \approx \left(1 - e^{-kn/m}\right)^k$$
- **DGIM Algorithm (Sliding Window Model)**:
  - Tracks surge purchases in a sliding window of size $N$ using only $O(\log^2 N)$ bits instead of $O(N)$.
  - Maintains at most 2 buckets of any power-of-2 size ($2^j$), guaranteeing maximum error $\le 50\%$.
- **Flajolet-Martin Algorithm (Distinct Element Counting)**:
  - Estimates unique customers/stores in unbounded streams using maximum trailing zeros $R$ of 256-bit hashes:
    $$\text{Distinct Estimate} = \frac{2^R}{\phi}, \quad \phi \approx 0.77351$$

### 4. High-Dimensional Similarity & CURE Clustering
- **Jaccard Tag Similarity**:
  $$J(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
- **CURE Clustering (Clustering Using Representatives)**:
  - Overcomes standard K-Means spherical centroid limitations by picking multiple well-scattered points per cluster and shrinking them toward the centroid by factor $\alpha = 0.2$.

### 5. Time-Series Demand Forecasting
- Evaluates 7-day rolling moving averages and Ordinary Least Squares (OLS) linear trend projections.
- Reports Mean Absolute Error (MAE) and caches model outputs in the NoSQL serving tier.

---

## File Structure

```
market-demand-forecasting/
├── storage_and_data.py      # HDFS lake & NoSQL storage adapters + synthetic POS ingestion
├── bigdata_algorithms.py    # MapReduce, Stream Mining, CURE/K-Means, and Forecasting
├── app.py                   # Clean enterprise data engineering dashboard
├── requirements.txt         # streamlit, pandas, numpy, scikit-learn
└── README.md
```
