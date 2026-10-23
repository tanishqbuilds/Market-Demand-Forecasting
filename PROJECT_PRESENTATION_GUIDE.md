# 🎓 Comprehensive Project Presentation & Technical Architecture Guide
## Market Demand Forecasting & Big Data Analytics Workbench

---

## 📌 Executive Summary

The **Market Demand Forecasting & Big Data Analytics Workbench** is an enterprise-grade Big Data platform and decision intelligence system. Built on the historical **UCI Online Retail Dataset** (541k+ raw transactions, 477k+ clean transaction lines, 2,868 catalog products, £9.37M+ recorded sales), this project demonstrates two complementary domains:

1. **Theoretical Big Data Analytics (BDA) Engines:** Interactive, algorithmically faithful demonstrations of distributed paradigms (Simulated HDFS Topology, MapReduce Aggregation & Reduce-Side Join, Sub-linear Streaming Algorithms like Bloom Filter, DGIM, and Flajolet-Martin, Jaccard Similarity, and K-Means Clustering).
2. **Industrial End-to-End PySpark & HDFS Data Pipeline:** A production-grade Medallion Lakehouse architecture (Bronze $\to$ Silver $\to$ Gold Parquet layers) running on a distributed Apache Hadoop cluster with NameNode and dual DataNodes, executing Spark ML **Gradient-Boosted Trees (GBT)** demand forecasting with rigorous 30-day temporal holdout evaluation and real-time inventory Reorder Point (ROP) simulation.

---

## 🖥️ Every Visible Presentation Component (Tab-by-Tab Walkthrough)

This section maps out every screen, card, slider, chart, and metric that will appear during your live demo on **Streamlit (`http://localhost:8501`)**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ STREAMLIT TOP BAR: Market Demand Forecasting Engine (Enterprise Typography)             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ TABS: [0] Decision Dashboard | [1] Storage Concepts | [2] MapReduce | [3] Stream Mining│
│       [4] Similarity & Clustering | [5] Demand Forecasting | [6] End-to-End Pipeline   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 1. Global Sidebar (Always Visible on the Left)

* **Dataset Source Link:** Clickable reference to the official UCI Machine Learning Repository.
* **Core KPI Metrics:**
  * **Products:** `2,868` (Distinct catalog SKUs).
  * **Clean Sales Lines:** `477,096` (Valid non-cancelled, positive price/quantity transactions).
  * **Recorded Sales:** `£9,367,991.82` (Total gross merchandise value).
* **Speaker Tip:** Point to these numbers first to prove that the dashboard is backed by actual large-scale historical transaction data, not dummy mockups.

---

### 2. Tab [0]: Decision Dashboard (Executive Business Intelligence)

Designed with a **Tableau / Power BI executive aesthetic**, this tab delivers macroeconomic and SKU-level commercial intelligence.

#### A. Interactive Slicers & Filters Ribbon
1. **Market / Country Slicer:** Multi-select dropdown (e.g., United Kingdom, Germany, France, EIRE, Spain). Filters all underlying data in real-time.
2. **Calendar Period (Quarterly):** Select buttons (`All`, `Q1`, `Q2`, `Q3`, `Q4`) for seasonal isolation.
3. **Product Price Tier:** Multi-select for pricing segments (`Budget <£2`, `Mid £2-£10`, `Premium >£10`).
4. **Fine Date Range Picker:** Interactive dual-date picker restricting the transaction timeline.

#### B. Top KPI Scorecard Ribbon
* **Net Invoiced Revenue:** `£9,367,992` (Sum of `quantity * unit_price` over filtered transactions).
* **Physical Units Sold:** `5,131,133 units` (Total merchandise velocity).
* **Completed Invoices:** `19,549 orders` (Distinct transaction count).
* **Average Order Value (AOV):** `£479.21` ($\text{Revenue} / \text{Invoices}$).
* **Active Customer Base:** `4,314 unique clients` (Distinct buyer IDs).

#### C. Visual Charts & Analytical Grids
1. **Revenue Dynamics & Physical Demand Velocity (Dual-Axis Combi Chart):**
   * *Left Axis (Blue Bars):* Monthly Gross Invoiced Revenue (£).
   * *Right Axis (Orange Spline):* Monthly Physical Units Sold.
   * *Key Takeaway:* Demonstrates strong Q4 seasonality (surging in November leading up to Christmas holiday shopping).
2. **Top Market Penetration (% Revenue Share - Horizontal Bar Chart):**
   * Ranks international markets.
   * *Insight:* UK dominates with **84.9%** share, followed by Germany, France, and EIRE.
3. **SKU Demand-Price Positioning Matrix (BCG-Style Bubble Quad Chart):**
   * *X-Axis (Log Scale):* Median Observed Unit Price (£).
   * *Y-Axis (Log Scale):* Total Units Sold.
   * *Bubble Size & Color:* Total Revenue generated.
   * *Four Quadrants:*
     * *Volume Drivers:* Low price, high volume.
     * *Crown Jewels:* High price, high volume.
     * *Boutique / Niche:* High price, low volume.
     * *Long Tail:* Low price, low volume.
4. **Commercial Trading Velocity Matrix (Day-of-Week vs Month Heatmap):**
   * Heatmap showing transaction intensity.
   * *Insight:* Friday/Thursday mornings and autumn months represent peak commercial activity. Note: UCI dataset records zero Sunday sales (warehouse closed).
5. **Pareto 80/20 Cumulative Revenue Concentration Curve:**
   * Validates the classic Pareto Principle: the top ~18% of SKUs generate 80% of aggregate enterprise turnover.

---

### 3. Tab [1]: Storage Concepts (Simulation)

Demonstrates how modern distributed file systems store big data blocks with redundancy.

* **NameNode In-Memory File System Image (`fsimage`):**
  * Interactive table displaying simulated block mapping: `file_path`, `block_id` (e.g., `blk_1073741825`), `size_bytes`, `replication_factor` (3x), and `status: HEALTHY`.
* **Local JSON Results Cache (NoSQL Concept Demo):**
  * JSON viewer showing key-value document persistence (`nosql_sim.json`) storing data lake summaries and forecast artifacts.
* **Illustrative Rack-Aware Placement Topology:**
  * ASCII / text architecture diagram showing:
    * Client write RPC to NameNode.
    * Data pipeline streaming: $\text{DN1} \to \text{DN2} \to \text{DN3}$.
    * Cross-rack fault isolation (Rack 1: DN1 & DN2; Rack 2: DN3 across top-of-rack remote switch).
* **Raw Cleaned Records Preview Table:**
  * Sample rows showing `tx_id`, `date`, `country`, `product_id`, `quantity`, `unit_price`, and `total_revenue`.

---

### 4. Tab [2]: MapReduce Execution

Illustrates single-process step-by-step MapReduce execution over real retail records.

#### A. 3-Stage Demand Aggregation Pipeline
1. **Stage 1 (Input Splits & Map):**
   * Emits key-value pairs: $\text{Key } K_2 = \text{product\_id}$, $\text{Value } V_2 = \{\text{quantity}, \text{revenue}\}$.
   * Table shows sample mapper emissions assigned to reducers.
2. **Stage 2 (Shuffle & Sort):**
   * Deterministic partitioning using the formula:
     $$\text{Reducer ID} = \text{SHA-256}(\text{product\_id}) \pmod 3$$
   * Displays exact key grouping across `Reducer-0`, `Reducer-1`, and `Reducer-2`.
3. **Stage 3 (Reduce Phase):**
   * Folds key value lists into aggregated totals: $\sum \text{units\_sold}$, $\sum \text{total\_revenue}$.

#### B. MapReduce Reduce-Side Join Implementation
* Solves the relational join between high-volume **Sales Transactions** and the **Product Catalog**.
* *Mapper Tagging:*
  * Sales record emits: $(\text{product\_id}, (\text{'TX'}, \text{qty}, \text{rev}, \text{date}))$.
  * Catalog record emits: $(\text{product\_id}, (\text{'CATALOG'}, \text{product\_name}, \text{price}))$.
* *Reducer Merging:* Merges records sharing `product_id` to create enriched transaction lines without shared memory.

---

### 5. Tab [3]: Stream Mining Engine

Demonstrates **sub-linear space** algorithms designed for high-velocity streaming data where storing the entire stream in RAM is impossible.

#### A. Bloom Filter (Approximate Set Membership)
* **Parameters:** 64-bit array, 3 independent hash functions (`MD5`, `SHA-1`, `SHA-256`).
* **Interactive Control:** Text input to query any Invoice ID (`tx_id`).
* **Visible Outputs:**
  * Exact bit indices computed by $H_1, H_2, H_3$.
  * Probabilistic status badge: `PROBABLY PRESENT` vs `DEFINITIVELY NOT IN STREAM`.
  * Theoretical False Positive Probability:
    $$P(\text{FP}) \approx \left(1 - e^{-k \cdot n / m}\right)^k$$
  * Dynamic visual of the 64-bit bitmask state.
* **Key Theory:** Zero false negatives guaranteed.

#### B. DGIM Algorithm (Sliding Window Bit Counter)
* Counts the number of large purchases ($\text{quantity} \ge 3$) in the last $N$ stream elements using $O(\log^2 N)$ memory.
* **Interactive Slider:** Sliding window size $N$ (from 30 to 120 events).
* **Visible Outputs:**
  * **Exact 1s in Window** vs **DGIM Estimated 1s**.
  * Dynamic delta displaying error percentage (provably $\le 50\%$, typically $<10\%$).
  * Table of **Active Exponential Buckets**: displays bucket sizes ($2^0, 2^1, 2^2, \dots$), timestamps, and age decay.

#### C. Flajolet-Martin Algorithm (Distinct Cardinality Estimation)
* Estimates the number of unique Customer IDs in a 10,000-event stream without a hash set.
* **Visible Outputs:**
  * Exact distinct count vs FM Estimate.
  * Maximum trailing zeros observed: $R$.
  * Cardinality formula:
    $$\text{Distinct Estimate} = \frac{2^R}{\phi}, \quad \text{where } \phi \approx 0.77351$$
  * Bit hash suffix trace table showing binary representations and zero-counts.

---

### 6. Tab [4]: Similarity & Clustering

#### A. Jaccard Similarity Matrix
* Measures lexical similarity across the top 30 best-selling product titles:
  $$J(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
* Interactive correlation heatmap: highlights product variations sharing keywords (e.g., color variants, size variants).

#### B. K-Means Product Segmentation
* Groups products across two standardized dimensions: **Observed Median Price (£)** and **Total Units Sold**.
* **Visible Scatter Chart:**
  * Color-coded cluster assignments:
    1. *High-Volume Fast-Movers (Budget/Impulse).*
    2. *Mid-Tier Steady Performers.*
    3. *High-Margin Premium SKUs.*
* **Centroid Table:** Displays coordinates and cluster representatives.

---

### 7. Tab [5]: Demand Forecasting (Single-SKU Walkthrough)

* **SKU Dropdown:** Select from all products with $\ge 30$ active transaction days (sorted by volume).
* **Forecast Horizon Slider:** 3 to 14 days ahead.
* **Visible Visuals:**
  * **Time Series Line Chart:** Actual Daily Demand vs 7-Day Rolling Moving Average.
  * **Holdout Validation Scorecards:** MAE on validation days, Trend Growth Slope (units/day), Historical Days Sampled.
  * **Projected Metrics Grid:** Daily forecasted demand units for upcoming days.
  * **NoSQL Auto-Save:** Caches the output directly into `nosql_sim.json`.

---

### 8. Tab [6]: End-to-End PySpark + HDFS Pipeline

The centerpiece of your architecture defense. Renders the complete results of the distributed PySpark ML job.

#### A. Architecture Workflow Ribbon (5 Step Cards)
1. **Ingestion:** `477,096 Rows` (Raw UCI Retail feed).
2. **Spark Hygiene:** `472,251 Valid` (Filtering cancellations, non-numeric SKUs, negative quantities/prices).
3. **Feature Matrix:** `1,072,632 Vectors` (Time-series lag features $t-1, t-7, t-14, t-21, t-28$ & rolling windows).
4. **30-Day Holdout:** `88,908 Product-Days Validated` (Strict temporal out-of-time evaluation).
5. **Production Lake:** Distributed Parquet partitions saved to HDFS.

#### B. Metric Switcher & KPI Scorecard
* **Radio Toggle:** Switch between **Mean Absolute Error (MAE)** and **Root Mean Squared Error (RMSE)**.
* **4 Comparative Scorecards:**
  * **Machine Learning Model (GBT):** `8.97 units` MAE (*Best Overall*).
  * **Recent 7-Day Moving Average Baseline:** `9.43 units` MAE (+5.1% error vs ML).
  * **4-Week Same-Weekday Seasonal Baseline:** `9.28 units` MAE (+3.5% error vs ML).
  * **Validation Coverage:** `88,908 rows` across 30 full calendar days.

#### C. Validation Errors Segmented by Daily Demand Band
* Grouped bar chart comparing ML model vs baselines across 5 volume tiers:
  * `0 units`, `1-5 units`, `6-20 units`, `21-100 units`, and `Over 100 units`.
* *Critical Observation:* In low/medium tiers, the GBT model dominates; in extreme spiky burst tiers (>100 units), moving averages track volatility closely.

#### D. 7-Day Demand Forecast vs Baseline Comparator
* Top N product selector ($10, 15, 25, 40$).
* Displays grouped bars for **ML Model 7-Day Projection** vs **Recent 7-Day Baseline**, with a dotted green spline representing the **Safety Demand Buffer**.

#### E. Interactive Safety Stock & Reorder Point (ROP) Simulator
* **Interactive Controls:**
  * SKU Selector (Top 100 volume products).
  * **Lead Time Slider:** 1 to 28 days (default: 7 days).
  * **Safety Stock Multiplier ($\alpha$):** 0.5 to 2.5 (service level factor).
* **Live Calculated Thresholds:**
  $$\text{ROP Trigger} = (\text{Daily Demand} \times \text{Lead Time}) + (\text{Safety Buffer} \times \alpha)$$
* Displays exact integer triggers for warehouse procurement reordering.

#### F. Top 20 Held-Out Products Evaluation Table
* Real-time Streamlit data table with progress bars showing actual demand, ML predictions, baseline predictions, and SKU-level MAE.

#### G. Distributed Lake & HDFS Partition Storage Specs (Expander)
* Confirms HDFS cluster root: `hdfs://namenode:9000/market-demand`.
* Displays bronze, silver, and gold Parquet partition paths.

---

## 🏗️ Deep-Dive Codebase & File Architecture

```
market-demand-forecasting/
├── app.py                     # Streamlit orchestration & presentation entrypoint
├── dashboard_ui.py            # Tableau/Power BI Plotly layout engine & UI renderers
├── bigdata_algorithms.py      # BDA core algorithms (MapReduce, Streams, Clustering)
├── pipeline_runner.py         # PySpark ML pipeline, HDFS Parquet writer, GBT model
├── storage_and_data.py        # UCI dataset downloader, cleaner, HDFS/NoSQL simulators
├── pipeline_outputs/          # JSON & CSV pipeline results (for dashboard consumption)
├── hdfs_sim/                  # Local normalized CSVs and simulated HDFS blocks
├── docker-compose.yml         # Container cluster orchestration (Hadoop + App)
├── Dockerfile                 # Multi-stage image (Python 3.10 + PySpark + Streamlit)
├── docker/hadoop/             # Hadoop NameNode/DataNode entrypoint & XML configs
└── requirements.txt           # Python dependencies (Streamlit, Plotly, PySpark, etc.)
```

### 1. `app.py`
* **Role:** Orchestrates the multi-tab layout, initializes storage adapters, checks dataset integrity, and injects clean CSS.
* **Key Functions:**
  * `inject_theme_css(is_dark=False)`: Enforces crisp typography, custom tab borders, and hides default Streamlit headers.
  * Initializes `HDFSAdapter` and `NoSQLStore`.
  * Calls `load_online_retail_dataset()` to guarantee data presence.
  * Dispatches rendering across `st.tabs()`.

### 2. `dashboard_ui.py`
* **Role:** High-performance visualization engine generating Plotly charts with consistent color tokens (`THEME_CONFIG`), responsive card grids, and interactive filtering.
* **Key Functions:**
  * `render_decision_dashboard()`: Implements slicing, KPI calculation, dual-axis combi charts, BCG scatter plots, and heatmaps.
  * `render_pipeline_dashboard()`: Implements the 5-step pipeline architecture ribbon, ML vs baseline error comparator, ROP simulator, and Top 20 validation table.
  * `build_plotly_layout()`: Standardizes margins, fonts, hover labels, and legends across all Plotly figures.

### 3. `bigdata_algorithms.py`
* **Role:** Contains standalone, algorithmically pure implementations of distributed and stream data mining algorithms.
* **Algorithms Implemented:**
  1. `mapreduce_product_demand_trace()`: Emulates Map, Shuffle/Sort, and Reduce steps using SHA-256 partition hashing.
  2. `mapreduce_reduce_side_join_trace()`: Demonstrates how two un-joined distributed files (transactions and catalog) are joined on reducer nodes without shared state.
  3. `BloomFilter`: Bit array with multiple hash functions, tracking occupancy and false positive rates.
  4. `DGIM`: Maintains exponential buckets whose sizes are powers of 2. When more than two buckets share the same size, the oldest two merge into a bucket of double the size ($2^{j+1}$).
  5. `flajolet_martin_trace()`: Hashes incoming stream values to bit strings, determines the maximum trailing zero count $R$, and applies the Flajolet-Martin stochastic averaging formula.
  6. `compute_jaccard_matrix()`: Tokenizes product titles into n-grams/words and computes pair-wise Jaccard set overlap.
  7. `cluster_products_kmeans()`: Scikit-learn K-Means implementation on standardized price and sales volume.
  8. `forecast_product_demand()`: Time-series feature extraction, 7-day rolling mean, and Ordinary Least Squares trend extrapolation.

### 4. `storage_and_data.py`
* **Role:** Source data loader and distributed storage abstraction layer.
* **Key Capabilities:**
  * Automatic download of the official UCI Online Retail Excel workbook (`.xlsx`) via urllib.
  * Rigorous data hygiene: filters cancellations (`InvoiceNo` starting with 'C'), non-positive quantities ($Q \le 0$), negative unit prices ($P \le 0$), and non-product line items (e.g., POSTAGE, BANK CHARGES).
  * `HDFSAdapter`: Simulates chunking files into 64 MB / 128 MB blocks, assigns 64-bit Block IDs, and generates simulated DataNode rack allocations.
  * `NoSQLStore`: File-backed key-value JSON store providing `get()` and `save()` operations for downstream caching.

### 5. `pipeline_runner.py`
* **Role:** The industrial PySpark engine.
* **Execution Flow:**
  1. Spawns a local or cluster `SparkSession` with Apache Hadoop HDFS connectors.
  2. Loads raw data via an explicit, typed `StructType` schema.
  3. Partitions data into **Bronze** (cleaned transaction lines).
  4. Aggregates daily demand per product and country into **Silver** (`daily_product_market`).
  5. Assembles high-dimensional feature vectors in **Gold** (`daily_product_features`) using PySpark Window specifications:
     * Lags: `lag_1`, `lag_7`, `lag_14`, `lag_21`, `lag_28`.
     * Rolling averages: `rolling_mean_7d`, `rolling_mean_28d`.
     * Calendar seasonality: `weekday_4week_mean`, `weekday`, `month`.
     * Pricing: `median_unit_price`.
  6. Trains a **Gradient-Boosted Trees (GBT) Regressor** via `pyspark.ml.Pipeline`.
  7. Evaluates predictions on a strict **30-day temporal holdout** (Nov 10 – Dec 09, 2011).
  8. Computes MAE, RMSE, and error by demand band, writing results to `pipeline_outputs/` and HDFS Parquet.

### 6. Distributed Docker & Hadoop Cluster Architecture
* **`docker-compose.yml`**: Defines a 4-container cluster:
  * `namenode`: Apache Hadoop 3.3 NameNode (Port 9000 for HDFS RPC, Port 9870 for Web UI).
  * `datanode1` & `datanode2`: Real distributed storage daemons with block replication factor 2.
  * `app`: Python 3.10 container with Java 17, PySpark, and Streamlit.
* Ensures fault-tolerant, persistent storage using Docker named volumes (`hadoop_namenode`, `hadoop_datanode1`, `hadoop_datanode2`).

---

## 🎤 Presentation Master Script (5-Minute Winning Pitch)

* **Minute 1 — The Hook & Business Context (Tab 0):**
  > "Good morning. In modern commerce, stockouts lead to lost revenue, while overstocking ties up working capital. Our project solves this through a Big Data Analytics Workbench and an End-to-End Market Demand Forecasting Pipeline built on 540,000+ real retail transactions from the UCI repository. In Tab 0, our Executive Decision Dashboard provides instant commercial visibility: tracking £9.37M in sales, identifying seasonal surges in Q4, and highlighting that the top 18% of products drive 80% of enterprise revenue."

* **Minute 2 — Theoretical Big Data Architecture (Tabs 1 & 2):**
  > "To process this volume at scale, we implemented foundational distributed computing patterns. In Tab 1, we demonstrate HDFS block chunking, 64-bit block IDs, and rack-aware replication across isolated network racks. In Tab 2, we trace a complete MapReduce execution: mappers emit key-value pairs, a SHA-256 partition function routes keys across reducers during the shuffle-and-sort phase, and a Reduce-Side Join seamlessly links high-velocity transaction streams with product catalogs without shared memory."

* **Minute 3 — High-Velocity Stream Mining (Tab 3):**
  > "When data arrives as continuous live streams, storing everything in RAM is impossible. Tab 3 showcases sub-linear space streaming algorithms: Bloom Filters for zero-false-negative invoice membership queries, the DGIM algorithm for counting high-quantity purchases within sliding windows using logarithmic $O(\log^2 N)$ memory, and Flajolet-Martin stochastic averaging for estimating distinct customer counts using hash trailing zeros."

* **Minute 4 — PySpark ML & Medallion Data Lake (Tab 6):**
  > "Moving from simulation to industrial data engineering, Tab 6 showcases our production PySpark pipeline. We process transactions through a Medallion architecture—Bronze clean records, Silver daily country aggregates, and Gold feature stores. Using Spark Window functions, we extract 11 lag and seasonal features to train a Gradient-Boosted Trees regressor evaluated against a rigorous 30-day holdout of 88,900+ product-days."

* **Minute 5 — Evaluation & Operational Inventory Signals (Tab 6):**
  > "Our ML model achieves an overall MAE of 8.97 units, outperforming moving-average and seasonal baselines. Finally, we translate forecast predictions into operational decisions through our Interactive Reorder Point Simulator—dynamically calculating safety stock buffers and procurement thresholds as supplier lead times vary. Thank you, and we welcome your questions."

---

## 🛡️ Anticipated Defense Questions & Model Answers

### Q1: Why did you use Gradient-Boosted Trees (GBT) instead of ARIMA or LSTM?
**Model Answer:**
> "ARIMA requires continuous, non-zero univariate time series and struggles when scaling to thousands of individual SKUs simultaneously. LSTMs have substantial computational overhead and are prone to overfitting on sparse retail demand. PySpark's GBT Regressor natively handles non-linear feature interactions (such as the interaction between weekday seasonality and price elasticity), scales efficiently across distributed clusters, and trains robust models over millions of feature rows."

### Q2: Why is the model's RMSE (~273 units) so much higher than its MAE (~8.97 units)?
**Model Answer:**
> "MAE calculates the linear average absolute error, giving an intuitive measure of typical daily miss (approx. 9 units). RMSE squares errors before averaging, making it heavily sensitive to extreme outlier bursts. In retail, occasional wholesale orders (e.g., a customer suddenly purchasing 2,000 units of a novelty item) create massive squared penalties. Our demand band breakdown proves that for 95% of daily transactions (<20 units), the error is very low, but wholesale spike events pull the aggregate RMSE upward."

### Q3: How does your DGIM implementation guarantee an error bounded within 50%?
**Model Answer:**
> "DGIM represents a sliding window of size $N$ using exponential buckets whose sizes are powers of 2 ($1, 2, 4, 8, \dots$). By maintaining the rule that at most two buckets can share the same size, memory is constrained to $O(\log^2 N)$. When an estimate is requested, we sum all fully enclosed buckets and add half the size of the oldest bucket that overlaps the window boundary. Because the oldest bucket has size $2^k$ and the sum of all preceding buckets is at least $2^k - 1$, the maximum possible error cannot exceed $2^{k-1} / \text{Total}$, which is mathematically proven to be $\le 50\%$."

### Q4: Why is the HDFS replication factor set to 2 in your cluster instead of the Hadoop default of 3?
**Model Answer:**
> "In enterprise production data centers with dozens of racks, a replication factor of 3 is standard (2 replicas on the local rack, 1 on a remote rack). In our containerized local environment, Docker runs two DataNode daemons on a single physical host. A replication factor of 2 provides authentic distributed block streaming and pipeline replication between distinct network containers without incurring unnecessary local disk and I/O overhead."

### Q5: How did you prevent data leakage during model training and evaluation?
**Model Answer:**
> "We enforced strict **temporal out-of-time validation**. Rather than using random train-test splitting (which causes future data to leak into past predictions), we trained the model exclusively on historical transactions up to November 09, 2011, and evaluated performance on the subsequent 30 calendar days (November 10 to December 09, 2011). Furthermore, all lag features ($t-1, t-7, t-28$) were computed strictly using preceding dates."

### Q6: What is the Medallion Architecture and how is it reflected in your HDFS storage?
**Model Answer:**
> "Medallion Architecture organizes lakehouse data into quality tiers:
> - **Bronze (`/market-demand/bronze`):** Raw, deduplicated, and schema-validated transaction lines.
> - **Silver (`/market-demand/silver`):** Cleaned, daily product-market aggregated metrics.
> - **Gold (`/market-demand/gold`):** Business-level feature matrices and ML forecast outputs stored as snappy-compressed Parquet files ready for BI consumption."

---

*Guide generated for the Market Demand Forecasting & Big Data Analytics Workbench presentation.*
