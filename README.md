# Market Demand Forecasting System (Big Data Analytics)

A beginner-friendly mini-project for undergraduate Big Data Analytics students.  
Runs completely in **Python with zero frontend code** using Streamlit.

## Quick Start (1 Command)
```bash
streamlit run app.py
```
Open `http://localhost:8501` in your browser.

## File Overview (Only 3 Code Files)
1. `storage_and_data.py`:
   - Simulates the HDFS storage tier (`hdfs_sim/` with NameNode/DataNode concepts) and NoSQL store (`nosql_sim.json`).
   - Generates synthetic retail catalog and POS sales transactions.
2. `bigdata_algorithms.py`:
   - MapReduce (WordCount demand counter & Reduce-side join).
   - Stream Mining (Bloom Filter, DGIM sliding-window counter, Flajolet-Martin distinct estimation).
   - Similarity & Clustering (Jaccard similarity matrix & K-Means market segmentation).
   - Demand Forecasting (7-day Moving Average & Trend Regression).
3. `app.py`:
   - Interactive Streamlit dashboard with tabs for Storage, MapReduce, Streaming, Clustering, and Forecasting.
