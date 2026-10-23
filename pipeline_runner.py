"""Run the UCI sales ETL, feature engineering, forecast evaluation, and HDFS writes."""

import json
import os
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from storage_and_data import DATA_DIR, load_online_retail_dataset


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "pipeline_outputs"
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://namenode:9000/market-demand")
FEATURE_COLUMNS = [
    "lag_1",
    "lag_7",
    "lag_28",
    "rolling_mean_7d",
    "rolling_mean_28d",
    "weekday",
    "month",
    "median_unit_price",
]


def _sales_schema():
    return StructType([
        StructField("tx_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_revenue", DoubleType(), True),
        StructField("customer_id", StringType(), True),
    ])


def _write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _wait_for_hdfs(spark, attempts=18):
    import time

    error = None
    filesystem = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    probe = spark._jvm.org.apache.hadoop.fs.Path(f"{HDFS_ROOT}/_pipeline_readiness_probe")
    for _ in range(attempts):
        try:
            # A successful create/delete confirms the NameNode has left safe
            # mode and Spark can actually write the output datasets.
            filesystem.create(probe, True).close()
            filesystem.delete(probe, False)
            return
        except Exception as exc:  # HDFS can take a few seconds to leave safe mode.
            error = exc
            time.sleep(5)
    raise RuntimeError(
        f"Could not connect to HDFS at {HDFS_ROOT}. Check that Docker Compose "
        "started the NameNode and both DataNodes."
    ) from error


def run_pipeline():
    """Execute the local educational pipeline and persist its lake outputs in HDFS."""
    products, _, metadata = load_online_retail_dataset()
    csv_path = DATA_DIR / "sales_transactions.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Expected normalized UCI sales file at {csv_path}")

    spark = (
        SparkSession.builder.appName("UCI-Market-Demand-Pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT.split("/market-demand")[0])
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        _wait_for_hdfs(spark)
        source = (
            spark.read.schema(_sales_schema()).option("header", True).csv(csv_path.resolve().as_uri())
            .withColumn("date", F.to_date("date"))
            .withColumn("quantity", F.col("quantity").cast("double"))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("total_revenue", F.col("total_revenue").cast("double"))
        )
        required = {"tx_id", "date", "country", "product_id", "quantity", "unit_price", "total_revenue"}
        missing = required.difference(source.columns)
        if missing:
            raise ValueError(f"UCI sales input is missing required fields: {sorted(missing)}")

        cleaned = (
            source.dropna(subset=list(required))
            .filter(
                (F.col("quantity") > 0)
                & (F.col("unit_price") > 0)
                & (F.col("total_revenue") > 0)
                & (F.length(F.trim("product_id")) > 0)
                & (F.length(F.trim("country")) > 0)
            )
            .dropDuplicates()
        )
        source_rows = source.count()
        cleaned_rows = cleaned.count()

        product_lookup = spark.createDataFrame(
            products[["product_id", "name", "price"]].astype({"product_id": str, "name": str}),
        ).select(
            F.col("product_id").cast("string"),
            F.col("name").alias("product_name"),
            F.col("price").cast("double").alias("median_unit_price"),
        )

        # Bronze: normalized line-level input, read and validated again by Spark.
        bronze_path = f"{HDFS_ROOT}/bronze/sales_transactions"
        cleaned.write.mode("overwrite").parquet(bronze_path)

        # Silver: daily demand at product-market grain, plus product-level demand
        # series used for forecasting. Country is a market proxy, not a store ID.
        daily_market = (
            cleaned.groupBy("product_id", "country", "date")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.sum("total_revenue").alias("revenue"),
                F.avg("unit_price").alias("avg_unit_price"),
                F.countDistinct("tx_id").alias("invoice_count"),
            )
        )
        daily_market_path = f"{HDFS_ROOT}/silver/daily_product_market"
        daily_market.write.mode("overwrite").parquet(daily_market_path)

        daily_product = (
            daily_market.groupBy("product_id", "date")
            .agg(
                F.sum("units_sold").alias("units_sold"),
                F.sum("revenue").alias("revenue"),
            )
            .join(F.broadcast(product_lookup), "product_id", "left")
        )
        date_bounds = daily_product.agg(F.min("date").alias("min_date"), F.max("date").alias("max_date")).first()
        calendar = spark.sql(
            "SELECT explode(sequence(to_date('{}'), to_date('{}'), interval 1 day)) AS date".format(
                date_bounds["min_date"], date_bounds["max_date"]
            )
        )
        product_ids = daily_product.select("product_id", "product_name", "median_unit_price").distinct()
        calendar_demand = (
            product_ids.crossJoin(calendar)
            .join(daily_product.select("product_id", "date", "units_sold", "revenue"), ["product_id", "date"], "left")
            .fillna({"units_sold": 0.0, "revenue": 0.0})
        )

        history = Window.partitionBy("product_id").orderBy("date")
        previous_7 = history.rowsBetween(-7, -1)
        previous_28 = history.rowsBetween(-28, -1)
        feature_rows = (
            calendar_demand
            .withColumn("lag_1", F.lag("units_sold", 1).over(history))
            .withColumn("lag_7", F.lag("units_sold", 7).over(history))
            .withColumn("lag_28", F.lag("units_sold", 28).over(history))
            .withColumn("rolling_mean_7d", F.avg("units_sold").over(previous_7))
            .withColumn("rolling_mean_28d", F.avg("units_sold").over(previous_28))
            .withColumn("weekday", F.dayofweek("date").cast("double"))
            .withColumn("month", F.month("date").cast("double"))
            .withColumn("median_unit_price", F.coalesce("median_unit_price", F.lit(0.0)))
        )
        feature_path = f"{HDFS_ROOT}/gold/daily_product_features"
        feature_rows.write.mode("overwrite").partitionBy("month").parquet(feature_path)

        model_rows = feature_rows.dropna(subset=FEATURE_COLUMNS + ["units_sold"])
        latest_date = date_bounds["max_date"]
        validation_start = spark.sql(
            "SELECT date_sub(to_date('{}'), 30) AS d".format(latest_date)
        ).first()["d"]
        train = model_rows.filter(F.col("date") < F.lit(validation_start))
        validation = model_rows.filter(F.col("date") >= F.lit(validation_start))
        if train.limit(1).count() == 0 or validation.limit(1).count() == 0:
            raise ValueError("Not enough history for a 30-day time-based validation split.")

        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="skip")
        regressor = GBTRegressor(
            featuresCol="features", labelCol="units_sold", predictionCol="ml_prediction",
            maxIter=20, maxDepth=4, seed=42,
        )
        model_pipeline = Pipeline(stages=[assembler, regressor])
        model = model_pipeline.fit(train)
        validation_predictions = model.transform(validation)
        validation_predictions = validation_predictions.withColumn(
            "baseline_prediction", F.col("rolling_mean_7d")
        )
        evaluation = validation_predictions.agg(
            F.count("units_sold").alias("validation_rows"),
            F.avg(F.abs(F.col("units_sold") - F.col("ml_prediction"))).alias("ml_mae"),
            F.sqrt(F.avg(F.pow(F.col("units_sold") - F.col("ml_prediction"), 2))).alias("ml_rmse"),
            F.avg(F.abs(F.col("units_sold") - F.col("baseline_prediction"))).alias("baseline_mae"),
            F.sqrt(F.avg(F.pow(F.col("units_sold") - F.col("baseline_prediction"), 2))).alias("baseline_rmse"),
        ).first().asDict()

        # Prepare features for the day after the final observation. The 7-day
        # projection holds this next-day estimate flat, so it is an educational
        # baseline horizon rather than a recursively simulated daily forecast.
        next_7 = history.rowsBetween(-6, 0)
        next_28 = history.rowsBetween(-27, 0)
        forecast_inputs = (
            calendar_demand
            .withColumn("next_lag_7", F.lag("units_sold", 6).over(history))
            .withColumn("next_lag_28", F.lag("units_sold", 27).over(history))
            .withColumn("next_rolling_mean_7d", F.avg("units_sold").over(next_7))
            .withColumn("next_rolling_mean_28d", F.avg("units_sold").over(next_28))
            .filter(F.col("date") == F.lit(latest_date))
            .select(
                "product_id", "product_name", "date", "units_sold", "median_unit_price",
                F.col("units_sold").alias("lag_1"),
                F.col("next_lag_7").alias("lag_7"),
                F.col("next_lag_28").alias("lag_28"),
                F.col("next_rolling_mean_7d").alias("rolling_mean_7d"),
                F.col("next_rolling_mean_28d").alias("rolling_mean_28d"),
                F.dayofweek(F.date_add("date", 1)).cast("double").alias("weekday"),
                F.month(F.date_add("date", 1)).cast("double").alias("month"),
            )
            .dropna(subset=FEATURE_COLUMNS)
        )
        forecasts = model.transform(forecast_inputs).select(
            "product_id", "product_name", "date", "units_sold", "rolling_mean_7d",
            "ml_prediction", "median_unit_price",
        )
        forecasts = (
            forecasts
            .withColumn("forecast_next_day_units", F.greatest(F.lit(0.0), F.col("ml_prediction")))
            .withColumn("forecast_next_7d_units", F.ceil(F.col("forecast_next_day_units") * 7))
            .withColumn("baseline_next_7d_units", F.ceil(F.col("rolling_mean_7d") * 7))
            .withColumn("assumed_lead_time_days", F.lit(7))
            .withColumn("demand_buffer_units", F.ceil(F.lit(1.65) * F.sqrt(F.lit(7.0)) * F.sqrt(F.col("rolling_mean_7d"))))
            .withColumn("reorder_point_proxy_units", F.col("forecast_next_7d_units") + F.col("demand_buffer_units"))
            .withColumn("inventory_data_available", F.lit(False))
            .drop("ml_prediction")
        )
        forecast_path = f"{HDFS_ROOT}/gold/product_demand_forecasts"
        forecasts.write.mode("overwrite").parquet(forecast_path)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        forecasts.orderBy(F.desc("forecast_next_7d_units")).toPandas().to_csv(
            OUTPUT_DIR / "product_forecasts.csv", index=False
        )
        evaluation_payload = {
            "dataset": metadata["dataset"],
            "source_start_date": metadata["start_date"],
            "source_end_date": metadata["end_date"],
            "source_rows_read_by_spark": source_rows,
            "clean_rows_after_spark": cleaned_rows,
            "daily_product_market_rows": daily_market.count(),
            "feature_rows": feature_rows.count(),
            "training_rows": train.count(),
            "validation_start": str(validation_start),
            "validation_end": str(latest_date),
            "evaluation": {key: float(value) if value is not None else None for key, value in evaluation.items()},
            "hdfs_root": HDFS_ROOT,
            "outputs": {
                "bronze_transactions": bronze_path,
                "silver_daily_product_market": daily_market_path,
                "gold_daily_features": feature_path,
                "gold_product_forecasts": forecast_path,
            },
            "inventory_note": "No inventory, store, or supplier data exists in the UCI dataset. Reorder point is a demand-only proxy with an assumed 7-day lead time, not an order recommendation.",
        }
        _write_json(OUTPUT_DIR / "pipeline_summary.json", evaluation_payload)
        print(json.dumps(evaluation_payload, indent=2, default=str))
        return evaluation_payload
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
