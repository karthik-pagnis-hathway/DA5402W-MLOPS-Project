"""
spark_etl.py — Batch ETL: parquet topic_dumps → flat events CSV
Reads user-events parquet written by spark-consumer; writes a CSV
consumed by feature.py and feature_churn.py.
Falls back gracefully if no parquet data exists yet.
"""
import argparse
import sys

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default="/opt/airflow/DB/topic_dumps/user_events")
    parser.add_argument("--output-csv", default="/opt/airflow/DB/events_from_kafka.csv")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("BatchETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(args.input_path)
    except Exception as e:
        print(f"[ETL] No parquet data at {args.input_path} ({e}). Skipping.")
        spark.stop()
        sys.exit(0)

    keep = [c for c in [
        "user_id", "visitor_id", "session_id", "timestamp",
        "event_type", "product_id", "is_recommended", "order_id_for_refund",
    ] if c in df.columns]

    row_count = df.count()
    df.select(keep).toPandas().to_csv(args.output_csv, index=False)
    print(f"[ETL] Wrote {row_count} events → {args.output_csv}")
    spark.stop()


if __name__ == "__main__":
    main()
