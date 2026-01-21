import argparse
from pyspark.sql import SparkSession

def parse_args():
    p = argparse.ArgumentParser(
        description="Iceberg schema evolution - add column"
    )
    p.add_argument("--catalog", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--column-name", required=True)
    p.add_argument("--column-type", required=True)
    p.add_argument("--comment", default=None)
    return p.parse_args()

def build_spark():
    return (
        SparkSession.builder
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .getOrCreate()
    )

def main():
    args = parse_args()
    spark = build_spark()

    full_table = f"{args.catalog}.{args.database}.{args.table}"

    comment_sql = (
        f"COMMENT '{args.comment}'"
        if args.comment else ""
    )

    spark.sql(f"""
        ALTER TABLE {full_table}
        ADD COLUMN {args.column_name} {args.column_type} {comment_sql}
    """)

    print(
        f"[OK] Coluna '{args.column_name}' adicionada em {full_table}"
    )

    spark.stop()

if __name__ == "__main__":
    main()
