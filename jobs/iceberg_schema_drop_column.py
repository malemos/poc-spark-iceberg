import argparse
from pyspark.sql import SparkSession

def parse_args():
    p = argparse.ArgumentParser(
        description="Iceberg schema evolution - drop column"
    )
    p.add_argument("--catalog", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--column-name", required=True)
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

    spark.sql(f"""
        ALTER TABLE {full_table}
        DROP COLUMN {args.column_name}
    """)

    print(
        f"[OK] Coluna '{args.column_name}' removida de {full_table}"
    )

    spark.stop()

if __name__ == "__main__":
    main()
