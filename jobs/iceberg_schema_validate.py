import argparse
from pyspark.sql import SparkSession

def parse_args():
    p = argparse.ArgumentParser(
        description="Validação de schema evolution Iceberg"
    )
    p.add_argument("--catalog", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--limit", type=int, default=10)
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

    print("\nSchema atual:")
    spark.sql(f"DESCRIBE TABLE {full_table}").show(truncate=False)

    print("\nLeitura de dados (arquivos antigos + novos):")
    spark.sql(f"SELECT * FROM {full_table} LIMIT {args.limit}").show()

    print("\nLeitura de snapshot anterior:")
    snap = spark.sql(
        f"SELECT snapshot_id FROM {full_table}.snapshots ORDER BY committed_at LIMIT 1"
    ).collect()[0][0]

    spark.sql(
        f"SELECT * FROM {full_table} VERSION AS OF {snap} LIMIT {args.limit}"
    ).show()

    spark.stop()

if __name__ == "__main__":
    main()
