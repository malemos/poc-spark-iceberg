import argparse

from datetime import datetime

from typing import List
from pyspark.sql import SparkSession

from common.io import read_partitioned_source
from common.utils import daterange, csv_to_list
from common.spark_ops.dedup import deduplicate
from common.spark_ops.iceberg import (
    ensure_iceberg_table,
    merge_iceberg,
)


# ------------------------------------------------------------------------------
# Args
# ------------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="PoC Silver Upsert Genérico usando Apache Iceberg"
    )

    # Source (Bronze)
    p.add_argument(
        "--bronze-uri",
        required=True,
        help="ex: file:///work/bronze/schema_teste/tabela_teste",
    )
    p.add_argument(
        "--source-format",
        default="parquet",
        choices=["parquet", "json", "avro", "csv"],
    )

    # Iceberg target
    p.add_argument("--catalog", required=True, help="ex: local")
    p.add_argument("--database", required=True, help="ex: silver")
    p.add_argument("--table", required=True, help="ex: tabela_teste")

    # Merge
    p.add_argument(
        "--pk",
        required=True,
        help="Colunas de chave de merge (csv), ex: idt_teste",
    )
    p.add_argument(
        "--dedup-col",
        default=None,
        help="Coluna para deduplicação (ex: dat_kafka)",
    )

    # Bronze partitions
    p.add_argument(
        "--partition-col",
        required=True,
        help="Coluna de partição da bronze (ex: dt_ref)",
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--partition-values",
        help="Valores de partição (csv), ex: 2026-01-01,2026-01-02",
    )
    group.add_argument(
        "--partition-range",
        help="Intervalo [ini,fim], ex: 2026-01-01,2026-01-03",
    )
    p.add_argument(
        "--partition-format",
        default="%Y-%m-%d",
        help="Formato das partições (default %%Y-%%m-%%d)",
    )

    # Iceberg options
    p.add_argument(
        "--silver-partitions",
        default=None,
        help="Colunas de partição da silver Iceberg (csv)",
    )
    p.add_argument(
        "--recreate-silver",
        action="store_true",
        help="Dropa e recria a tabela Iceberg",
    )

    # CSV / JSON options
    p.add_argument("--infer-schema", action="store_true")
    p.add_argument("--csv-header", action="store_true")

    return p.parse_args()


# ------------------------------------------------------------------------------
# Spark
# ------------------------------------------------------------------------------

def build_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def resolve_partition_values(args) -> List[str]:
    if args.partition_values:
        return csv_to_list(args.partition_values)

    start, end = [s.strip() for s in args.partition_range.split(",")]
    return daterange(start, end, fmt=args.partition_format)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    args = parse_args()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    spark = build_spark(
        app_name=f"poc-silver-iceberg-{args.database}.{args.table}-{now}"
    )

    try:
        keys = [c.strip() for c in args.pk.split(",")]
        silver_partitions = (
            [c.strip() for c in args.silver_partitions.split(",")]
            if args.silver_partitions
            else []
        )
        print("==================================================================")
        print(args)
        print(f"[Info] Chaves de merge: {keys}")
        print(f"[Info] Partições da silver: {silver_partitions}")

        part_values = resolve_partition_values(args)
        print(f"[Info] Valores de partição: {part_values}")
        print("==================================================================")

        # ----------------------------------------------------------------------
        # Read Bronze
        # ----------------------------------------------------------------------
        df = read_partitioned_source(
            spark=spark,
            fmt=args.source_format,
            base_uri=args.bronze_uri,
            partition_col=args.partition_col,
            partition_values=part_values,
            infer_schema=args.infer_schema,
            csv_header=args.csv_header,
        )


        print(f"[Info] Registros lidos da bronze: {df.count()}")
        df.printSchema()

        # ----------------------------------------------------------------------
        # Validate Silver partitions
        # ----------------------------------------------------------------------
        for pcol in silver_partitions:
            if pcol not in df.columns:
                raise ValueError(
                    f"[Silver Partition] Coluna '{pcol}' não existe no DF."
                )

        # ----------------------------------------------------------------------
        # Dedup
        # ----------------------------------------------------------------------
        df = deduplicate(df, keys, order_col=args.dedup_col)

        # ----------------------------------------------------------------------
        # Iceberg MERGE
        # ----------------------------------------------------------------------
        staging_view = f"stg_{args.table}"
        df.createOrReplaceTempView(staging_view)

        full_table = f"{args.catalog}.{args.database}.{args.table}"

        ensure_iceberg_table(
            spark=spark,
            full_table_name=full_table,
            staging_view=staging_view,
            partitions=silver_partitions,
            recreate=args.recreate_silver,
        )

        merge_iceberg(
            spark=spark,
            full_table_name=full_table,
            staging_view=staging_view,
            keys=keys,
        )

        print(f"[Success] MERGE concluído em {full_table}")

    finally:
        spark.stop()


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
