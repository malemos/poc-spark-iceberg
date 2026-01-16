import argparse
from typing import List
from pyspark.sql import SparkSession, functions as F

from common.io import read_partitioned_source
from common.spark_ops.dedup import deduplicate
from common.utils import daterange, csv_to_list


# ------------------------------------------------------------------------------
# Args
# ------------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Validação Bronze vs Silver (Iceberg)"
    )

    # Bronze
    p.add_argument("--bronze-uri", required=True)
    p.add_argument("--source-format", default="parquet")

    # Silver
    p.add_argument("--catalog", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--table", required=True)

    # Merge logic
    p.add_argument("--pk", required=True, help="Chave(s) primária(s) csv")
    p.add_argument("--dedup-col", default=None)

    # Partições Bronze
    p.add_argument("--partition-col", required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--partition-values")
    group.add_argument("--partition-range")
    p.add_argument("--partition-format", default="%Y-%m-%d")

    # Comparação
    p.add_argument(
        "--compare-columns",
        default=None,
        help="Colunas a comparar (csv). Default: todas exceto partições",
    )

    # CSV / JSON
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
# Validations
# ------------------------------------------------------------------------------

def validate_counts(bronze_df, silver_df, keys):
    bronze_cnt = bronze_df.count()
    silver_cnt = silver_df.count()
    print("==================================================================")
    print("[Check] Validating record counts...")
    print(f"[Check] Bronze dedup count: {bronze_cnt}")
    print(f"[Check] Silver count       : {silver_cnt}")

    if bronze_cnt != silver_cnt:
        raise AssertionError(
            f"Count mismatch: bronze={bronze_cnt}, silver={silver_cnt}"
        )


def validate_key_coverage(bronze_df, silver_df, keys):
    cond = [bronze_df[k] == silver_df[k] for k in keys]

    missing = (
        bronze_df
        .select(*keys)
        .dropDuplicates()
        .join(
            silver_df.select(*keys).dropDuplicates(),
            on=keys,
            how="left_anti",
        )
        .count()
    )

    if missing > 0:
        raise AssertionError(
            f"[Fail] {missing} chaves da bronze não existem na silver"
        )

    print("==================================================================")
    print("[Check] Cobertura de chaves OK")


def validate_column_values(bronze_df, silver_df, keys, columns):
    cond = [bronze_df[k] == silver_df[k] for k in keys]

    diffs = (
        bronze_df.alias("b")
        .join(silver_df.alias("s"), on=keys, how="inner")
        .select([
            F.sum(
                F.when(
                    F.col(f"b.{c}") != F.col(f"s.{c}"), 1
                ).otherwise(0)
            ).alias(c)
            for c in columns
        ])
        .collect()[0]
        .asDict()
    )

    errors = {c: v for c, v in diffs.items() if v > 0}

    if errors:
        raise AssertionError(
            f"[Fail] Diferença de valores detectada: {errors}"
        )

    print("==================================================================")
    print("[Check] Valores das colunas OK")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    args = parse_args()
    spark = build_spark(
        app_name=f"validate-bronze-vs-silver-{args.database}.{args.table}"
    )

    try:
        keys = [c.strip() for c in args.pk.split(",")]

        part_values = resolve_partition_values(args)

        # ----------------------------------------------------------------------
        # Bronze (deduplicada)
        # ----------------------------------------------------------------------
        bronze_df = read_partitioned_source(
            spark=spark,
            fmt=args.source_format,
            base_uri=args.bronze_uri,
            partition_col=args.partition_col,
            partition_values=part_values,
            infer_schema=args.infer_schema,
            csv_header=args.csv_header,
        )

        print("==================================================================")
        print(f"[Info] Registros lidos da bronze: {bronze_df.count()}")

        bronze_df = deduplicate(bronze_df, keys, args.dedup_col)

        # ----------------------------------------------------------------------
        # Silver
        # ----------------------------------------------------------------------
        silver_df = spark.table(
            f"{args.catalog}.{args.database}.{args.table}"
        )

        # ----------------------------------------------------------------------
        # Validações
        # ----------------------------------------------------------------------
        validate_counts(bronze_df, silver_df, keys)
        validate_key_coverage(bronze_df, silver_df, keys)

        if args.compare_columns:
            cols = [c.strip() for c in args.compare_columns.split(",")]
        else:
            cols = [
                c for c in bronze_df.columns
                if c not in keys and c != args.partition_col
            ]

        validate_column_values(bronze_df, silver_df, keys, cols)

        print("==================================================================")
        print("[SUCCESS] Bronze e Silver estão consistentes")
        print("==================================================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
