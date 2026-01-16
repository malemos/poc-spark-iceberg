from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame

def read_partitioned_source(
    spark: SparkSession,
    fmt: str,
    base_uri: str,
    partition_col: Optional[str],
    partition_values: Optional[List[str]],
    infer_schema: bool,
    csv_header: bool
) -> DataFrame:
    reader = spark.read.format(fmt)

    if fmt in ("json", "csv") and infer_schema:
        reader = reader.option("inferSchema", "true")
    if fmt == "csv":
        reader = reader.option("header", "true" if csv_header else "false")

    df = reader.load(base_uri)

    if partition_col and partition_values:
        quoted_vals = ",".join([f"'{v}'" for v in partition_values])
        df = df.where(f"{partition_col} IN ({quoted_vals})")

    return df
