def ensure_iceberg_table(
    spark,
    full_table_name: str,
    staging_view: str,
    partitions=None,
    recreate=False
):
    if recreate:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

    partition_sql = (
        f"PARTITIONED BY ({', '.join(partitions)})"
        if partitions else ""
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING iceberg
        {partition_sql}
        AS SELECT * FROM {staging_view} WHERE 1=0
    """)


def merge_iceberg(
    spark,
    full_table_name: str,
    staging_view: str,
    keys
):
    cond = " AND ".join([f"t.{k} <=> s.{k}" for k in keys])

    spark.sql(f"""
        MERGE INTO {full_table_name} t
        USING {staging_view} s
        ON {cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
