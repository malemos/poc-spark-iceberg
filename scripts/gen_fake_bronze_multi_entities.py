"""
Gera dados fake na camada Bronze para múltiplas entidades.
Simula Kafka -> Bronze para testes de consolidação Silver (Iceberg).

Compatível com Spark 3.4+
"""

import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F


# ------------------------------------------------------------------------------
# Args
# ------------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Gerador fake de múltiplas entidades (Bronze)"
    )

    p.add_argument(
        "--output-root",
        default="file:///work/bronze",
        help="Root da bronze (default file:///work/bronze)",
    )
    p.add_argument(
        "--schema",
        default="schema_teste",
        help="Schema lógico (default schema_teste)",
    )
    p.add_argument(
        "--entities",
        required=True,
        help="Lista de entidades/tabelas (csv), ex: cliente,produto,pedido",
    )

    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)

    p.add_argument("--rows-per-day", type=int, default=1000)
    p.add_argument(
        "--key-space",
        type=int,
        default=500,
        help="Cardinalidade da chave (força updates entre dias)",
    )

    return p.parse_args()


# ------------------------------------------------------------------------------
# Spark
# ------------------------------------------------------------------------------

def build_spark():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return (
        SparkSession.builder
        .appName(f"gen-fake-bronze-multi-entities-{now}")
        .getOrCreate()
    )


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def daterange(start, end):
    d = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    while d <= e:
        yield d
        d += timedelta(days=1)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    args = parse_args()
    spark = build_spark()

    entities = [e.strip() for e in args.entities.split(",")]

    for entity in entities:
        dfs = []

        for i, dt in enumerate(daterange(args.start_date, args.end_date)):
            base_ts = datetime.combine(dt, datetime.min.time())

            df_day = (
                spark.range(0, args.rows_per_day)
                .withColumn(
                    f"id_{entity}",
                    (F.col("id") % args.key_space).cast("string"),
                )
                .withColumn(
                    "dat_update",
                    F.lit(base_ts + timedelta(hours=i)).cast("timestamp"),
                )
                .withColumn(
                    "ind_status",
                    F.when(F.col("id") % 3 == 0, F.lit("A"))
                     .when(F.col("id") % 3 == 1, F.lit("I"))
                     .otherwise(F.lit("P")),
                )
                .withColumn(
                    f"des_{entity}",
                    F.concat(
                        F.lit(f"{entity}_"),
                        F.col(f"id_{entity}"),
                        F.lit("_v"),
                        F.lit(i),
                    ),
                )
                .withColumn(
                    "flg_ativo",
                    F.when(F.col("id") % 2 == 0, F.lit("Y"))
                     .otherwise(F.lit("N")),
                )
                .withColumn(
                    "dat_kafka",
                    (
                        F.lit(base_ts).cast("timestamp").cast("long")
                        + F.col("id")
                    ).cast("timestamp"),
                )
                .withColumn(
                    "dt_ref",
                    F.lit(dt).cast("date"),
                )
                .drop("id")
            )

            dfs.append(df_day)

        final_df = dfs[0]
        for d in dfs[1:]:
            final_df = final_df.unionByName(d)

        output_path = (
            f"{args.output_root.rstrip('/')}/"
            f"{args.schema}/{entity}"
        )

        (
            final_df
            .write
            .mode("overwrite")
            .format("parquet")
            .partitionBy("dt_ref")
            .save(output_path)
        )

        print(f"[OK] Entidade '{entity}' gerada em {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
