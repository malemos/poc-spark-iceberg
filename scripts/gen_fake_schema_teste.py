# gen_fake_schema_teste.py
#
# Gera dados fake no formato Kafka → Bronze
# Pronto para testar MERGE INTO Iceberg (Spark 3.4+)

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta

OUTPUT_PATH = "file:///work/bronze/schema_teste/tabela_teste"

START_DATE = "2026-01-01"
END_DATE   = "2026-01-03"

ROWS_PER_DAY = 1000
KEY_SPACE    = 500  # força repetição de idt_teste entre dias

spark = (
    SparkSession.builder
    .appName("gen-fake-bronze-schema_teste-tabela_teste")
    .getOrCreate()
)

def daterange(start, end):
    d = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    while d <= e:
        yield d
        d += timedelta(days=1)

dfs = []

for i, dt in enumerate(daterange(START_DATE, END_DATE)):
    base_ts = datetime.combine(dt, datetime.min.time())

    df_day = (
        spark.range(0, ROWS_PER_DAY)
        .withColumn(
            "idt_teste",
            (F.col("id") % KEY_SPACE).cast("string")   # 🔑 chave reaproveitada
        )
        .withColumn(
            "dat_update",
            F.lit(base_ts + timedelta(hours=i)).cast("timestamp")
        )
        .withColumn(
            "ind_status",
            F.when(F.col("id") % 3 == 0, F.lit("A"))
             .when(F.col("id") % 3 == 1, F.lit("I"))
             .otherwise(F.lit("P"))
        )
        .withColumn(
            "des_teste",
            F.concat(
                F.lit("descricao_"),
                F.col("idt_teste"),
                F.lit("_v"),
                F.lit(i)
            )
        )
        .withColumn(
            "flg_teste",
            F.when(F.col("id") % 2 == 0, F.lit("Y")).otherwise(F.lit("N"))
        )
        .withColumn(
            "dat_kafka",
            (
                F.lit(base_ts).cast("timestamp").cast("long") + F.col("id")
            ).cast("timestamp")
        )
        .withColumn(
            "dt_ref",
            F.lit(dt).cast("date")
        )
        .drop("id")
    )

    dfs.append(df_day)

final_df = dfs[0]
for d in dfs[1:]:
    final_df = final_df.unionByName(d)

(
    final_df
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("dt_ref")
    .save(OUTPUT_PATH)
)

spark.stop()
print(f"[Info] Dados fake gerados em: {OUTPUT_PATH}")
