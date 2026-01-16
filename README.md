# Build da imagem docker
```
docker build --network=host -t poc-spark-delta:latest .
```

# Executando docker local
```
docker run --rm -it \
  -v "$PWD/work":/work \
  -v "$PWD/common":/opt/app/common \
  -v "$PWD/jobs":/opt/app/jobs \
  -v "$PWD/scripts":/opt/app/scripts \
  poc-spark-delta:latest bash
```

# conferindo se a conf esta correta
```
echo $SPARK_HOME                            # /opt/spark
which spark-submit                          # /opt/spark/bin/spark-submit
spark-submit --version                      # deve imprimir Spark 3.4.1
ls $SPARK_HOME/jars | grep iceberg          # iceberg-spark-runtime-3.4_2.12-1.4.3.jar
```
# gerar bronze
```
spark-submit scripts/gen_fake_schema_teste.py
```

# Gerando dados (bronze) fake generico
```
spark-submit \
  scripts/gen_fake_bronze_multi_entities.py \
  --entities cliente,produto,pedido,fatura \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --rows-per-day 1000 \
  --key-space 300
```

# zip do common e job
```
cd /opt/app
rm /work/archive.zip
zip -r /work/archive.zip common -x "*/__pycache__/*"
```

# Consolidando para aa camada silver
```
spark-submit \
  --py-files /work/archive.zip \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/work/warehouse \
  jobs/poc_silver_iceberg_generic.py \
  --bronze-uri file:///work/bronze/schema_teste/tabela_teste \
  --catalog local \
  --database silver \
  --table tabela_teste \
  --pk idt_teste \
  --dedup-col dat_kafka \
  --partition-col dt_ref \
  --partition-range 2026-01-01,2026-01-03
```

# validar bronze vs silver
```
spark-submit \
  --py-files /work/archive.zip \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/work/warehouse \
  jobs/validate_bronze_vs_silver.py \
  --bronze-uri file:///work/bronze/schema_teste/tabela_teste \
  --catalog local \
  --database silver \
  --table tabela_teste \
  --pk idt_teste \
  --dedup-col dat_kafka \
  --partition-col dt_ref \
  --partition-range 2026-01-01,2026-01-03
```
