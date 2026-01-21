## PoC Spark + Apache Iceberg (Bronze → Silver)

Prova de conceito de um pipeline **Lakehouse** usando **Apache Spark** e **Apache Iceberg**, com ingestão simulada via Kafka, geração de dados fake na camada Bronze, consolidação genérica na camada Silver via `MERGE INTO` e validação de consistência entre camadas.

O projeto roda **localmente via Docker** e está preparado para futura execução em **OCI Data Flow**.

---

## Stack

- Apache Spark 3.4.1
- Apache Iceberg 1.4.3
- Docker
- Python (PySpark)
- Arquitetura Bronze → Silver
- MERGE ACID (Iceberg)

---

## Estrutura do projeto

- **common/** – Bibliotecas Python reutilizáveis
- **jobs/** – Jobs Spark (consolidação Silver e validações)
- **scripts/** – Geradores de dados fake (simulação Kafka → Bronze)
- **libs/** – Binários do Spark e JARs do Iceberg
- **work/** – Dados locais e warehouse Iceberg (não versionado)
- **Dockerfile** – Ambiente Spark local
- **entrypoint.sh** – Inicialização do Spark
---
## Download das dependências (obrigatório)

Antes do build da imagem, faça o download dos binários necessários.

### Apache Spark 3.4.1

```bash
mkdir -p libs
cd libs

wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar
```
---
## Build da imagem Docker
```
docker build --network=host -t poc-spark-delta:latest .
```
---
## Executando o container local
```
docker run --rm -it \
  --network=host \
  -v "$PWD/work":/work \
  -v "$PWD/common":/opt/app/common \
  -v "$PWD/jobs":/opt/app/jobs \
  -v "$PWD/scripts":/opt/app/scripts \
  poc-spark-delta:latest bash
```
---
## Validação do ambiente Spark
Dentro do container:
```
echo $SPARK_HOME                            # /opt/spark
which spark-submit                          # /opt/spark/bin/spark-submit
spark-submit --version                      # deve imprimir Spark 3.4.1
ls $SPARK_HOME/jars | grep iceberg          # iceberg-spark-runtime-3.4_2.12-1.4.3.jar
```
---
## Geração de dados Bronze (entidade única)
Simula múltiplos tópicos Kafka.
```
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.executor.memoryOverhead=1g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  scripts/gen_fake_schema_teste.py
```
---
## Geração de dados Bronze (múltiplas entidades)
```
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.executor.memoryOverhead=1g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  scripts/gen_fake_bronze_multi_entities.py \
  --entities cliente,produto,pedido,fatura \
  --start-date 2026-01-01 \
  --end-date 2026-01-20 \
  --rows-per-day 1000000 \
  --key-space 3000
```
---
## Gerando dependency archive (libs Python)
Obrigatório para execução distribuída do Spark.
```
cd /opt/app
rm /work/archive.zip
zip -r /work/archive.zip common -x "*/__pycache__/*"
```
---
## Consolidação Bronze → Silver (Iceberg)
```
spark-submit \
  --py-files /work/archive.zip \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.executor.memoryOverhead=1g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
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
---
## Validação Bronze vs Silver
Verifica:

  * contagem
  * cobertura de chaves
  * consistência de valores
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
---
## Observações importantes

* O job Silver é genérico (sem regra de negócio).
* O MERGE é feito via Apache Iceberg (ACID).
* O mesmo código pode ser executado em OCI Data Flow sem alterações.
* Todos os dados locais ficam em work/ (não versionado).