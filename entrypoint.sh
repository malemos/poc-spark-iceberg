
#!/usr/bin/env bash
# entrypoint.sh

# Se quiser, pode exportar aqui variáveis padrão para Spark
export SPARK_OPTS=" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
"

echo "Iniciando container. Use 'spark-shell' / 'pyspark' / 'spark-submit' com as configs Delta."
exec "$@"
