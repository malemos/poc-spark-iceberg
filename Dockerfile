# Dockerfile
FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG SPARK_VERSION=3.4.1
ARG HADOOP_VERSION=3
ARG JAVA_VERSION=17

# 1) SO base + Java + Python + utilitários
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates wget bash procps tini \
    openjdk-${JAVA_VERSION}-jdk python3 python3-pip python3-venv \
    git nano vim zip unzip \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 2) Variáveis padrão do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 3) Copia o tar do Spark (local) e instala corretamente
COPY libs/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN set -eux; \
    mkdir -p /opt; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    # descobre a pasta raiz dentro do tgz
    SPARK_DIR="$(tar -tzf /tmp/spark.tgz | head -1 | cut -f1 -d'/')"; \
    # cria link /opt/spark apontando para a pasta extraída
    ln -sfn "/opt/${SPARK_DIR}" "${SPARK_HOME}"; \
    rm -f /tmp/spark.tgz; \
    # remove wrappers conflitantes e cria symlinks que apontam para o Spark correto
    rm -f /usr/local/bin/spark-submit /usr/local/bin/pyspark /usr/local/bin/spark-shell /usr/local/bin/spark-class || true; \
    ln -s ${SPARK_HOME}/bin/spark-submit /usr/local/bin/spark-submit; \
    ln -s ${SPARK_HOME}/bin/pyspark      /usr/local/bin/pyspark; \
    ln -s ${SPARK_HOME}/bin/spark-shell  /usr/local/bin/spark-shell; \
    ln -s ${SPARK_HOME}/bin/spark-class  /usr/local/bin/spark-class

# 4) Copia os JARs do Iceberg (coloque-os em libs/ no host)
# Iceberg Spark Runtime
COPY libs/iceberg-spark-runtime-3.4_2.12-1.4.3.jar $SPARK_HOME/jars/

# Hadoop + AWS (necessário para S3 / OCI compat)
# COPY libs/hadoop-aws-3.3.4.jar                     $SPARK_HOME/jars/
# COPY libs/aws-java-sdk-bundle-1.12.262.jar         $SPARK_HOME/jars/


# 5) (Opcional) Dependências Python — se você não precisa, pode remover requirements.txt e este passo
COPY requirements.txt /tmp/requirements.txt
RUN pip3 config set global.trusted-host \
    "pypi.org files.pythonhosted.org pypi.python.org repo.intranet.pags" \
    --trusted-host=pypi.python.org \
    --trusted-host=pypi.org \
    --trusted-host=files.pythonhosted.org \
    --trusted-host=repo.intranet.pags
RUN if [ -s /tmp/requirements.txt ]; then pip3 install --no-cache-dir -r /tmp/requirements.txt; fi

# 6) App e entrypoint
RUN mkdir -p /opt/app /work
WORKDIR /opt/app

COPY common/ ./common/
COPY jobs/   ./jobs/
COPY scripts/ ./scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PYTHONUNBUFFERED=1

# 7) EntryPoint: deixa as configs do Delta prontas via SPARK_OPTS
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["bash"]
