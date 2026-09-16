FROM apache/airflow:2.9.0

USER root

# Java 17 é obrigatório para PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Diretório para os JARs do conector S3A (MinIO)
RUN mkdir -p /opt/airflow/spark_jars && chown -R airflow: /opt/airflow/spark_jars

USER airflow

# PySpark — versão alinhada com Hadoop 3.3.4
RUN pip install --no-cache-dir pyspark==3.4.3 pyarrow s3fs

# JARs necessários para Spark ler/escrever no MinIO via protocolo s3a://
# hadoop-aws 3.3.4 é a versão embarcada no Spark 3.4.x
# aws-java-sdk-bundle 1.12.276 é compatível com hadoop-aws 3.3.4
RUN curl -fSL -o /opt/airflow/spark_jars/hadoop-aws-3.3.4.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    curl -fSL -o /opt/airflow/spark_jars/aws-java-sdk-bundle-1.12.276.jar \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.276/aws-java-sdk-bundle-1.12.276.jar"

ENV SPARK_JARS_DIR=/opt/airflow/spark_jars
