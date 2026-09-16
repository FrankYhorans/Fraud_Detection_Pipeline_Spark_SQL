import os
from pyspark.sql import SparkSession

_JARS_DIR = os.environ.get("SPARK_JARS_DIR", "/opt/airflow/spark_jars")
_HADOOP_JAR = f"{_JARS_DIR}/hadoop-aws-3.3.4.jar"
_SDK_JAR = f"{_JARS_DIR}/aws-java-sdk-bundle-1.12.276.jar"


def get_spark_session(app_name: str = "fraud_pipeline") -> SparkSession:
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", f"{_HADOOP_JAR},{_SDK_JAR}")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
