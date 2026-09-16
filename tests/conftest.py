import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartilhada por toda a sessão de testes (local mode, sem MinIO)."""
    session = (
        SparkSession.builder
        .appName("fraud-pipeline-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
