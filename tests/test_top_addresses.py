import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)
from src.transformation.top_addresses import calc_top_addresses


SCHEMA = StructType([
    StructField("receiving_address", StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("transaction_type",  StringType(),  True),
    StructField("timestamp",         TimestampType(), True),
    StructField("dq_flag_amount",    BooleanType(), False),
])


@pytest.fixture
def silver_view(spark: SparkSession):
    """Silver com 5 linhas: 4 sale + 1 purchase. Sem flags de DQ."""
    data = [
        ("0xAAA", 5000.0, "sale",     datetime(2021, 1, 5), False),
        ("0xBBB", 3000.0, "sale",     datetime(2021, 1, 3), False),
        ("0xCCC", 4000.0, "sale",     datetime(2021, 1, 4), False),
        ("0xDDD", 2000.0, "sale",     datetime(2021, 1, 1), False),
        ("0xEEE", 1000.0, "purchase", datetime(2021, 1, 6), False),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    df.createOrReplaceTempView("silver_transactions")
    return df


def test_filtra_apenas_sale(spark: SparkSession, silver_view):
    result = calc_top_addresses(spark, "silver_transactions")
    enderecos = [r["receiving_address"] for r in result.collect()]

    assert "0xEEE" not in enderecos     # purchase → excluído


def test_exclui_flag_amount(spark: SparkSession):
    data = [
        ("0xAAA", 9000.0, "sale", datetime(2021, 1, 5), True),   # flagado → excluído
        ("0xBBB", 3000.0, "sale", datetime(2021, 1, 3), False),
        ("0xCCC", 4000.0, "sale", datetime(2021, 1, 4), False),
        ("0xDDD", 2000.0, "sale", datetime(2021, 1, 1), False),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    df.createOrReplaceTempView("silver_transactions")

    result = calc_top_addresses(spark, "silver_transactions")
    enderecos = [r["receiving_address"] for r in result.collect()]

    assert "0xAAA" not in enderecos


def test_retorna_top_3(spark: SparkSession, silver_view):
    result = calc_top_addresses(spark, "silver_transactions")
    assert result.count() == 3


def test_usa_ultimo_timestamp(spark: SparkSession):
    """
    0xAAA aparece 2x: amount=9000 (jan) e amount=100 (jun).
    A regra mantém o mais recente (jun, amount=100), não o maior amount.
    """
    data = [
        ("0xAAA", 9000.0, "sale", datetime(2021, 1, 1),  False),
        ("0xAAA", 100.0,  "sale", datetime(2021, 6, 1),  False),
        ("0xBBB", 5000.0, "sale", datetime(2021, 3, 1),  False),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    df.createOrReplaceTempView("silver_transactions")

    result = calc_top_addresses(spark, "silver_transactions")
    rows = {r["receiving_address"]: r["amount"] for r in result.collect()}

    assert rows["0xAAA"] == 100.0   # mais recente, não maior
