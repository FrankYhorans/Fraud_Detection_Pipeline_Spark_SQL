import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)
from src.transformation.risk_region import calc_risk_region


@pytest.fixture
def silver_view(spark: SparkSession):
    """Registra um Silver mínimo como view 'silver_transactions'."""
    schema = StructType([
        StructField("location_region",    StringType(),  True),
        StructField("risk_score",         DoubleType(),  True),
        StructField("dq_flag_risk_score", BooleanType(), False),
        StructField("dq_flag_region",     BooleanType(), False),
    ])
    data = [
        ("North America", 80.0, False, False),   # válida
        ("Europe",        40.0, False, False),   # válida
        ("Asia",          60.0, True,  False),   # excluída por flag_risk_score
        ("Africa",        50.0, False, True),    # excluída por flag_region
    ]
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("silver_transactions")
    return df


def test_exclui_flag_risk_score(spark: SparkSession, silver_view):
    result = calc_risk_region(spark, "silver_transactions")
    regioes = [r["location_region"] for r in result.collect()]

    assert "Asia" not in regioes
    assert len(regioes) == 2


def test_exclui_flag_region(spark: SparkSession, silver_view):
    result = calc_risk_region(spark, "silver_transactions")
    regioes = [r["location_region"] for r in result.collect()]

    assert "Africa" not in regioes
    assert len(regioes) == 2


def test_resultado_ordenado_descrescente(spark: SparkSession, silver_view):
    result = calc_risk_region(spark, "silver_transactions")
    scores = [r["avg_risk_score"] for r in result.collect()]

    assert scores[0] >= scores[1]


def test_colunas_corretas(spark: SparkSession, silver_view):
    result = calc_risk_region(spark, "silver_transactions")
    assert result.columns == ["location_region", "avg_risk_score"]
