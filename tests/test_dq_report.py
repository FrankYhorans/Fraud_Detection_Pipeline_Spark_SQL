import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType
from src.data_quality.dq_report import calc_metrics, run, CONFORMIDADE_MINIMA


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

CSV_HEADER = (
    "timestamp,amount,risk_score,location_region,transaction_type,"
    "sending_address,receiving_address,purchase_pattern,age_group,anomaly\n"
)


def make_row(i: int) -> str:
    return (
        f"{1609459200 + i},100.0,50.0,North America,sale,"
        f"0xSEND{i},0xRECV{i},online,18-25,low_risk\n"
    )


def create_raw_csv(path, n_rows: int) -> str:
    content = CSV_HEADER + "".join(make_row(i) for i in range(n_rows))
    path.write_text(content, encoding="utf-8")
    return str(path)


def make_silver_df(spark: SparkSession, flags: list[tuple[bool, bool, bool]]):
    """Cria DataFrame Silver com apenas as colunas de flags DQ."""
    schema = StructType([
        StructField("dq_flag_amount",     BooleanType(), False),
        StructField("dq_flag_risk_score", BooleanType(), False),
        StructField("dq_flag_region",     BooleanType(), False),
    ])
    return spark.createDataFrame(flags, schema)


# ---------------------------------------------------------------------------
# TESTES — calc_metrics: contagens básicas
# ---------------------------------------------------------------------------

def test_total_registros_raw(spark: SparkSession, tmp_path):
    """total_registros_raw deve refletir o número de linhas do CSV bruto."""
    create_raw_csv(tmp_path / "raw.csv", n_rows=5)
    df = make_silver_df(spark, [(False, False, False)] * 4)
    metrics = calc_metrics(df, total_raw=5)
    assert metrics["total_registros_raw"] == 5


def test_total_apos_limpeza(spark: SparkSession):
    """total_registros_apos_limpeza = número de linhas do Silver."""
    df = make_silver_df(spark, [(False, False, False)] * 4)
    metrics = calc_metrics(df, total_raw=5)
    assert metrics["total_registros_apos_limpeza"] == 4


def test_duplicatas_contadas(spark: SparkSession):
    """duplicatas_removidas = raw - silver."""
    df = make_silver_df(spark, [(False, False, False)] * 4)
    metrics = calc_metrics(df, total_raw=5)
    assert metrics["duplicatas_removidas"] == 1


def test_erros_amount(spark: SparkSession):
    """1 flag_amount=True → erros_amount == 1."""
    df = make_silver_df(spark, [
        (True,  False, False),
        (False, True,  False),
        (False, False, False),
        (False, False, False),
    ])
    metrics = calc_metrics(df, total_raw=5)
    assert metrics["erros_amount"] == 1


def test_erros_risk_score(spark: SparkSession):
    """1 flag_risk_score=True → erros_risk_score == 1."""
    df = make_silver_df(spark, [
        (True,  False, False),
        (False, True,  False),
        (False, False, False),
        (False, False, False),
    ])
    metrics = calc_metrics(df, total_raw=5)
    assert metrics["erros_risk_score"] == 1


# ---------------------------------------------------------------------------
# TESTE — sem dupla contagem (OR lógico)
# ---------------------------------------------------------------------------

def test_sem_dupla_contagem(spark: SparkSession):
    """
    Linha 0 tem flag_amount=True E flag_risk_score=True.
    O OR deve contar esse registro como 1 problemático, não 2.
    """
    df = make_silver_df(spark, [
        (True,  True,  False),
        (False, False, False),
        (False, False, False),
    ])
    metrics = calc_metrics(df, total_raw=3)
    assert metrics["total_registros_problematicos"] == 1


# ---------------------------------------------------------------------------
# TESTES — percentuais de conformidade
# ---------------------------------------------------------------------------

def test_conformidade_100_pct(spark: SparkSession):
    """Silver sem nenhum flag → conformidade geral = 100%."""
    df = make_silver_df(spark, [(False, False, False)] * 4)
    metrics = calc_metrics(df, total_raw=4)
    assert metrics["pct_conformidade_geral"] == 100.0


def test_conformidade_50_pct(spark: SparkSession):
    """2 de 4 registros problemáticos → conformidade = 50%."""
    df = make_silver_df(spark, [
        (True,  False, False),
        (True,  False, False),
        (False, False, False),
        (False, False, False),
    ])
    metrics = calc_metrics(df, total_raw=4)
    assert metrics["pct_conformidade_geral"] == 50.0


# ---------------------------------------------------------------------------
# TESTES — quality gate (via run())
# ---------------------------------------------------------------------------

def test_quality_gate_levanta_erro(spark: SparkSession, tmp_path):
    """
    Silver com 50% conformidade (abaixo de 95%) deve fazer run() levantar ValueError.
    O Silver é escrito em Parquet local para que run() consiga ler com Spark.
    """
    df = make_silver_df(spark, [
        (True,  False, False),
        (True,  False, False),
        (False, False, False),
        (False, False, False),
    ])
    silver_path = str(tmp_path / "silver.parquet")
    df.write.mode("overwrite").parquet(silver_path)

    raw_path = str(tmp_path / "raw.csv")
    create_raw_csv(tmp_path / "raw.csv", n_rows=4)

    report_path = str(tmp_path / "report.json")

    with pytest.raises(ValueError, match="Quality gate falhou"):
        run(silver_path=silver_path, report_path=report_path, file_path_raw=raw_path)


def test_quality_gate_aprovado(spark: SparkSession, tmp_path):
    """Silver com 100% conformidade → run() retorna sem erro."""
    df = make_silver_df(spark, [(False, False, False)] * 4)
    silver_path = str(tmp_path / "silver2.parquet")
    df.write.mode("overwrite").parquet(silver_path)

    raw_path = str(tmp_path / "raw2.csv")
    create_raw_csv(tmp_path / "raw2.csv", n_rows=4)

    report_path = str(tmp_path / "report2.json")

    metrics = run(silver_path=silver_path, report_path=report_path, file_path_raw=raw_path)
    assert metrics["pct_conformidade_geral"] == 100.0
