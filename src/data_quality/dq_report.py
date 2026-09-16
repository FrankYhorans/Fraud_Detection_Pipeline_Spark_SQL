import json
import s3fs
from datetime import datetime
from pyspark.sql import DataFrame
from src.utils.logger import get_logger
from src.utils.storage import get_storage_options
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

CONFORMIDADE_MINIMA = 95.0


def count_raw_lines(file_path: str) -> int:
    """Conta linhas do CSV bruto sem carregar tudo na memória."""
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1  # desconta o header


def calc_metrics(df_silver: DataFrame, total_raw: int) -> dict:
    """Calcula métricas de qualidade via Spark SQL a partir de um DataFrame Silver."""
    spark = df_silver.sparkSession
    df_silver.createOrReplaceTempView("silver_for_dq")

    row = spark.sql("""
        SELECT
            COUNT(*) AS total_apos_limpeza,
            SUM(CAST(dq_flag_amount AS BIGINT))      AS erros_amount,
            SUM(CAST(dq_flag_risk_score AS BIGINT))  AS erros_risk_score,
            SUM(CAST(dq_flag_region AS BIGINT))      AS erros_location_region,
            SUM(CAST(
                (dq_flag_amount OR dq_flag_risk_score OR dq_flag_region)
            AS BIGINT))                              AS total_problematicos,
            ROUND(
                (1 - SUM(CAST(dq_flag_amount AS BIGINT)) / COUNT(*)) * 100, 2
            ) AS pct_amount,
            ROUND(
                (1 - SUM(CAST(dq_flag_risk_score AS BIGINT)) / COUNT(*)) * 100, 2
            ) AS pct_risk,
            ROUND(
                (1 - SUM(CAST(dq_flag_region AS BIGINT)) / COUNT(*)) * 100, 2
            ) AS pct_region,
            ROUND(
                (1 - SUM(CAST(
                    (dq_flag_amount OR dq_flag_risk_score OR dq_flag_region)
                AS BIGINT)) / COUNT(*)) * 100, 2
            ) AS pct_geral
        FROM silver_for_dq
    """).first()

    n = row["total_apos_limpeza"]
    metrics = {
        "generated_at":                     datetime.now().isoformat(),
        "total_registros_raw":              int(total_raw),
        "total_registros_apos_limpeza":     int(n),
        "duplicatas_removidas":             int(total_raw - n),
        "erros_amount":                     int(row["erros_amount"]),
        "erros_risk_score":                 int(row["erros_risk_score"]),
        "erros_location_region":            int(row["erros_location_region"]),
        "total_registros_problematicos":    int(row["total_problematicos"]),
        "pct_conformidade_amount":          float(row["pct_amount"]),
        "pct_conformidade_risk_score":      float(row["pct_risk"]),
        "pct_conformidade_location_region": float(row["pct_region"]),
        "pct_conformidade_geral":           float(row["pct_geral"]),
    }

    conformidade = metrics["pct_conformidade_geral"]
    logger.info(
        f"Raw: {total_raw:,} | Pós-limpeza: {n:,} | "
        f"Duplicatas: {total_raw - n:,}"
    )
    logger.info(
        f"Erros — amount: {metrics['erros_amount']:,} | "
        f"risk_score: {metrics['erros_risk_score']:,} | "
        f"region: {metrics['erros_location_region']:,}"
    )
    if conformidade < 90:
        logger.warning(f"Conformidade geral CRÍTICA: {conformidade:.2f}%")
    elif conformidade < 95:
        logger.warning(f"Conformidade geral abaixo do esperado: {conformidade:.2f}%")
    else:
        logger.info(f"Conformidade geral: {conformidade:.2f}% — dentro do esperado.")

    return metrics


def save_report(metrics: dict, output_path: str) -> None:
    """Salva o relatório JSON no MinIO (s3://) ou no filesystem local."""
    try:
        if output_path.startswith("s3://"):
            opts = get_storage_options()
            fs = s3fs.S3FileSystem(**opts)
            s3_path = output_path.replace("s3://", "")
            with fs.open(s3_path, "w") as f:
                json.dump(metrics, f, indent=2, ensure_ascii=False)
        else:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2, ensure_ascii=False)
        logger.info(f"Relatório salvo em: {output_path}")
    except Exception:
        logger.exception("Erro ao salvar o relatório.")
        raise


def run(
    silver_path: str = "s3a://silver/transactions_clean.parquet",
    report_path: str = "s3://reports/dq_report.json",
    file_path_raw: str = "dados/bronze/df_fraud_credit.csv",
) -> dict:
    spark = get_spark_session("dq_report")
    df_silver = spark.read.parquet(silver_path)
    total_raw = count_raw_lines(file_path_raw)

    metrics = calc_metrics(df_silver, total_raw)
    save_report(metrics, report_path)

    conformidade = metrics["pct_conformidade_geral"]
    if conformidade < CONFORMIDADE_MINIMA:
        raise ValueError(
            f"Quality gate falhou: conformidade geral {conformidade:.2f}% "
            f"abaixo do mínimo de {CONFORMIDADE_MINIMA}%"
        )

    logger.info(f"Quality gate aprovado: {conformidade:.2f}%")
    logger.info("Relatório completo:\n" + json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    import json as _json
    metrics = run()
    logger.info(_json.dumps(metrics, indent=2))
