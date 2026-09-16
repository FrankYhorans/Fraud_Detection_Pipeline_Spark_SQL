import zipfile
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


def extrair_zip(zip_path: str, arquivo_esperado: str = "df_fraud_credit.csv") -> None:
    zip_path = Path(zip_path)

    if not zip_path.exists():
        logger.warning(f"ZIP não encontrado: {zip_path}")
        return

    logger.info(f"Extraindo {zip_path.name}...")
    with zipfile.ZipFile(zip_path, "r") as z:
        nomes = z.namelist()

        if arquivo_esperado not in nomes:
            logger.error(
                f"Arquivo '{arquivo_esperado}' não encontrado dentro do ZIP. "
                f"Arquivos presentes: {nomes}"
            )
            raise FileNotFoundError(
                f"'{arquivo_esperado}' não existe dentro de {zip_path.name}"
            )

        destino = zip_path.parent
        destino.mkdir(parents=True, exist_ok=True)
        z.extractall(destino)
        logger.info(f"Arquivo(s) extraído(s): {', '.join(nomes)}")
    logger.info("Extração concluída.")


def apply_dq_flags(spark: SparkSession, view_name: str = "raw_transactions") -> DataFrame:
    """Converte tipos e cria flags DQ via Spark SQL."""
    return spark.sql(f"""
        SELECT
            CAST(from_unixtime(CAST(timestamp AS BIGINT)) AS TIMESTAMP) AS timestamp,
            CAST(timestamp AS BIGINT)                                    AS timestamp_unix,
            sending_address,
            receiving_address,
            CASE WHEN amount = 'none' THEN NULL
                 ELSE CAST(amount AS DOUBLE) END                         AS amount,
            transaction_type,
            location_region,
            CAST(ip_prefix AS DOUBLE)                                    AS ip_prefix,
            CAST(login_frequency AS INT)                                 AS login_frequency,
            CAST(session_duration AS INT)                                AS session_duration,
            purchase_pattern,
            age_group,
            CASE WHEN risk_score = 'none' THEN NULL
                 ELSE CAST(risk_score AS DOUBLE) END                     AS risk_score,
            anomaly,
            (amount = 'none')       AS dq_flag_amount,
            (risk_score = 'none')   AS dq_flag_risk_score,
            (location_region = '0') AS dq_flag_region
        FROM {view_name}
    """)


def remove_duplicates(spark: SparkSession, view_name: str = "flagged_transactions") -> DataFrame:
    """Remove duplicatas priorizando registros de fraude (scam/phishing) via Spark SQL."""
    return spark.sql(f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY sending_address, receiving_address, timestamp_unix
                    ORDER BY CASE
                        WHEN transaction_type IN ('scam', 'phishing') THEN 1
                        ELSE 0
                    END DESC
                ) AS rn
            FROM {view_name}
        )
        SELECT
            timestamp, sending_address, receiving_address, amount,
            transaction_type, location_region, ip_prefix, login_frequency,
            session_duration, purchase_pattern, age_group, risk_score, anomaly,
            dq_flag_amount, dq_flag_risk_score, dq_flag_region
        FROM ranked
        WHERE rn = 1
    """)


def clean(
    file_path: str = "dados/bronze/df_fraud_credit.csv",
    silver_path: str = "s3a://silver/transactions_clean.parquet",
    base_zip: str = "dados/bronze/df_fraud_credit.zip",
) -> None:
    extrair_zip(base_zip)

    spark = get_spark_session("clean")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
    )

    if df_raw.rdd.isEmpty():
        raise ValueError(f"Arquivo vazio: {file_path}")

    n_raw = df_raw.count()
    logger.info(f"{n_raw:,} registros carregados.")

    df_raw.createOrReplaceTempView("raw_transactions")
    df_flagged = apply_dq_flags(spark)

    flag_counts = df_flagged.selectExpr(
        "SUM(CAST(dq_flag_amount AS INT)) AS fa",
        "SUM(CAST(dq_flag_risk_score AS INT)) AS fr",
        "SUM(CAST(dq_flag_region AS INT)) AS fg",
    ).first()
    logger.info(f"dq_flag_amount:     {flag_counts['fa']:,} registros")
    logger.info(f"dq_flag_risk_score: {flag_counts['fr']:,} registros")
    logger.info(f"dq_flag_region:     {flag_counts['fg']:,} registros")

    df_flagged.createOrReplaceTempView("flagged_transactions")
    df_clean = remove_duplicates(spark)

    n_clean = df_clean.count()
    logger.info(f"{n_raw - n_clean:,} duplicata(s) removida(s). {n_clean:,} registros salvos.")

    df_clean.write.mode("overwrite").parquet(silver_path)
    logger.info(f"Salvo em: {silver_path}")


if __name__ == "__main__":
    clean()
