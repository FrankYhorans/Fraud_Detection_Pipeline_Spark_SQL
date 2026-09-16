from pyspark.sql import SparkSession, DataFrame
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


def calc_top_addresses(spark: SparkSession, view_name: str = "silver_transactions") -> DataFrame:
    """
    Top 3 receiving_address em transações 'sale', usando o timestamp mais recente
    de cada endereço e excluindo flags DQ, via Spark SQL.

    Regra de negócio: se um endereço aparece N vezes, mantém a ocorrência
    mais recente (não a de maior amount).
    """
    return spark.sql(f"""
        WITH latest_per_address AS (
            SELECT
                receiving_address,
                amount,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY receiving_address
                    ORDER BY timestamp DESC
                ) AS rn
            FROM {view_name}
            WHERE dq_flag_amount = false
              AND transaction_type = 'sale'
        )
        SELECT receiving_address, amount, timestamp
        FROM latest_per_address
        WHERE rn = 1
        ORDER BY amount DESC
        LIMIT 3
    """)


def run(
    silver_path: str = "s3a://silver/transactions_clean.parquet",
    gold_path: str = "s3a://gold/top_addresses.parquet",
) -> DataFrame:
    spark = get_spark_session("top_addresses")

    df_silver = spark.read.parquet(silver_path)
    logger.info(f"{df_silver.count():,} registros lidos da camada Silver.")

    df_silver.createOrReplaceTempView("silver_transactions")
    result = calc_top_addresses(spark)

    result.write.mode("overwrite").parquet(gold_path)
    logger.info(f"Salvo em: {gold_path}")

    rows = result.collect()
    logger.info(f"Top {len(rows)} endereços receptores:")
    for r in rows:
        logger.info(f"  {r['receiving_address']} | amount: {r['amount']:,.1f} | timestamp: {r['timestamp']}")

    return result


if __name__ == "__main__":
    result = run()
    result.show(truncate=False)
