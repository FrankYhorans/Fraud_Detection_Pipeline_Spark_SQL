from pyspark.sql import SparkSession, DataFrame
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


def calc_risk_region(spark: SparkSession, view_name: str = "silver_transactions") -> DataFrame:
    """Calcula média de risk_score por região, excluindo flags DQ, via Spark SQL."""
    return spark.sql(f"""
        SELECT
            location_region,
            ROUND(AVG(risk_score), 4) AS avg_risk_score
        FROM {view_name}
        WHERE dq_flag_risk_score = false
          AND dq_flag_region = false
        GROUP BY location_region
        ORDER BY avg_risk_score DESC
    """)


def run(
    silver_path: str = "s3a://silver/transactions_clean.parquet",
    gold_path: str = "s3a://gold/risk_region.parquet",
) -> DataFrame:
    spark = get_spark_session("risk_region")

    df_silver = spark.read.parquet(silver_path)
    logger.info(f"{df_silver.count():,} registros lidos da camada Silver.")

    df_silver.createOrReplaceTempView("silver_transactions")
    result = calc_risk_region(spark)

    result.write.mode("overwrite").parquet(gold_path)
    logger.info(f"Salvo em: {gold_path}")

    rows = result.collect()
    logger.info(f"Resultado risk_region ({len(rows)} regiões):")
    for r in rows:
        logger.info(f"  {r['location_region']:<20}  avg_risk_score: {r['avg_risk_score']}")

    return result


if __name__ == "__main__":
    result = run()
    result.show()
