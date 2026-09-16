from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="fraud_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["localiza", "fraud"],
)
def fraud_pipeline():

    @task()
    def clean():
        from src.cleaning.clean import clean as run_clean
        run_clean()

    @task()
    def risk_region():
        from src.transformation.risk_region import run as run_risk_region
        run_risk_region()

    @task()
    def top_addresses():
        from src.transformation.top_addresses import run as run_top_addresses
        run_top_addresses()

    @task()
    def dq_report():
        from src.data_quality.dq_report import run as run_report
        run_report()

    # Instancia as tasks e define dependências
    cleaned = clean()
    risk    = risk_region()
    top     = top_addresses()
    report  = dq_report()

    cleaned >> report >> [risk, top]    


fraud_pipeline()
