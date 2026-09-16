import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType, BooleanType
)
from src.cleaning.clean import apply_dq_flags, remove_duplicates


# ---------------------------------------------------------------------------
# FIXTURE — dados brutos (strings, como chegam do CSV)
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_view(spark: SparkSession):
    """Registra um mini-CSV como view 'raw_transactions' para os testes de apply_dq_flags."""
    schema = StructType([
        StructField("timestamp",          StringType(), True),
        StructField("sending_address",    StringType(), True),
        StructField("receiving_address",  StringType(), True),
        StructField("amount",             StringType(), True),
        StructField("transaction_type",   StringType(), True),
        StructField("location_region",    StringType(), True),
        StructField("ip_prefix",          StringType(), True),
        StructField("login_frequency",    StringType(), True),
        StructField("session_duration",   StringType(), True),
        StructField("purchase_pattern",   StringType(), True),
        StructField("age_group",          StringType(), True),
        StructField("risk_score",         StringType(), True),
        StructField("anomaly",            StringType(), True),
    ])
    data = [
        # ts,        sender,  receiver, amount, type,      region,         ip,  freq, dur, pattern,  age,   risk,   anomaly
        ("1609459200", "0xAAA", "0x111", "100.0",  "sale",     "North America", "10.0", "3", "60", "online",   "18-25", "30.0",  "low_risk"),
        ("1609459200", "0xBBB", "0x222", "none",   "purchase", "0",            "10.0", "2", "45", "in-store", "26-35", "45.0",  "low_risk"),
        ("1609459300", "0xCCC", "0x333", "50.0",   "transfer", "Europe",       "10.0", "5", "90", "online",   "36-45", "none",  "high_risk"),
        ("1609459400", "0xDDD", "0x444", "200.0",  "scam",     "Asia",         "10.0", "8", "120", "online",  "46-55", "60.0",  "high_risk"),
    ]
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("raw_transactions")
    return df


@pytest.fixture
def flagged_view(spark: SparkSession, raw_view):
    """Aplica DQ flags e registra 'flagged_transactions' para os testes de remove_duplicates."""
    df = apply_dq_flags(spark, "raw_transactions")
    df.createOrReplaceTempView("flagged_transactions")
    return df


# ---------------------------------------------------------------------------
# TESTES — apply_dq_flags
# ---------------------------------------------------------------------------

def test_apply_dq_flags_flag_amount(spark: SparkSession, raw_view):
    result = apply_dq_flags(spark, "raw_transactions")
    rows = result.select("dq_flag_amount").collect()

    flagged = [r["dq_flag_amount"] for r in rows]
    assert sum(flagged) == 1           # apenas 1 registro com amount="none"
    assert flagged[0] == False         # linha 0 tem "100.0" → válida
    assert flagged[1] == True          # linha 1 tem "none" → flagada


def test_apply_dq_flags_flag_risk_score(spark: SparkSession, raw_view):
    result = apply_dq_flags(spark, "raw_transactions")
    rows = result.select("dq_flag_risk_score").collect()

    flagged = [r["dq_flag_risk_score"] for r in rows]
    assert sum(flagged) == 1
    assert flagged[2] == True          # linha 2 tem risk_score="none"
    assert flagged[0] == False


def test_apply_dq_flags_flag_region(spark: SparkSession, raw_view):
    result = apply_dq_flags(spark, "raw_transactions")
    rows = result.select("dq_flag_region").collect()

    flagged = [r["dq_flag_region"] for r in rows]
    assert sum(flagged) == 1
    assert flagged[1] == True          # linha 1 tem location_region="0"
    assert flagged[0] == False


def test_apply_dq_flags_converte_amount_para_double(spark: SparkSession, raw_view):
    result = apply_dq_flags(spark, "raw_transactions")
    schema_map = {f.name: f.dataType for f in result.schema.fields}

    assert isinstance(schema_map["amount"], DoubleType)

    row_none = result.collect()[1]
    assert row_none["amount"] is None   # "none" → null


def test_apply_dq_flags_converte_timestamp(spark: SparkSession, raw_view):
    result = apply_dq_flags(spark, "raw_transactions")
    schema_map = {f.name: f.dataType for f in result.schema.fields}

    assert isinstance(schema_map["timestamp"], TimestampType)


# ---------------------------------------------------------------------------
# TESTES — remove_duplicates
# ---------------------------------------------------------------------------

def test_remove_duplicates_mantem_registro_fraude(spark: SparkSession):
    """
    Dois registros com a mesma chave natural (sender + receiver + timestamp):
    um 'transfer' e um 'scam'. O 'scam' (fraude) deve sobreviver.
    """
    schema = "timestamp TIMESTAMP, timestamp_unix BIGINT, sending_address STRING, " \
             "receiving_address STRING, amount DOUBLE, transaction_type STRING, " \
             "location_region STRING, ip_prefix DOUBLE, login_frequency INT, " \
             "session_duration INT, purchase_pattern STRING, age_group STRING, " \
             "risk_score DOUBLE, anomaly STRING, dq_flag_amount BOOLEAN, " \
             "dq_flag_risk_score BOOLEAN, dq_flag_region BOOLEAN"

    from datetime import datetime
    ts = datetime(2021, 1, 1, 0, 0, 0)
    data = [
        (ts, 1609459200, "0xAAA", "0x111", 100.0, "transfer", "North America", 10.0, 3, 60, "online", "18-25", 30.0, "low_risk",  False, False, False),
        (ts, 1609459200, "0xAAA", "0x111", 100.0, "scam",     "North America", 10.0, 3, 60, "online", "18-25", 80.0, "high_risk", False, False, False),
    ]
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("flagged_transactions")

    result = remove_duplicates(spark, "flagged_transactions")
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["transaction_type"] == "scam"


def test_remove_duplicates_sem_duplicatas_nao_remove_nada(spark: SparkSession):
    """Registros com chaves distintas: nenhum deve ser removido."""
    schema = "timestamp TIMESTAMP, timestamp_unix BIGINT, sending_address STRING, " \
             "receiving_address STRING, amount DOUBLE, transaction_type STRING, " \
             "location_region STRING, ip_prefix DOUBLE, login_frequency INT, " \
             "session_duration INT, purchase_pattern STRING, age_group STRING, " \
             "risk_score DOUBLE, anomaly STRING, dq_flag_amount BOOLEAN, " \
             "dq_flag_risk_score BOOLEAN, dq_flag_region BOOLEAN"

    from datetime import datetime
    data = [
        (datetime(2021, 1, 1), 1609459200, "0xAAA", "0x111", 100.0, "transfer", "North America", 10.0, 3, 60, "online", "18-25", 30.0, "low_risk", False, False, False),
        (datetime(2021, 1, 2), 1609545600, "0xBBB", "0x222", 200.0, "purchase", "Europe",        10.0, 2, 45, "online", "26-35", 45.0, "low_risk", False, False, False),
    ]
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("flagged_transactions")

    result = remove_duplicates(spark, "flagged_transactions")

    assert result.count() == 2
