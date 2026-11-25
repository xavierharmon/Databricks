"""Tier B: needs Spark. Start small; grow toward the golden numbers and WAP."""
import pytest

pytestmark = pytest.mark.spark


def test_spark_session_works(spark):
    assert spark.sql("select 1 as x").collect()[0]["x"] == 1


def test_decimal_money_is_exact(spark):
    # Why DECIMAL(12,2): floats drift, decimals don't.
    v = spark.sql("select cast(0.10 as decimal(12,2)) * 3 as t").collect()[0]["t"]
    assert str(v) == "0.30"


@pytest.mark.skip(reason="TODO: needs runner/gold.py signatures. Failed audit => gold-publish copies nothing.")
def test_failed_audit_blocks_publish(spark):
    ...


@pytest.mark.skip(reason="TODO: needs the runner entry point. FBL: 46 rejected, gold 8/31/53.")
def test_fbl_golden_numbers(spark):
    ...
