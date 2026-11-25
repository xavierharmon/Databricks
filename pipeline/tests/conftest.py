"""Shared pytest setup.

Tier A (no marker): plain Python, no Spark. Runs in seconds.
Tier B (@pytest.mark.spark): needs pyspark. Locally we build a session;
  inside Databricks we reuse the one that already exists.
"""
import os
import sys
from pathlib import Path

import pytest

PIPELINE_ROOT = Path(__file__).resolve().parents[1]   # the pipeline/ folder
# `lakehouse` lives in pipeline/, and `lib` lives in pipeline/src (the zip flattens it; a checkout does not).
for p in (PIPELINE_ROOT / "src", PIPELINE_ROOT):
    sys.path.insert(0, str(p))


@pytest.fixture(scope="session")
def pipeline_root() -> Path:
    return PIPELINE_ROOT


@pytest.fixture(scope="session")
def contract_set(pipeline_root):
    """The real contracts, loaded the same way the runner loads them."""
    from lakehouse.contract_store import load_contracts
    return load_contracts(str(pipeline_root / "contracts"))


@pytest.fixture(scope="session")
def spark():
    """One session for the whole test run. Only created if a test asks for it."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        yield SparkSession.builder.getOrCreate()      # reuse the cluster's session; never call .master() here
        return

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(PIPELINE_ROOT / ".spark-warehouse"))
    )
    # Plain open-source Spark is the default: it needs no downloads and matches the runner's --local-mode.
    # Set LAKEHOUSE_TEST_DELTA=1 to test against Delta (needs internet the first time, to fetch the Delta jar).
    if os.environ.get("LAKEHOUSE_TEST_DELTA") == "1":
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    session = builder.getOrCreate()
    yield session
    session.stop()