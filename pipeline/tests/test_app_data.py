"""The app's data layer, against a fake SQL runner (no warehouse, no network)."""
import sys
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import data  # noqa: E402


def test_every_query_reads_only_published_gold_ml_or_governance_tables():
    for name, q in data.QUERIES.items():
        refs = [t for t in q.replace("\n", " ").split() if t.startswith(data.CATALOG + ".")]
        assert refs and all(r.split(".")[1] in ("gold", "ml", "governance") for r in refs), name
        assert "gold_stage" not in q and "silver" not in q


def test_run_query_passes_the_sql_to_the_runner_and_rejects_unknown_names():
    seen = []
    out = data.run_query("be_backs", lambda s: seen.append(s) or pd.DataFrame({"be_backs": [0], "purchases": [5]}))
    assert "tix_fact_purchase_journey" in seen[0] and out["purchases"].iloc[0] == 5
    with pytest.raises(KeyError):
        data.run_query("nope", lambda s: None)


def test_kpis_add_fee_revenue_and_profit():
    df = pd.DataFrame({"fee_revenue": ["10.50", "4.25"], "owned_inventory_profit": ["0.00", "3.00"]})
    assert data.kpis(df) == {"fee_revenue": 14.75, "owned_inventory_profit": 3.0, "total_earnings": 17.75}
    assert data.kpis(df.iloc[0:0])["total_earnings"] == 0.0
