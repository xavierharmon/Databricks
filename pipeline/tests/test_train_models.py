"""tools/train_models.py: the honesty rules (time split, immature rows, baseline) and that a real signal is learned."""
import sys
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("sklearn")
np = pytest.importorskip("numpy")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import train_models as T  # noqa: E402

SPEC = dict(T.SPECS["purchase_propensity"])


def frame(n=1200, signal=True, seed=1):
    rng = np.random.default_rng(seed)
    d = pd.DataFrame({
        "as_of_ts": pd.date_range("2026-01-01", periods=n, freq="h"),
        "days_to_event": rng.uniform(1, 90, n), "price_at_first_visit": rng.uniform(20, 200, n), "price_level_ratio": rng.uniform(0.7, 1.3, n),
        "prior_purchases": rng.integers(0, 5, n), "prior_visits": rng.integers(0, 20, n), "prior_visits_same_sport": rng.integers(0, 10, n),
        "prior_purchases_same_sport": rng.integers(0, 3, n), "tenure_days": rng.integers(0, 400, n),
        "has_favorite": rng.integers(0, 2, n), "favorite_matches_event": rng.integers(0, 2, n),
        "event_category": rng.choice(["sport", "concert"], n), "sport": rng.choice(["baseball", "football", "none"], n), "label_mature": 1})
    p = 1 / (1 + np.exp(-(0.5 * d["prior_purchases_same_sport"] + 1.0 * d["favorite_matches_event"] * d["has_favorite"] - 0.6 * (d["price_level_ratio"] - 1) * 5 - 1.5))) if signal else np.full(n, 0.3)
    d["label_bought_30d"] = (rng.random(n) < p).astype(int)
    return d


def test_time_split_never_mixes_time():
    tr, te = T.time_split(frame(), "as_of_ts")
    assert tr["as_of_ts"].max() < te["as_of_ts"].min() and abs(len(tr) / (len(tr) + len(te)) - 0.7) < 0.02


def test_a_real_signal_is_learned_and_beats_the_baseline():
    m, model = T.fit_evaluate(frame(), SPEC)
    assert model is not None and m["auc"] > 0.62 and m["auc"] > m["baseline_auc"] + 0.03


def test_pure_noise_is_not_mistaken_for_signal():
    m, _ = T.fit_evaluate(frame(signal=False), SPEC)
    assert m["auc"] < 0.58


def test_immature_rows_are_dropped_and_tiny_data_is_skipped():
    d = frame(300)
    d["label_mature"] = 0
    m, model = T.fit_evaluate(d, SPEC)
    assert model is None and "mature" in m["skipped"]


def test_single_class_labels_are_reported_not_trained():
    d = frame(400)
    d["label_bought_30d"] = 0
    m, model = T.fit_evaluate(d, SPEC)
    assert model is None and "single class" in m["skipped"]


def test_small_samples_use_the_steadier_linear_model():
    spec = T.SPECS["purchase_propensity"]
    m, model = T.fit_evaluate(frame(420), spec)           # 70 percent of 420 rows is below the 400-row threshold
    assert model is not None and m["model_kind"] == "logistic"
    m2, _ = T.fit_evaluate(frame(1200), spec)
    assert m2["model_kind"] == "boosted trees"
