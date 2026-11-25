"""The tix generator must be repeatable, and its answer key must agree with an independent re-derivation from the files."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import generate_tix_data as g  # noqa: E402

ARGS = ["--events", "20", "--persons", "90", "--batches", "4", "--history-days", "60", "--history-rate", "10", "--batch-rate", "25", "--seed", "3"]


def gen(out, *extra):
    g.main(["--out", str(out)] + ARGS + list(extra))
    return json.load(open(out / "answer_key" / "answer_key.json"))


def test_same_seed_gives_identical_answer_key(tmp_path):
    a, b = gen(tmp_path / "a"), gen(tmp_path / "b")
    assert a == b


def test_independent_checker_agrees_with_the_answer_key(tmp_path):
    gen(tmp_path / "d")
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "check_tix_data.py"), str(tmp_path / "d")], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_chain_break_is_planted_only_when_asked(tmp_path):
    clean = gen(tmp_path / "c")
    assert not any("TXN-E10_broken_ownership_chain" in v for v in clean["planted_counts_by_batch"].values())
    dirty = gen(tmp_path / "x", "--chain-break-batch", "2")
    assert dirty["planted_counts_by_batch"]["2"]["TXN-E10_broken_ownership_chain"] == 2


def test_bps_rounding_is_half_up_in_whole_numbers():
    assert g.bp(1000, 1200) == 120 and g.bp(1001, 1200) == 120 and g.bp(1004, 1250) == 126


def test_identity_reference_is_accurate_enough(tmp_path):
    gen(tmp_path / "i")
    import identity_reference as ir
    R = ir.resolve(str(tmp_path / "i"))
    total = ir.score(str(tmp_path / "i"), R)
    known = total["correct"] + total["WRONG"]
    assert known > 0 and total["correct"] / known > 0.97
