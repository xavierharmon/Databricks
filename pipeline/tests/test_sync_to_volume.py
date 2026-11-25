"""tools/sync_to_volume.py: copy, stamp, verify and refuse to copy what does not lint."""
import json
import shutil
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src")); sys.path.insert(0, str(ROOT / "tools"))
import sync_to_volume as S  # noqa: E402
from lakehouse import runner  # noqa: E402


def test_first_sync_copies_everything_and_writes_the_stamp_last(tmp_path):
    assert S.main(["--dest", str(tmp_path), "--commit", "abc123"]) == 0
    v = json.load(open(tmp_path / "VERSION.json"))
    assert v["commit"] == "abc123" and len(v["content_fingerprint"]) == 64 and v["files_new"] > 50
    assert (tmp_path / "code" / "lakehouse.zip").exists() and (tmp_path / "run_pipeline.py").exists()
    assert (tmp_path / "contracts" / "tix" / "silver" / "tix_silver_event.yaml").exists()
    assert (tmp_path / "definitions").is_dir() and not list((tmp_path / "definitions").glob("*/*"))     # flattened


def test_a_repeat_sync_without_a_commit_keeps_the_recorded_commit(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "find_commit", lambda repo, given: given or "unknown")      # inside a Git checkout the tool would otherwise read HEAD
    S.main(["--dest", str(tmp_path), "--commit", "abc123"])
    S.main(["--dest", str(tmp_path)])
    assert json.load(open(tmp_path / "VERSION.json"))["commit"] == "abc123"
    S.main(["--dest", str(tmp_path), "--commit", "def456"])                      # a new commit with identical files still updates the stamp
    assert json.load(open(tmp_path / "VERSION.json"))["commit"] == "def456"


def test_second_sync_changes_nothing_and_verify_agrees(tmp_path):
    S.main(["--dest", str(tmp_path)])
    first = json.load(open(tmp_path / "VERSION.json"))
    assert S.main(["--dest", str(tmp_path)]) == 0
    second = json.load(open(tmp_path / "VERSION.json"))
    assert second == first                       # nothing to do, so the stamp is not rewritten
    assert S.main(["--dest", str(tmp_path), "--verify"]) == 0


def test_verify_fails_when_the_runtime_copy_was_edited_by_hand(tmp_path, capsys):
    S.main(["--dest", str(tmp_path)])
    f = tmp_path / "contracts" / "tix" / "silver" / "tix_silver_event.yaml"
    f.write_text(f.read_text() + "\n# edited by hand\n")
    assert S.main(["--dest", str(tmp_path), "--verify"]) == 1
    assert "tix_silver_event.yaml" in capsys.readouterr().out
    assert S.main(["--dest", str(tmp_path)]) == 0 and S.main(["--dest", str(tmp_path), "--verify"]) == 0      # a sync repairs it


def test_dry_run_writes_nothing(tmp_path):
    assert S.main(["--dest", str(tmp_path / "d"), "--dry-run"]) == 0
    assert not (tmp_path / "d").exists()


def test_a_contract_that_fails_lint_blocks_the_whole_sync(tmp_path):
    repo = tmp_path / "repo"
    (repo / "contracts").mkdir(parents=True)
    bad = yaml.safe_load(open(ROOT / "contracts" / "tix" / "silver" / "tix_silver_venue.yaml"))
    bad["customProperties"] = [c for c in bad["customProperties"] if c["property"] != "owner"]     # an owner is required
    (repo / "contracts" / "x.yaml").write_text(yaml.safe_dump(bad))
    cfg = {"dest_root": str(tmp_path / "dest"), "zip": "code/lakehouse.zip", "sync": [{"name": "c", "src": "contracts", "dst": "contracts", "kind": "contracts"}]}
    (repo / "deploy").mkdir()
    (repo / "deploy" / "sync.yml").write_text(yaml.safe_dump(cfg))
    assert S.main(["--repo-root", str(repo)]) == 1
    assert not (tmp_path / "dest").exists()


def test_two_sources_with_one_name_and_different_content_are_refused(tmp_path):
    repo = tmp_path / "repo"
    for d, text in (("a", "select 1"), ("b", "select 2")):
        (repo / d).mkdir(parents=True)
        (repo / d / "mv.sql").write_text(text)
    cfg = {"dest_root": str(tmp_path / "dest"), "zip": "code/lakehouse.zip", "sync": [{"name": "d", "src": ["a", "b"], "dst": "definitions", "flatten": True}]}
    try:
        S.gather(repo, cfg)
    except SystemExit as e:
        assert "mv.sql" in str(e)
    else:
        raise AssertionError("expected a refusal")


def test_runner_reports_which_code_it_is_running(tmp_path):
    assert "no VERSION.json" in runner.version_note(str(tmp_path))
    (tmp_path / "VERSION.json").write_text(json.dumps({"content_fingerprint": "f" * 64, "commit": "abcdef1234567890", "synced_at": "2026-11-24T10:00:00Z"}))
    note = runner.version_note(str(tmp_path))
    assert "ffffffffffff" in note and "abcdef123456" in note and "2026-11-24" in note
