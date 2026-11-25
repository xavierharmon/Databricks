"""run_tool.py: the launcher for the helper tools (argument handling only; the tools have their own tests)."""
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("run_tool", ROOT / "run_tool.py")
rt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rt)


def test_code_dir_is_removed_from_the_tool_arguments():
    val, rest = rt._take(["--code-dir", "/v/code", "generate", "--out", "/x"], "--code-dir")
    assert val == "/v/code" and rest == ["generate", "--out", "/x"]


def test_equals_form_and_absence():
    assert rt._take(["--code-dir=/v/code", "check", "/d"], "--code-dir") == ("/v/code", ["check", "/d"])
    assert rt._take(["check", "/d"], "--code-dir") == (None, ["check", "/d"])


def test_a_missing_zip_gives_a_helpful_message(tmp_path):
    rt.__file__ = str(tmp_path / "run_tool.py")          # pretend the launcher sits in an empty folder
    try:
        rt._make_importable(str(tmp_path))
    except SystemExit as e:
        assert "--code-dir" in str(e)
    else:
        raise AssertionError("expected SystemExit")
