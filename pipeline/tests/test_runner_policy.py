"""Runner behaviour: failure policies, the always-report promise, and exit codes.

No JVM starts here. The runner is called through its real main() with fake step functions swapped in, so we test the
runner's decisions (order, skipping, reporting, exit code) and not Spark. The runner module imports pyspark at the top, so
these tests need pyspark INSTALLED (hence the `spark` marker) but never create a session.
"""
import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from lakehouse import runner  # noqa: E402


class Harness:
    """Records what the runner did, with every real stage replaced by a fake."""

    def __init__(self, monkeypatch, pipeline_root, fail=()):
        self.ran, self.logged, self.reported = [], [], []
        self.fail = set(fail)
        monkeypatch.setattr(runner, "ensure_objects", lambda ctx: None)
        monkeypatch.setattr(runner, "publish_contracts", lambda ctx: None)
        monkeypatch.setattr(runner, "log_step", lambda ctx, name, status, started, note="": self.logged.append((name, status)))
        monkeypatch.setattr(runner.report, "run", lambda ctx, failure: (self.reported.append(True), failure)[1])
        fakes = {kind: self._fake(kind) for kind in runner.RUNNERS}
        monkeypatch.setattr(runner, "RUNNERS", fakes)
        self.argv_base = ["--catalog", "t", "--pipeline-root", str(pipeline_root), "--data-root", str(pipeline_root / "data"),
                          "--product", "fbl", "--local-mode", "--fail-on-rejects", "false"]

    def _fake(self, kind):
        def run(ctx, step):
            key = step.key
            self.ran.append(key)
            if key in self.fail:
                raise RuntimeError(f"planted failure in {key}")
        return run

    def go(self, *extra):
        # spark=object(): the runner only needs "something that is not None"; every stage that would use it is faked.
        return runner.main(self.argv_base + list(extra), spark=object())

    def status(self, key):
        return dict(self.logged).get(key)


def test_all_green_returns_zero_and_reports(monkeypatch, pipeline_root):
    h = Harness(monkeypatch, pipeline_root)
    assert h.go() == 0
    assert "fbl:gold-publish" in h.ran or "gold-publish" in h.ran
    assert h.reported == [True]


def test_stop_policy_skips_everything_after_the_first_failure(monkeypatch, pipeline_root):
    h = Harness(monkeypatch, pipeline_root, fail={"bronze:batting"})
    assert h.go("--failure-policy", "stop") == 1
    assert h.status("bronze:batting") == "failed"          # the failure really happened (not skipped by accident)
    assert h.ran == ["bronze:batting"]                      # nothing else was even attempted
    assert h.status("bronze:player") == "skipped" and h.status("silver:team") == "skipped"
    assert h.reported == [True], "the report must still run after a failure"


def test_isolate_policy_skips_only_dependents(monkeypatch, pipeline_root):
    h = Harness(monkeypatch, pipeline_root, fail={"bronze:batting"})
    assert h.go("--failure-policy", "isolate") == 1
    assert h.status("bronze:batting") == "failed"
    assert h.status("silver:batting") == "skipped"          # depends on the failed bronze:batting
    assert "bronze:player" in h.ran and "bronze:team" in h.ran      # independent, so they still ran
    assert "silver:team" in h.ran                           # independent of batting
    assert h.reported == [True]


def test_isolate_is_not_the_same_as_stop(monkeypatch, pipeline_root):
    """The two policies differ in exactly one situation; make sure a refactor cannot collapse them."""
    a, b = Harness(monkeypatch, pipeline_root, fail={"bronze:batting"}), None
    a.go("--failure-policy", "stop")
    stop_ran = list(a.ran)
    b = Harness(monkeypatch, pipeline_root, fail={"bronze:batting"})
    b.go("--failure-policy", "isolate")
    assert len(b.ran) > len(stop_ran)


def test_blocked_unit_rules():
    deps = {"b": ["a"], "c": []}
    assert runner.blocked("c", deps, {"a"}, "isolate", False, True) is False
    assert runner.blocked("b", deps, {"a"}, "isolate", False, True) is True
    assert runner.blocked("c", deps, {"a"}, "stop", False, True) is True
    assert runner.blocked("c", deps, set(), "isolate", True, True) is True     # a setup failure blocks everything


def test_unknown_step_is_rejected_loudly(monkeypatch, pipeline_root):
    h = Harness(monkeypatch, pipeline_root)
    with pytest.raises(SystemExit, match="unknown step"):
        h.go("--steps", "silver:does_not_exist")
