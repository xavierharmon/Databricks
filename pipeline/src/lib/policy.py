"""Quality-action policy: per-environment overrides of what a rule DOES when it is violated.

The contract stays the source of truth for the rule (what it checks, and the action it declares). This file adds ONE thin layer
on top: "in this environment, for this rule, use a different action, for this reason, approved by this person". Nothing here
changes what a rule checks, and nothing here can invent a rule.

Design choices (each one is tested in tests/test_policy.py):
  * Pure Python, no Spark. Lint, CI and the runner all use this one module, so they cannot disagree.
  * Only silver rules whose declared action is fail / quarantine / warn can be overridden. Gold rules always ASSERT (they
    stop the build whatever their action says), so an override there would only mislead. dedupe / standardize are mechanical
    corrections, not decisions about letting bad data through.
  * Strictness order: warn < quarantine < fail. Moving DOWN that ladder lets bad data further through, so it is a downgrade.
  * A downgrade needs a reason, an approver and an expiry date. After the date the runner ignores it and goes back to the
    contract's action (fail safe), and says so.
  * No --env means NO overrides are applied. A run that forgot to say where it is can never pick up another environment's leniency.
  * Every override evaluated in a run is written to governance.dq_action_overrides, and every new version of this file to
    governance.dq_policy_history. dq_rule_results.action already records the action that actually ran.
"""
import hashlib
import json
import re
from datetime import date, datetime

OVERRIDABLE = ("warn", "quarantine", "fail")          # ordered from most lenient to strictest
RANK = {a: i for i, a in enumerate(OVERRIDABLE, 1)}
ENVS = ("dev", "staging", "prod")
MAX_DOWNGRADE_DAYS = 90
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")

OVERRIDES_DDL = ("run_id STRING, applied_at TIMESTAMP, env STRING, rule_id STRING, layer STRING, table_name STRING, "
                 "rule_type STRING, declared_action STRING, effective_action STRING, direction STRING, status STRING, "
                 "reason STRING, approved_by STRING, ticket STRING, expires STRING, policy_version STRING, policy_checksum STRING")
HISTORY_DDL = "checksum STRING, version STRING, loaded_at TIMESTAMP, content STRING"


# --------------------------------------------------------------------------- loading
def empty_policy():
    return {"version": "0.0.0", "overrides": []}


def parse_policy(text, parse_yaml):
    doc = parse_yaml(text) if (text or "").strip() else None
    return doc if isinstance(doc, dict) else empty_policy()


def load_policy(path, parse_yaml):
    """A missing file is an empty policy (nothing overridden), not an error."""
    try:
        with open(path) as fh:
            return parse_policy(fh.read(), parse_yaml)
    except FileNotFoundError:
        return empty_policy()


def checksum(policy):
    return hashlib.sha256(json.dumps(policy, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _day(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _overrides(policy):
    return [o for o in (policy or {}).get("overrides") or [] if isinstance(o, dict)]


def direction(declared, effective):
    if RANK[effective] < RANK[declared]:
        return "downgrade"
    return "upgrade" if RANK[effective] > RANK[declared] else "same"


# --------------------------------------------------------------------------- validation
def validate(policy, rules, today=None):
    """Return [(severity, reference, message)]. `rules` are compiled contract rules (lib.contracts.compile_all_rules)."""
    today = today or date.today()
    out = []
    ver = str((policy or {}).get("version", ""))
    if not SEMVER.match(ver):
        out.append(("error", "policy", f"version must look like 1.2.3 (got {ver!r})"))
    by_id = {r["rule_id"]: r for r in rules}
    seen = set()
    for i, o in enumerate(_overrides(policy), 1):
        ref = o.get("rule") or f"override #{i}"
        err = lambda m: out.append(("error", ref, m))      # noqa: E731
        warn = lambda m: out.append(("warning", ref, m))   # noqa: E731
        envs = o.get("env")
        if not isinstance(envs, list) or not envs or any(e not in ENVS for e in envs):
            err(f"env must be a non-empty list drawn from {list(ENVS)}")
            envs = []
        action = o.get("action")
        if action not in OVERRIDABLE:
            err(f"action must be one of {list(OVERRIDABLE)} (got {action!r})")
        for need in ("reason", "approved_by"):
            if not str(o.get(need) or "").strip():
                err(f"{need} is required: every override must say why and who approved it")
        rule = by_id.get(o.get("rule"))
        if rule is None:
            err("no such rule in the contracts")
            continue
        if rule["layer"] != "silver":
            err("only silver rules can be overridden: gold rules always assert, so an override would change nothing")
            continue
        if rule["action"] not in OVERRIDABLE:
            err(f"declared action {rule['action']!r} is a mechanical correction and cannot be overridden")
            continue
        for e in envs:
            if (rule["rule_id"], e) in seen:
                err(f"more than one override for {e}")
            seen.add((rule["rule_id"], e))
        if action not in OVERRIDABLE:
            continue
        d = direction(rule["action"], action)
        if d == "same":
            err(f"action {action!r} equals the contract's action, so this override does nothing: delete it")
        if d == "downgrade":
            exp = _day(o.get("expires"))
            if exp is None:
                err("a downgrade needs `expires: YYYY-MM-DD`: leniency must have an end date")
            elif exp < today:
                warn(f"expired on {exp}: the runner ignores it and uses the contract's action. Delete it, or re-approve with a new date")
            elif (exp - today).days > MAX_DOWNGRADE_DAYS:
                warn(f"expires {exp}, more than {MAX_DOWNGRADE_DAYS} days away: long-lived leniency should become a contract change")
    return out


# --------------------------------------------------------------------------- applying
def apply(rules, policy, env, run_id, now=None, today=None):
    """Return (effective rules, audit rows for governance.dq_action_overrides). Never mutates `rules`."""
    now, today = now or datetime.now(), today or date.today()
    if not env:
        return [dict(r) for r in rules], []
    ver, chk = str((policy or {}).get("version", "")), checksum(policy)
    wanted = {}
    for o in _overrides(policy):
        if env in (o.get("env") or []):
            wanted[o.get("rule")] = o
    effective, rows = [], []
    for r in rules:
        r = dict(r)
        o = wanted.get(r["rule_id"])
        if o and r["layer"] == "silver" and r["action"] in OVERRIDABLE and o.get("action") in OVERRIDABLE:
            declared, new = r["action"], o["action"]
            exp = _day(o.get("expires"))
            expired = exp is not None and exp < today
            status = "expired_ignored" if expired else "applied"
            if not expired:
                r["action"] = new
            rows.append((run_id, now, env, r["rule_id"], r["layer"], r["table_name"], r["rule_type"], declared,
                         r["action"], direction(declared, new), status, str(o.get("reason", "")), str(o.get("approved_by", "")),
                         str(o.get("ticket", "") or ""), str(exp or ""), ver, chk))
        effective.append(r)
    return effective, rows


# --------------------------------------------------------------------------- change classification
def _effective_map(policy, rules):
    declared = {r["rule_id"]: r["action"] for r in rules if r["layer"] == "silver" and r["action"] in OVERRIDABLE}
    out = {}
    for rule_id, action in declared.items():
        for env in ENVS:
            out[(rule_id, env)] = (action, None, None)
    for o in _overrides(policy):
        for env in o.get("env") or []:
            key = (o.get("rule"), env)
            if key in out and o.get("action") in OVERRIDABLE:
                out[key] = (o["action"], _day(o.get("expires")), o)
    return out, declared


def classify(old, new, rules):
    """Compare two policies against the same contracts. Returns (changes, problems).

    A change is BREAKING when, for some rule and environment, the new policy lets more through than the old one did:
    a new downgrade, a removed upgrade, or a downgrade whose end date was pushed out. Breaking changes need a MAJOR version bump;
    any change at all needs the version to change, so the file's own history is an audit trail.
    """
    om, declared = _effective_map(old, rules)
    nm, _ = _effective_map(new, rules)
    changes = []
    for key in sorted(set(om) | set(nm)):
        (oa, oexp, oo), (na, nexp, no) = om.get(key, (None, None, None)), nm.get(key, (None, None, None))
        if oa is None or na is None:
            continue
        rule_id, env = key
        if RANK[na] < RANK[oa]:
            changes.append((True, f"{rule_id} [{env}]: {oa} -> {na} (lets more through)"))
        elif RANK[na] > RANK[oa]:
            changes.append((False, f"{rule_id} [{env}]: {oa} -> {na} (stricter)"))
        elif direction(declared[rule_id], na) == "downgrade" and oexp and nexp and nexp > oexp:
            changes.append((True, f"{rule_id} [{env}]: downgrade extended from {oexp} to {nexp}"))
        elif (oo or no) and (oo or {}) != (no or {}):
            changes.append((False, f"{rule_id} [{env}]: reason / approver / ticket / date changed"))
    breaking = any(b for b, _ in changes)
    problems = []
    ov, nv = str((old or {}).get("version", "0.0.0")), str((new or {}).get("version", "0.0.0"))
    if changes or checksum(old) != checksum(new):
        if SEMVER.match(ov) and SEMVER.match(nv):
            if ov == nv:
                problems.append(f"policy changed but version is still {nv}: bump it so the change is visible in the history")
            elif breaking and int(nv.split(".")[0]) <= int(ov.split(".")[0]):
                problems.append(f"breaking change (lets more bad data through) needs a MAJOR bump: {ov} -> {nv} is not one")
    return changes, problems
