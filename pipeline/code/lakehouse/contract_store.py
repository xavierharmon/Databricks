"""Read the contracts (ODCS YAML) and metric view SQL from folders.

Why read YAML directly: the contract files ARE the source of truth, so the pipeline reads exactly what you uploaded.
There is no intermediate bundle to forget to rebuild. (PyYAML ships in the Databricks serverless base environment.)
"""
import glob
import os

from lib.contracts import ContractSet, parse_yaml
from lakehouse.errors import StageFailure


def load_contracts(contracts_dir: str) -> ContractSet:
    if not os.path.isdir(contracts_dir):
        raise StageFailure(f"contracts folder not found: {contracts_dir}. Upload your contract YAML files there.")
    files = sorted(glob.glob(os.path.join(contracts_dir, "**", "*.y*ml"), recursive=True))
    if not files:
        raise StageFailure(f"no contract YAML files found in {contracts_dir}")
    contracts = []
    for f in files:
        with open(f) as fh:
            try:
                doc = parse_yaml(fh.read())
            except Exception as e:  # noqa: BLE001
                raise StageFailure(f"{f} is not valid YAML: {str(e)[:200]}")
        if not isinstance(doc, dict) or "id" not in doc:
            raise StageFailure(f"{f} does not look like a data contract (no top-level `id`)")
        contracts.append(doc)
    return ContractSet(contracts)


def load_definitions(definitions_dir: str) -> dict:
    """Metric view SQL files keyed by file name. A missing folder is fine until a semantic contract needs it."""
    out = {}
    if os.path.isdir(definitions_dir):
        for f in glob.glob(os.path.join(definitions_dir, "*.sql")):
            with open(f) as fh:
                out[os.path.basename(f)] = fh.read()
    return out
