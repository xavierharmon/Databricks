"""Arguments shared by the runner and by every stage script.

Why a shared parser: a Job passes the same few settings to every task. Defining them once means a stage script run
by hand and the same stage run by the runner can never disagree about what `--catalog` means.
"""
import argparse


def add_common_args(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog to use (e.g. mlb_dev)")
    p.add_argument("--pipeline-root", required=True, help="folder with contracts/, transforms/, definitions/ (a Volume path)")
    p.add_argument("--data-root", required=True, help="folder with the landed files (fbl/...) and reports/ (a Volume path)")
    p.add_argument("--contracts-dir", help="override: where the contract YAML files are (default <pipeline-root>/contracts)")
    p.add_argument("--transforms-dir", help="override: where the gold transform SQL files are (default <pipeline-root>/transforms)")
    p.add_argument("--definitions-dir", help="override: where the metric view SQL files are (default <pipeline-root>/definitions)")
    p.add_argument("--landing-dir", help="override: folder holding the entity folders (default <data-root>/fbl)")
    p.add_argument("--run-id", default="", help="ties every row of one run together (default: a timestamp)")
    p.add_argument("--product", default="fbl", help="which dataProduct in the contracts to run")
    p.add_argument("--fail-on-rejects", default="true", choices=["true", "false"],
                   help="true: end the run in failure when rows were rejected or a contract is broken")
    p.add_argument("--local-mode", action="store_true", help="tests on open-source Spark only")
    return p
