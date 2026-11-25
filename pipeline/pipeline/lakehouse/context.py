"""RunContext: the single object every step receives.

Why a context object: each stage is a small function `run(ctx, entity)`. Passing one object (instead of ten arguments)
keeps stage signatures stable when you add a setting, and makes every stage easy to call from a test.
"""
import os
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class RunContext:
    catalog: str
    run_id: str
    pipeline_root: str                 # folder holding contracts/, transforms/, definitions/ (and code/)
    data_root: str                     # folder holding the landed files and reports/
    contracts_dir: str
    transforms_dir: str
    definitions_dir: str
    landing_dir: str
    reports_dir: str
    product: str = "fbl"
    local_mode: bool = False           # tests on open-source Spark: no USE CATALOG, no metric view creation
    env: str = ""                      # dev | staging | prod. Empty = apply no action overrides (see lib/policy.py)
    policy_path: str = ""              # the data-quality action policy file
    fail_on_rejects: bool = True
    spark: object = None
    contracts: object = None           # lib.contracts.ContractSet, filled by the runner
    definitions: dict = field(default_factory=dict)   # metric view SQL text keyed by file name
    semantic_executor_factory: object = None          # tests only: stand-in for MEASURE() queries
    log: object = print

    def use_catalog(self):
        if not self.local_mode:
            self.spark.sql(f"USE CATALOG {self.catalog}")


def make_context(args, spark=None) -> RunContext:
    root = args.pipeline_root.rstrip("/")
    data = args.data_root.rstrip("/")
    return RunContext(
        catalog=args.catalog,
        run_id=args.run_id or datetime.now().strftime("%Y%m%d-%H%M%S"),
        pipeline_root=root,
        data_root=data,
        contracts_dir=args.contracts_dir or f"{root}/contracts",
        transforms_dir=args.transforms_dir or f"{root}/transforms",
        definitions_dir=args.definitions_dir or f"{root}/definitions",
        landing_dir=args.landing_dir or f"{data}/fbl",
        reports_dir=f"{data}/reports",
        product=args.product,
        local_mode=args.local_mode,
        env=args.env,
        policy_path=args.policy_file or f"{root}/policy/dq_actions.yml",
        fail_on_rejects=(str(args.fail_on_rejects).lower() == "true"),
        spark=spark,
    )
