"""Contract-driven lakehouse pipeline: scripts and a runner (no notebooks).

Layout
  lakehouse/context.py          RunContext: everything a step needs (spark, paths, contracts, run id)
  lakehouse/cli.py              the command-line arguments shared by the runner and every stage script
  lakehouse/contract_store.py   reads the ODCS YAML contracts (and metric view definitions) from a folder
  lakehouse/governance.py       creates governance tables, publishes contracts, writes the step log
  lakehouse/plan.py             derives the execution order from the contracts themselves
  lakehouse/stages/bronze.py    one entity per call: file -> bronze table, contract-checked
  lakehouse/stages/silver.py    one entity per call: type, clean, validate, quarantine
  lakehouse/stages/gold.py      build + audit into a staging schema, then publish
  lakehouse/stages/semantic.py  create a metric view and test it against its contract
  lakehouse/stages/report.py    summarise the run and decide whether it counts as failed
  lakehouse/runner.py           runs the plan, stops on a hard failure, always reports
The shared engine and contract library live in the sibling package `lib` (dq_engine, contracts, contract_checks).
"""
