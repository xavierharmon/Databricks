# Data contracts: from source file to semantic view

A **data contract** is a written promise about a dataset: its columns, its quality rules, how fresh it is, who owns it, and what
changes are allowed. Here the contracts are not just documents. The pipeline **reads them from Databricks and enforces them**.

## What exists

| Layer | Contract files (`contracts/`) | What the contract promises | Enforced by |
| --- | --- | --- | --- |
| Source (bronze) | `source/fbl_source_team.yaml`, `_player`, `_batting` | The file has these columns and is not empty. Values are NOT promised valid. | bronze stage: columns present, not empty, schema conformance |
| Silver | `silver/fbl_silver_team.yaml`, `_player`, `_batting` | Typed, deduplicated, validated rows. Lists every rule, with what happens to a bad row. | DQ engine (rules compiled from the contract) + schema conformance. The typing SELECT is generated from it |
| Gold | `gold/fbl_gold_dim_team.yaml`, `_dim_player`, `_fact_batting` | NULL-free, unique keys, no orphans, row counts reconcile, refreshed daily | gold assertions + schema conformance + reconciliation |
| Semantic | `semantic/fbl_semantic_mv_fbl_batting.yaml`, `_mv_fbl_roster` | Fields and measures consumers can use, behaviour tests, freshness | semantic stage (interface, tests, SLA) |
| Semantic (Lahman) | `semantic/lahman_semantic_mv_batting.yaml`, `_mv_pitching`, `_mv_team_season` | Same, for the MLB metric views, with record-book golden values | same checks (run for product `lahman`) |

Lineage between them is in each contract's `dependsOn` (silver depends on source, gold on silver, semantic on gold).

## Anatomy of a contract (ODCS v3.1 style)

The files follow the Open Data Contract Standard v3.1 layout: `apiVersion`, `kind`, `id`, `version`, `status`, `schema[]` with `properties[]` and nested `quality[]`, `slaProperties[]`. Anything the standard has no field for (owner, layer, the engine's rule settings) goes in `customProperties`, which is the standard's sanctioned extension point. I modelled field names on the published ODCS v3.1 documentation, but I have not run an ODCS validator on these files. Check them against the version your organisation standardises on; `src/lib/contracts.py` is the only place that maps the format to behaviour.

```yaml
id: fbl.silver.player              # <product>.<layer>.<name>
version: 1.0.0                     # semantic version; breaking changes need a MAJOR bump
status: active                     # draft | active | deprecated
customProperties:
  - {property: layer, value: silver}
  - {property: owner, value: Data Engineering}
  - {property: dependsOn, value: fbl.source.player}
schema:
  - name: fbl_player
    physicalName: silver.fbl_player
    properties:
      - name: bats
        logicalType: string
        required: true                       # lint: a not_null rule must cover this
        customProperties:
          - {property: standardize, value: upper}   # drives the generated typing SQL
      - name: team_id
        relationships: [{to: fbl_team.team_id, type: foreignKey}]   # lint: a referential rule must exist
    quality:
      - name: PLR-004                          # rule id
        type: custom
        engine: fbl-dq-engine                  # ODCS extension: a rule only this pipeline understands
        description: Bats must be L, R or B
        implementation: {rule_type: allowed_values, columns: [bats], expression: "'L','R','B'", action: quarantine}
```

`required`, `primaryKey` and `relationships` are not decoration: **lint fails if the schema says one thing and the rules say another** (for example a required column with no not-null rule).

## How the pipeline consumes a contract

```
contracts/*.yaml --lint--> contracts_bundle.json --upload--> Volume
   --stage "contracts"--> governance.data_contracts (registry)  +  governance.dq_rules (compiled rules)
        bronze stage : expected columns, landing folder  <- source contract; then schema conformance
        silver stage : typing SELECT generated from the silver + source contracts; rules from dq_rules; conformance
        gold stage   : assertions from dq_rules; reconciliation target; conformance
        semantic stage: creates the metric views, then interface + tests + SLA from the semantic contract
```
Stages read the **registry in Databricks**, not files in git, so what runs is exactly what was published and versioned
(`governance.data_contracts_history` keeps every version by checksum). If nothing is published a stage falls back to its built-in definition and says so in the log.

## Manual movement (what to do now)

1. **Edit** a contract in `contracts/`. If it changes behaviour, bump `version`.
2. **Check it** on your computer: `python tools/contract_tool.py lint` (needs `pip install pyyaml`).
3. **Bundle it:** `python tools/contract_tool.py bundle`. This writes `contracts_bundle.json`: every contract plus the metric view SQL they point at, in one file. Databricks does not need PyYAML to read it.
4. **Upload it** to the Volume: Catalog > `<catalog>` > `bronze` > `landing` > create folders `fbl/contracts/` > upload `contracts_bundle.json` there. (Path: `/Volumes/<catalog>/bronze/landing/fbl/contracts/contracts_bundle.json`.)
5. **Publish it:** run the notebook with `stage = contracts` (or the Job). It lints again, writes the registry, and compiles the rules.
6. **Confirm:** `SELECT * FROM governance.v_contract_status;` shows the new version and load time.

You can also upload the YAML files themselves instead of the bundle (needs PyYAML on the cluster), or skip uploading entirely: the notebook carries a built-in copy of the contracts as of the day it was built (`python tools/build_fbl_notebook.py` refreshes it). An uploaded bundle always wins over the built-in copy.

## Automated movement (when you are ready)

`.github/workflows/contracts.yml` is written for this and has not been run against your workspace:
- **Pull request:** lint, run the contract tests, and run `contract_tool.py diff --git-ref origin/<base>` to block breaking changes without a major version bump.
- **Merge to `main`:** build the bundle and copy it into the prod Volume with `databricks fs cp`. The next run of the Job's `contracts_publish` task enforces it. A commented line shows how to trigger the Job immediately.
- It uses the same service-principal secrets as `deploy.yml`. For a deploy-with-code approach you could instead sync `contracts/` as bundle files and read them from the workspace; the Volume route keeps data and contract versions together.

## Semantic contracts for consumers

A metric view's schema is not enough, because a formula can change while every column name stays the same. Each semantic contract therefore promises four things:

| Section | What | Enforced how |
| --- | --- | --- |
| Interface | every promised field and measure exists and returns the promised type | runs a real `MEASURE()` query for each; a type mismatch is a warning until `strictTypes` is set to true |
| Meaning | each formula's fingerprint (`exprHash`) | lint fails when the metric view SQL changes without the contract being updated |
| Behaviour | `reconcile` (view equals a direct query on gold), `expect` (known golden values), `bounds` (value range) | semantic stage; `severity: warning` tests are recorded but never block |
| Service | freshness SLA: gold built within N hours and its last gate was clean | reads `governance.dq_table_stats` and `dq_rule_results` |

**Changing a formula on purpose:** edit the metric view SQL, run `python tools/contract_tool.py stamp` to refresh the fingerprints, bump the contract version (MAJOR if consumers' numbers change), and let the diff gate record it as a breaking change.

**Consumers:** each contract lists them under `consumers`. That is documentation today. Before a breaking change, those are the people to notify. (Whether Databricks query history can discover undeclared consumers has not been verified.)

**Deprecation:** set `status: deprecated` and `replacedBy` in `customProperties`, keep the old view for the notice period, and publish the new version under a new major.

**Why `strictTypes` starts off:** the interface check compares the type each query returns with the contract. I could confirm the types on open-source Spark but not on Databricks metric views, so mismatches are warnings at first. Run once, read any warnings, then flip `strictTypes: true` per contract.

## What the checks found on the sample data
- **Real finding:** the 900 lb player (P027) is only a *warning* in silver (PLR-008), so he reaches gold. The roster contract's weight test (`SEM-FBLR-004`, warning level) surfaces him as an out-of-bounds average for Redrock Coyotes / LF. Decide whether PLR-008 should become `quarantine`; that would change the answer key.
- **Breaking-change demo:** rename a measure and run `contract_tool.py diff`: it reports the removal as breaking and a version-policy error.

## Reading results

```sql
SELECT * FROM governance.v_contract_status;                 -- what is published
SELECT * FROM governance.v_contract_failures;               -- broken or warned checks in the latest run
SELECT layer, check_type, passed, count(*) n FROM governance.contract_results
WHERE run_id = '<run id>' GROUP BY 1,2,3 ORDER BY 1,2,3;
SELECT contract_id, version, loaded_at FROM governance.data_contracts_history ORDER BY loaded_at DESC;
```

## Try these
1. Add a column to a source CSV and see the source contract's schema check warn about an undeclared column.
2. Make a gold column `string` in the contract: the conformance check fails the run.
3. Change a rule's `action` in a silver contract, bump the version, re-bundle, upload, and watch row counts change.
4. Tighten `SEM-FBLR-004` to an error and watch the run fail on P027; then promote PLR-008 to `quarantine` and fix the answer key.
5. Rename a measure in a metric view and run `lint`, then `diff`.
6. Add a new consumer-facing measure, `stamp`, bump the minor version, and confirm the diff calls it non-breaking.

## Limits, stated plainly
- Tested on a local Spark session: publish, lint, rules equal to the previous CSV rules, the whole FBL pipeline from contracts, every semantic test with a stand-in query (inlined measures, since open-source Spark has no `MEASURE()`), the Lahman golden values, and the failure paths. **Not tested in Databricks:** `CREATE VIEW ... WITH METRICS`, real `MEASURE()` queries and their return types, reading the Volume bundle, PyYAML availability, and the GitHub workflow.
- ODCS: structure follows v3.1 documentation but is unvalidated against an ODCS tool; rules use the `custom` quality type with this project's engine name.
- Access (who may query what) is documented through grants, not enforced by the contract.
- Contract diffing compares file versions; it does not know who consumes a view.
