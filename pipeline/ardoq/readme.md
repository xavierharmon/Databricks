# Ardoq integration

Goal: Ardoq (enterprise architecture) always shows what is *actually* in Unity Catalog: assets, owners, lineage, and the
business glossary, without anyone re-keying it.

## 1. Model it in Ardoq once (the metamodel)
Create one workspace (e.g. **Data Platform - Databricks**) with these component types and reference types.

| Component type | Parent | Maps to | Suggested fields |
|---|---|---|---|
| Catalog | none | UC catalog (environment) | environment |
| Schema | Catalog | UC schema (medallion layer) | layer |
| Table | Schema | table in bronze/silver/gold | layer, owner, table_type |
| Metric View | Schema | metric view in `semantic` | layer, owner |
| Business Term | none | row in `governance.business_glossary` | owner, formula |

| Reference type | From -> To | Meaning |
|---|---|---|
| feeds | Table/Metric View -> Table/Metric View | lineage (from `system.access.table_lineage`) |
| defines | Business Term -> Table/Metric View/Schema | the term is implemented by that asset |

## 2. Wire up IDs
1. Create a service account / API token in Ardoq (Admin > Access control > Service accounts) and store it as `ARDOQ_API_TOKEN`.
2. `GET {base_url}/api/v2/workspaces/{workspace_id}/context` returns the `typeId` and reference-type ids. Put them in `ardoq/config.yml` (copy from `config.example.yml`).

## 3. Try it safely
```bash
python ardoq/sync_to_ardoq.py --sample --dry-run      # builds a payload from ardoq/sample_metadata.json
cat ardoq/last_payload.json                           # read what would be sent
python ardoq/sync_to_ardoq.py --catalog mlb_dev --warehouse-id <id> --dry-run   # real UC metadata, still nothing sent
python ardoq/sync_to_ardoq.py --catalog mlb_dev --warehouse-id <id>              # real sync
```

## 4. How it stays correct
* Uses the Ardoq **Batch API** with `upsert` + `uniqueBy: [rootWorkspace, name]`: re-running never duplicates.
* The batch is **transactional**: all or nothing.
* `prune: true` deletes components of these types that no longer exist in Unity Catalog. Leave it off until you trust the sync.
* In CI it runs only on `main` after a successful prod deploy (`.github/workflows/deploy.yml`).

## 5. Ideas once the basics work
* Add `application` components in Ardoq (e.g. Power BI, Genie space) and `consumes` references to metric views.
* Send DQ results (`governance.dq_results`) as a custom field so architects see data health next to each asset.
* Pull Ardoq ownership back into UC tags (reverse sync) to keep `owner_team` authoritative.
