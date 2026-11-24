# Answer key: planted errors in `data/fbl/*.csv`

`row_id` in the quarantine table = 0-based position of the row in its file (header not counted).
Run the pipeline, then check that every line below shows up exactly as described. If it does, the checks work.

## teams.csv (15 rows in)
| team_id | row_id | Planted problem | Rule | What should happen |
| --- | --- | --- | --- | --- |
| T05 | 4 and 8 | Identical duplicate row | TEAM-001 | Extra copy removed, **auto-corrected** |
| T06 | 5 | League typed `south` | TEAM-007 | Fixed to `South`, **auto-corrected** |
| T09 | 9 and 10 | Same team twice, different founded_year (2016 vs 2017) | TEAM-002 | **Both rejected** (conflict needs a human) |
| T10 | 11 | Blank team_name | TEAM-003 | Rejected |
| T11 | 12 | founded_year 2099 | TEAM-005 | Rejected |
| T12 | 13 | League `XX` | TEAM-004 | Rejected (also logged as a TEAM-007 re-casing, which is harmless) |
| T13 | 14 | payroll_cap_musd -50 (business rule) | TEAM-006 | Rejected |

**Expected: in 15, deduped 1, rejected 6, out 8.**

## players.csv (42 rows in)
| player_id | Planted problem | Rule | What should happen |
| --- | --- | --- | --- |
| P007 | Identical duplicate row | PLR-001 | Extra copy removed, auto-corrected |
| P012 | bats typed `r` | PLR-011 | Fixed to `R`, auto-corrected |
| P015 | Two rows, different birth_date | PLR-002 | **Both rows rejected** |
| P019 | Blank weight_lb | PLR-003 | Rejected |
| P021 | position `XX` | PLR-006 | Rejected |
| P023 | Blank last_name | PLR-003 | Rejected |
| P027 | weight_lb 900 | PLR-008 | **Warning only**, row stays |
| P031 | bats `Z` | PLR-004 | Rejected |
| P034 | height_in 12 | PLR-007 | Rejected |
| P036 | birth_date 2031-04-02 (age rule) | PLR-009 | Rejected |
| P038 | team_id `T99` (no such team) | PLR-010 | Rejected |
| P040 | team_id `T09` (team T09 was rejected above) | PLR-010 | Rejected: **cascade** from the team failure |

**Expected: in 42, deduped 1, rejected 10, out 31.**

## batting.csv (84 rows in)
| Row | Planted problem | Rule | What should happen |
| --- | --- | --- | --- |
| P001 2024 (twice) | Identical duplicate | BAT-001 | Extra copy removed, auto-corrected |
| P002 2023 (twice) | Same key, different hits | BAT-002 | **Both rows rejected** |
| P003 2024 | hits greater than at bats | BAT-006 | Rejected |
| P004 2023 | at bats = `N/A` | BAT-003 | Rejected (text fails the numeric cast, becomes NULL; the quarantine payload still shows `N/A`) |
| P005 2024 | home runs greater than hits | BAT-007 and BAT-008 | Rejected, two reasons on one row |
| P006 2023 | doubles+triples+HR greater than hits | BAT-008 | Rejected |
| P008 2024 | 190 games | BAT-005 | Rejected |
| P009 2023 | 100 at bats in 10 games | BAT-009 (also BAT-006 as a side effect) | Rejected |
| P010 2024 | stolen bases -4 | BAT-010 | Rejected |
| P011 2023 | season 1999 | BAT-004 | Rejected |
| P999 2024 | player does not exist | BAT-011 | Rejected |
| P014 2024 with team `T77` | team does not exist | BAT-012 | Rejected |
| 18 rows for P015, P019, P021, P023, P031, P034, P036, P038, P040 | their player was rejected | BAT-011 | Rejected: **cascade** (9 players x 2 seasons) |

**Expected: in 84, deduped 1, rejected 30, out 53.**

## Gold
`fbl_dim_team` 8 rows, `fbl_dim_player` 31 rows, `fbl_fact_batting` 53 rows. Every GLD rule passes (no NULLs, unique keys, no orphans, row counts match silver).

**Totals: 46 rows rejected, 6 auto-corrections (1 team dupe, 1 player dupe, 1 batting dupe, plus the re-casings of T06 and T12 and the P012 fix), 1 warning.** The report banner shows the same numbers.
Note: auto-corrections are counted per rule hit in the report, so a re-cased row may appear in both the rejected and auto-corrected sections.

## Contract mode: extra expectations
When the pipeline runs from the contracts (the default), you should also see:
- **Contract checks all pass** for the source, silver and gold contracts (schema conformance), and for both FBL semantic contracts.
- **One warning-level contract result:** `SEM-FBLR-004` reports `avg_weight_lb` out of bounds for **Redrock Coyotes / LF** (average 900). This is player P027 (weight 900), who silver only warns about (PLR-008) and who therefore reaches gold. It is the contract catching a data issue a row-level warning let through.
- The row counts above are identical to the pre-contract run: the contracts compile to exactly the same 41 rules.
