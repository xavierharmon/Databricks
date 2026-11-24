-- FBL batting metrics for consumers. Governed by contract fbl.semantic.mv_fbl_batting.
-- OBP and OPS are "approx": the fictional league data has no hit-by-pitch or sacrifice-fly columns.
CREATE OR REPLACE VIEW {{catalog}}.semantic.mv_fbl_batting
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "FBL batting metrics. Source grain is player-season-team. Ratios are computed from summed components, never averaged."
source: {{catalog}}.gold.fbl_fact_batting

joins:
  - name: player
    source: {{catalog}}.gold.fbl_dim_player
    'on': source.player_id = player.player_id
    rely:
      at_most_one_match: true
  - name: team
    source: {{catalog}}.gold.fbl_dim_team
    'on': source.team_id = team.team_id
    rely:
      at_most_one_match: true

fields:
  - name: season
    expr: source.season
    display_name: "Season"
    synonyms: ["year"]
  - name: player_name
    expr: player.full_name
    display_name: "Player"
    synonyms: ["batter", "hitter"]
  - name: batting_hand
    expr: player.bats
    display_name: "Bats"
  - name: position
    expr: player.position
    display_name: "Position"
  - name: team_name
    expr: team.team_name
    display_name: "Team"
  - name: league
    expr: team.league
    display_name: "League"
  - name: city
    expr: team.city
    display_name: "City"

measures:
  - name: games
    expr: SUM(g)
    display_name: "Games"
  - name: at_bats
    expr: SUM(ab)
    display_name: "At Bats"
    synonyms: ["AB"]
  - name: hits
    expr: SUM(h)
    display_name: "Hits"
    synonyms: ["H"]
  - name: doubles
    expr: SUM(doubles)
    display_name: "Doubles"
  - name: triples
    expr: SUM(triples)
    display_name: "Triples"
  - name: home_runs
    expr: SUM(hr)
    display_name: "Home Runs"
    synonyms: ["HR", "homers"]
  - name: rbi
    expr: SUM(rbi)
    display_name: "RBI"
  - name: walks
    expr: SUM(bb)
    display_name: "Walks"
    synonyms: ["BB"]
  - name: strikeouts
    expr: SUM(so)
    display_name: "Strikeouts"
    synonyms: ["SO", "K"]
  - name: stolen_bases
    expr: SUM(sb)
    display_name: "Stolen Bases"
    synonyms: ["SB"]
  - name: batting_avg
    expr: SUM(h) / NULLIF(SUM(ab), 0)
    display_name: "Batting Average"
    synonyms: ["AVG", "BA"]
    comment: "Summed hits divided by summed at bats. Never average player averages."
  - name: on_base_pct_approx
    expr: (SUM(h) + SUM(bb)) / NULLIF(SUM(ab) + SUM(bb), 0)
    display_name: "On-Base Percentage (approx)"
    synonyms: ["OBP"]
  - name: slugging_pct
    expr: SUM(total_bases) / NULLIF(SUM(ab), 0)
    display_name: "Slugging Percentage"
    synonyms: ["SLG"]
  - name: ops_approx
    expr: MEASURE(on_base_pct_approx) + MEASURE(slugging_pct)
    display_name: "OPS (approx)"
    synonyms: ["OPS"]
  - name: home_run_rate
    expr: SUM(hr) * 1.0 / NULLIF(SUM(ab), 0)
    display_name: "Home Run Rate (per at bat)"
  - name: batters
    expr: COUNT(DISTINCT source.player_id)
    display_name: "Distinct Batters"
    comment: "Non-additive distinct count: do not sum across groups."
$$;
