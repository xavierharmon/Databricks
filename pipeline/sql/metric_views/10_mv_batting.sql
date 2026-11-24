-- METRIC VIEW = governed semantic object. Define each KPI ONCE here; every dashboard, notebook, Genie space
-- and BI tool then gets the same answer. Query with: SELECT dim, MEASURE(measure) FROM view GROUP BY dim.
-- Key idea: measures are aggregations evaluated at QUERY time at whatever grain the caller groups by.
CREATE OR REPLACE VIEW {{catalog}}.semantic.mv_batting
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Batting metrics. Source grain is player-season-stint. Ratios are computed from summed components, never averaged."
source: {{catalog}}.gold.fact_batting_season

joins:
  - name: player
    source: {{catalog}}.gold.dim_player
    'on': source.player_key = player.player_key
    rely:
      at_most_one_match: true
  - name: team
    source: {{catalog}}.gold.dim_team_season
    'on': source.team_season_key = team.team_season_key
    rely:
      at_most_one_match: true
  - name: calendar
    source: {{catalog}}.gold.dim_season
    'on': source.season = calendar.season
    rely:
      at_most_one_match: true

fields:
  - name: season
    expr: source.season
    display_name: "Season"
    synonyms: ["year"]
  - name: decade
    expr: calendar.decade
    display_name: "Decade"
  - name: baseball_era
    expr: calendar.era
    display_name: "Baseball Era"
    comment: "Business-defined era classification, see glossary term Era."
  - name: player_name
    expr: player.full_name
    display_name: "Player"
    synonyms: ["batter", "hitter", "name"]
  - name: batting_hand
    expr: player.batting_hand
    display_name: "Bats"
    synonyms: ["handedness"]
  - name: birth_country
    expr: player.birth_country
    display_name: "Birth Country"
  - name: team_name
    expr: team.team_name
    display_name: "Team"
  - name: franchise
    expr: team.franchise_name
    display_name: "Franchise"
  - name: league
    expr: team.league_id
    display_name: "League"
  - name: division
    expr: team.division_id
    display_name: "Division"

measures:
  - name: games
    expr: SUM(g)
    display_name: "Games"
  - name: at_bats
    expr: SUM(ab)
    display_name: "At Bats"
    synonyms: ["AB"]
  - name: plate_appearances
    expr: SUM(plate_appearances)
    display_name: "Plate Appearances"
    synonyms: ["PA"]
  - name: runs
    expr: SUM(r)
    display_name: "Runs"
  - name: hits
    expr: SUM(h)
    display_name: "Hits"
    synonyms: ["H"]
  - name: home_runs
    expr: SUM(hr)
    display_name: "Home Runs"
    synonyms: ["HR", "homers", "dingers"]
  - name: rbi
    expr: SUM(rbi)
    display_name: "RBI"
    synonyms: ["runs batted in"]
  - name: stolen_bases
    expr: SUM(sb)
    display_name: "Stolen Bases"
    synonyms: ["SB", "steals"]
  - name: walks
    expr: SUM(bb)
    display_name: "Walks"
    synonyms: ["BB", "bases on balls"]
  - name: strikeouts
    expr: SUM(so)
    display_name: "Strikeouts"
    synonyms: ["SO", "K"]
  - name: batting_avg
    expr: SUM(h) / NULLIF(SUM(ab), 0)
    display_name: "Batting Average"
    synonyms: ["AVG", "BA"]
    comment: "Hits divided by at bats. Never average player averages: always divide summed hits by summed at bats."
  - name: on_base_pct
    expr: (COALESCE(SUM(h), 0) + COALESCE(SUM(bb), 0) + COALESCE(SUM(hbp), 0)) / NULLIF(COALESCE(SUM(ab), 0) + COALESCE(SUM(bb), 0) + COALESCE(SUM(hbp), 0) + COALESCE(SUM(sf), 0), 0)
    display_name: "On-Base Percentage"
    synonyms: ["OBP"]
  - name: slugging_pct
    expr: SUM(total_bases) / NULLIF(SUM(ab), 0)
    display_name: "Slugging Percentage"
    synonyms: ["SLG"]
  - name: ops
    expr: MEASURE(on_base_pct) + MEASURE(slugging_pct)
    display_name: "OPS"
    synonyms: ["on-base plus slugging"]
    comment: "Composed measure: built from two earlier measures with MEASURE(). Change OBP once, OPS follows."
  - name: isolated_power
    expr: MEASURE(slugging_pct) - MEASURE(batting_avg)
    display_name: "Isolated Power"
    synonyms: ["ISO"]
  - name: home_run_rate
    expr: SUM(hr) * 1.0 / NULLIF(SUM(plate_appearances), 0)
    display_name: "Home Run Rate"
  - name: strikeout_rate
    expr: SUM(so) * 1.0 / NULLIF(SUM(plate_appearances), 0)
    display_name: "Strikeout Rate"
    synonyms: ["K%"]
  - name: walk_rate
    expr: SUM(bb) * 1.0 / NULLIF(SUM(plate_appearances), 0)
    display_name: "Walk Rate"
    synonyms: ["BB%"]
  - name: stolen_base_success_rate
    expr: SUM(sb) * 1.0 / NULLIF(SUM(sb) + SUM(cs), 0)
    display_name: "Stolen Base Success Rate"
  - name: batters
    expr: COUNT(DISTINCT source.player_key)
    display_name: "Distinct Batters"
    comment: "Non-additive: distinct counts cannot be summed across groups. The metric view handles that for you."
  - name: home_runs_regular_stints
    expr: SUM(hr) FILTER (WHERE ab >= 400)
    display_name: "Home Runs (400+ AB stints)"
    comment: "Filtered measure example: only rows with at least 400 at bats."
  - name: cumulative_home_runs
    expr: SUM(hr)
    display_name: "Cumulative Home Runs"
    comment: "Window measure: running total ordered by season. Group by season to see the curve."
    window:
      - order: season
        range: cumulative
$$;
