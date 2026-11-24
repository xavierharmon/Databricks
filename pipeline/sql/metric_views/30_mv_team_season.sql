CREATE OR REPLACE VIEW {{catalog}}.semantic.mv_team_season
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Team-season results, run environment, attendance and payroll. Source grain is team-season. Payroll exists from 1985 only."
source: {{catalog}}.gold.fact_team_season

joins:
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
  - name: ballpark
    expr: team.park_name
    display_name: "Ballpark"
  - name: won_world_series
    expr: team.won_world_series
    display_name: "Won World Series"
    synonyms: ["champion"]

measures:
  - name: team_seasons
    expr: COUNT(1)
    display_name: "Team Seasons"
  - name: games
    expr: SUM(g)
    display_name: "Games"
  - name: wins
    expr: SUM(w)
    display_name: "Wins"
    synonyms: ["W"]
  - name: losses
    expr: SUM(l)
    display_name: "Losses"
    synonyms: ["L"]
  - name: win_pct
    expr: SUM(w) * 1.0 / NULLIF(SUM(w) + SUM(l), 0)
    display_name: "Win Percentage"
    synonyms: ["winning percentage", "WPCT"]
  - name: runs_scored
    expr: SUM(r)
    display_name: "Runs Scored"
  - name: runs_allowed
    expr: SUM(ra)
    display_name: "Runs Allowed"
  - name: run_differential
    expr: SUM(r) - SUM(ra)
    display_name: "Run Differential"
    synonyms: ["run diff"]
  - name: pythagorean_win_pct
    expr: POWER(SUM(r), 1.83) / NULLIF(POWER(SUM(r), 1.83) + POWER(SUM(ra), 1.83), 0)
    display_name: "Pythagorean Win Percentage"
    synonyms: ["expected win pct"]
    comment: "Expected win percentage from runs scored and allowed (exponent 1.83)."
  - name: wins_over_expected
    expr: MEASURE(win_pct) - MEASURE(pythagorean_win_pct)
    display_name: "Win Pct Over Expected"
    synonyms: ["luck"]
  - name: attendance
    expr: SUM(attendance)
    display_name: "Attendance"
  - name: avg_attendance_per_home_game
    expr: SUM(attendance) / NULLIF(SUM(g_home) FILTER (WHERE attendance IS NOT NULL), 0)
    display_name: "Average Attendance per Home Game"
    comment: "Only counts home games where attendance was recorded, so missing data does not drag the average down."
  - name: payroll
    expr: SUM(payroll_usd)
    display_name: "Payroll (USD)"
    comment: "Sum of player salaries. NULL before 1985."
  - name: cost_per_win
    expr: SUM(payroll_usd) / NULLIF(SUM(w) FILTER (WHERE payroll_usd IS NOT NULL), 0)
    display_name: "Payroll per Win (USD)"
    comment: "Wins in the denominator are restricted to seasons that have payroll, otherwise pre-1985 wins would flatter the ratio."
$$;
