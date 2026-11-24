CREATE OR REPLACE VIEW {{catalog}}.semantic.mv_pitching
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Pitching metrics. Source grain is player-season-stint. innings = ipouts / 3, so every rate uses outs, not decimal innings."
source: {{catalog}}.gold.fact_pitching_season

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
  - name: player_name
    expr: player.full_name
    display_name: "Pitcher"
    synonyms: ["player", "name"]
  - name: throws
    expr: player.throws
    display_name: "Throws"
  - name: team_name
    expr: team.team_name
    display_name: "Team"
  - name: franchise
    expr: team.franchise_name
    display_name: "Franchise"
  - name: league
    expr: team.league_id
    display_name: "League"

measures:
  - name: games
    expr: SUM(g)
    display_name: "Games"
  - name: games_started
    expr: SUM(gs)
    display_name: "Games Started"
  - name: wins
    expr: SUM(w)
    display_name: "Wins"
  - name: losses
    expr: SUM(l)
    display_name: "Losses"
  - name: saves
    expr: SUM(sv)
    display_name: "Saves"
  - name: complete_games
    expr: SUM(cg)
    display_name: "Complete Games"
  - name: shutouts
    expr: SUM(sho)
    display_name: "Shutouts"
  - name: strikeouts
    expr: SUM(so)
    display_name: "Strikeouts"
    synonyms: ["K", "SO"]
  - name: walks
    expr: SUM(bb)
    display_name: "Walks"
    synonyms: ["BB"]
  - name: hits_allowed
    expr: SUM(h)
    display_name: "Hits Allowed"
  - name: home_runs_allowed
    expr: SUM(hr)
    display_name: "Home Runs Allowed"
  - name: earned_runs
    expr: SUM(er)
    display_name: "Earned Runs"
  - name: innings_pitched
    expr: SUM(ipouts) / 3.0
    display_name: "Innings Pitched"
    synonyms: ["IP"]
  - name: era
    expr: 27.0 * SUM(er) / NULLIF(SUM(ipouts), 0)
    display_name: "ERA"
    synonyms: ["earned run average"]
    comment: "Earned runs per nine innings = 27 x ER / outs recorded."
  - name: whip
    expr: 3.0 * (SUM(h) + SUM(bb)) / NULLIF(SUM(ipouts), 0)
    display_name: "WHIP"
    synonyms: ["walks plus hits per inning"]
  - name: strikeouts_per_9
    expr: 27.0 * SUM(so) / NULLIF(SUM(ipouts), 0)
    display_name: "Strikeouts per 9"
    synonyms: ["K/9"]
  - name: walks_per_9
    expr: 27.0 * SUM(bb) / NULLIF(SUM(ipouts), 0)
    display_name: "Walks per 9"
    synonyms: ["BB/9"]
  - name: home_runs_per_9
    expr: 27.0 * SUM(hr) / NULLIF(SUM(ipouts), 0)
    display_name: "Home Runs per 9"
    synonyms: ["HR/9"]
  - name: strikeout_to_walk_ratio
    expr: SUM(so) * 1.0 / NULLIF(SUM(bb), 0)
    display_name: "Strikeout-to-Walk Ratio"
    synonyms: ["K/BB"]
  - name: win_pct
    expr: SUM(w) * 1.0 / NULLIF(SUM(w) + SUM(l), 0)
    display_name: "Win Percentage"
  - name: pitchers
    expr: COUNT(DISTINCT source.player_key)
    display_name: "Distinct Pitchers"
$$;
