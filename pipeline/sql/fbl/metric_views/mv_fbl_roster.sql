-- FBL roster metrics for consumers. Governed by contract fbl.semantic.mv_fbl_roster.
CREATE OR REPLACE VIEW {{catalog}}.semantic.mv_fbl_roster
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "FBL roster metrics. Source grain is one row per player."
source: {{catalog}}.gold.fbl_dim_player

joins:
  - name: team
    source: {{catalog}}.gold.fbl_dim_team
    'on': source.team_id = team.team_id
    rely:
      at_most_one_match: true

fields:
  - name: team_name
    expr: team.team_name
    display_name: "Team"
  - name: league
    expr: team.league
    display_name: "League"
  - name: position
    expr: source.position
    display_name: "Position"
  - name: batting_hand
    expr: source.bats
    display_name: "Bats"
  - name: throwing_hand
    expr: source.throws
    display_name: "Throws"

measures:
  - name: players
    expr: COUNT(1)
    display_name: "Players"
    synonyms: ["headcount", "roster size"]
  - name: avg_height_in
    expr: AVG(height_in)
    display_name: "Average Height (in)"
  - name: avg_weight_lb
    expr: AVG(weight_lb)
    display_name: "Average Weight (lb)"
  - name: switch_hitters
    expr: COUNT(1) FILTER (WHERE source.bats = 'B')
    display_name: "Switch Hitters"
  - name: teams
    expr: COUNT(DISTINCT source.team_id)
    display_name: "Distinct Teams"
$$;
