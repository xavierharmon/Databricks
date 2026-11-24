-- GOLD transform for fact_batting: one row per player, season and team.
-- Store ADDITIVE components only. Ratios (batting average) live in the metric view, because an average of averages is wrong.
SELECT player_id, season, team_id, g, ab, h, doubles, triples, hr, rbi, bb, so, sb,
       h - doubles - triples - hr           AS singles,
       h + doubles + 2 * triples + 3 * hr   AS total_bases,
       ab + bb                              AS plate_appearances_approx
FROM silver.fbl_batting
