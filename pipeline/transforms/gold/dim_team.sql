-- GOLD transform for dim_team: one row per team.
-- Reads CLEAN silver only. Optional descriptive text gets an explicit label ('None') so reports show a group instead of dropping NULLs.
SELECT team_id, team_name, city, league, founded_year, home_park, payroll_cap_musd,
       coalesce(mascot, 'None') AS mascot
FROM silver.fbl_team
