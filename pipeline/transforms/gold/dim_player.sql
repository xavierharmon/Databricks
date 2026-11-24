-- GOLD transform for dim_player: one row per player.
SELECT player_id, first_name, last_name, concat(first_name, ' ', last_name) AS full_name, birth_date, bats, throws,
       team_id, position, height_in, weight_lb, coalesce(nickname, 'None') AS nickname
FROM silver.fbl_player
