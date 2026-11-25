-- GOLD transform for dim_event: one row per event, with its venue.
-- Reads CLEAN silver only. A concert has no sport or league, so they get the explicit label 'none' (gold never contains NULL).
SELECT e.event_id, e.event_title, e.event_category, coalesce(e.sport, 'none') AS sport, coalesce(e.league, 'none') AS league,
       e.venue_id, v.venue_name, v.city, v.timezone, e.start_ts, e.on_sale_ts, e.status
FROM silver.tix_event e
JOIN silver.tix_venue v ON e.venue_id = v.venue_id
