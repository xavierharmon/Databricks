-- Identity stitching, strongest evidence first. See the glossary page 'Identity stitching'.
--  tier 1: the session logged in.  tier 2: the session bought and the order belongs to a customer.  tier 3: a guest checkout carried an email hash.
-- A browser id linked to exactly one person is attributed to that person for ALL its sessions (retroactive). A browser id linked to two or
-- more people is a shared device: only sessions with their own evidence are attributed, the rest are left ambiguous on purpose.
-- A new browser id after a cookie reset is linked only through a fingerprint that belongs to exactly one person. Bots are labelled BOT.
WITH sess AS (
  SELECT user_pseudo_id, ga_session_id, min(event_ts) AS session_ts, max(client_fp) AS client_fp, max(user_id) AS login_customer,
         max(transaction_id) AS order_id, max(email_hash) AS guest_email_hash,
         sum(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS page_views,
         max(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased,
         max(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) AS has_login
  FROM silver.tix_ga_event GROUP BY user_pseudo_id, ga_session_id),
cp AS (SELECT customer_id, person_id FROM gold_stage.tix_customer_person),
email_person AS (SELECT DISTINCT c.email_hash, cp.person_id FROM silver.tix_customer c JOIN cp ON c.customer_id = cp.customer_id),
ev AS (
  SELECT s.user_pseudo_id, s.ga_session_id, cp.person_id, 1 AS tier FROM sess s JOIN cp ON s.login_customer = cp.customer_id
  UNION ALL
  SELECT s.user_pseudo_id, s.ga_session_id, cp.person_id, 2 AS tier FROM sess s JOIN silver.tix_order_plan o ON s.order_id = o.order_id JOIN cp ON o.customer_id = cp.customer_id
  UNION ALL
  SELECT s.user_pseudo_id, s.ga_session_id, ep.person_id, 3 AS tier FROM sess s JOIN email_person ep ON s.guest_email_hash = ep.email_hash),
direct AS (SELECT user_pseudo_id, ga_session_id, count(DISTINCT person_id) AS n, min(person_id) AS person_id, min(tier) AS tier FROM ev GROUP BY user_pseudo_id, ga_session_id),
pseudo AS (SELECT user_pseudo_id, count(DISTINCT person_id) AS n_people, min(person_id) AS only_person FROM ev GROUP BY user_pseudo_id),
fp AS (SELECT s.client_fp, count(DISTINCT p.only_person) AS n, min(p.only_person) AS person_id
       FROM sess s JOIN pseudo p ON s.user_pseudo_id = p.user_pseudo_id WHERE p.n_people = 1 GROUP BY s.client_fp),
bots AS (SELECT user_pseudo_id FROM sess GROUP BY user_pseudo_id HAVING sum(page_views) > 100 AND max(purchased) = 0 AND max(has_login) = 0)
SELECT s.user_pseudo_id, CAST(s.ga_session_id AS INT) AS ga_session_id, s.session_ts,
  CASE WHEN b.user_pseudo_id IS NOT NULL THEN 'BOT'
       WHEN p.n_people = 1 THEN p.only_person
       WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN d.person_id ELSE 'UNRESOLVED' END
       WHEN f.n = 1 THEN f.person_id
       ELSE 'UNRESOLVED' END AS person_id,
  CASE WHEN b.user_pseudo_id IS NOT NULL THEN 'bot'
       WHEN p.n_people = 1 THEN CASE WHEN d.n = 1 THEN concat('direct_t', CAST(d.tier AS STRING)) ELSE 'retroactive_same_browser_id' END
       WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN concat('shared_device_direct_t', CAST(d.tier AS STRING)) ELSE 'shared_device_ambiguous' END
       WHEN f.n = 1 THEN 'fingerprint_after_cookie_reset'
       ELSE 'unresolved' END AS method,
  CAST(CASE WHEN b.user_pseudo_id IS NOT NULL THEN 0
            WHEN p.n_people = 1 THEN CASE WHEN d.n = 1 THEN 95 ELSE 80 END
            WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN 90 ELSE 0 END
            WHEN f.n = 1 THEN 60 ELSE 0 END AS INT) AS confidence_pct
FROM sess s
LEFT JOIN bots b ON s.user_pseudo_id = b.user_pseudo_id
LEFT JOIN pseudo p ON s.user_pseudo_id = p.user_pseudo_id
LEFT JOIN direct d ON s.user_pseudo_id = d.user_pseudo_id AND s.ga_session_id = d.ga_session_id
LEFT JOIN fp f ON s.client_fp = f.client_fp
