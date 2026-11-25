-- GOLD transform for dim_customer: one row per customer record. Hashes only; never raw email or phone.
SELECT customer_id, first_name, last_name, email_hash, phone_hash, postal_code, created_ts
FROM silver.tix_customer
