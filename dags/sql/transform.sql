-- Create schema
-- CREATE SCHEMA IF NOT EXISTS transformed;

-- Policy summary
DROP TABLE IF EXISTS transformed.policy_summary;
CREATE TABLE transformed.policy_summary (
    customer_id VARCHAR,
    name VARCHAR,
    total_policies BIGINT,
    active_policies BIGINT,
    total_premium NUMERIC
);

INSERT INTO transformed.policy_summary
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS name,
    COUNT(p.policy_id) AS total_policies,
    SUM(CASE WHEN p.status = 'active' THEN 1 ELSE 0 END) AS active_policies,
    SUM(p.premium_amount) AS total_premium
FROM public.customers c
LEFT JOIN public.policies p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, CONCAT(c.first_name, ' ', c.last_name);

-- Claims trends
DROP TABLE IF EXISTS transformed.claims_trends;
CREATE TABLE transformed.claims_trends (
    claim_month DATE,
    claim_type VARCHAR,
    claim_count BIGINT,
    total_claim_amount NUMERIC
);

INSERT INTO transformed.claims_trends
SELECT
    DATE_TRUNC('month', claim_date) AS claim_month,
    claim_type,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount
FROM public.claims
GROUP BY DATE_TRUNC('month', claim_date), claim_type;