-- 1. Number of users with more than 5 events last week by product
WITH weekly_user_events AS (
    SELECT
        f.user_id,
        p.product_name,
        COUNT(*) as event_count
    FROM nessie.warehouse.fact_events f
    LEFT JOIN nessie.warehouse.dim_product p
        ON f.product = p.product_name
    WHERE f.time_stamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    GROUP BY f.user_id, p.product_name
    HAVING COUNT(*) > 5
)
SELECT
    product_name,
    COUNT(DISTINCT user_id) as users_with_more_than_5_events
FROM weekly_user_events
GROUP BY product_name
ORDER BY users_with_more_than_5_events DESC;


-- 2. Number of Events per user per product last week
SELECT
    f.user_id,
    p.product_name,
    COUNT(*) as event_count
FROM nessie.warehouse.fact_events f
LEFT JOIN nessie.warehouse.dim_product p
    ON f.product = p.product_name
LEFT JOIN nessie.warehouse.dim_user_type2 u
    ON f.user_id = u.user_id
    AND f.time_stamp BETWEEN u.effective_date AND u.expiration_date
WHERE f.time_stamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY f.user_id, p.product_name
ORDER BY p.product_name, event_count DESC;


-- 3. Number of Selected Verb events by product by month
SELECT
    TO_CHAR(CAST(f.time_stamp AS DATE) , 'YYYY-MM') AS year_month,
    p.product_name,
    COUNT(*) as selected_events
FROM nessie.warehouse.fact_events f
LEFT JOIN nessie.warehouse.dim_product p
    ON f.product = p.product_name
LEFT JOIN nessie.warehouse.dim_verb v
    ON f.verb = v.verb
WHERE f.verb = 'selected'
GROUP BY year_month, p.product_name
ORDER BY year_month DESC, p.product_name;


-- 4. Number of overall events by user last month segmented by Verbs and Objects
SELECT
    f.user_id,
    u.last_name,
    v.verb,
    o.object_type,
    COUNT(*) as event_count
FROM nessie.warehouse.fact_events f
LEFT JOIN nessie.warehouse.dim_verb v
    ON f.verb = v.verb
LEFT JOIN nessie.warehouse.dim_object o
    ON f.object = o.object_type
LEFT JOIN nessie.warehouse.dim_user_type2 u
    ON f.user_id = u.user_id
    AND f.time_stamp BETWEEN u.effective_date AND u.expiration_date
WHERE f.time_stamp >= TIMESTAMP'2023-02-01' - INTERVAL '1' MONTH --Instead of CURRENT_TIMESTAMP use date closer to what was INSERTED to get results
GROUP BY f.user_id, u.last_name, v.verb, o.object_type
ORDER BY f.user_id, event_count DESC;