/* ============================================================================
   02_product_analysis.sql
   
   Depends on: views.sql (v_order_items_enriched)
   MySQL 8.0+
   ============================================================================ */

USE ecommerce_db;

/* ============================================================================
   Query 1 - Product Leaderboard (by Net Revenue)
   =========================================================================== */
SELECT
    product_name,
    category_name,
    SUM(line_net) AS net_revenue
FROM v_order_items_enriched
WHERE cancel_at_payment = 0
GROUP BY product_name, category_name
ORDER BY net_revenue DESC
LIMIT 5;

/* ============================================================================
   Query 2 - Product Leaderboard (by Units Sold)
   =========================================================================== */
SELECT
    product_name,
    category_name,
    SUM(quantity) AS units_sold
FROM v_order_items_enriched
WHERE cancel_at_payment = 0
GROUP BY product_name, category_name
ORDER BY units_sold DESC
LIMIT 10;

/* ============================================================================
   Query 3 - Return Rate (by Product)
   =========================================================================== */
SELECT
    product_name,
    category_name,
    SUM(returned = 1) AS returned_items,
    SUM(quantity) AS total_items,
    ROUND(100 * SUM(returned = 1) / SUM(quantity), 2) AS return_rate_pct
FROM v_order_items_enriched
WHERE cancel_at_payment = 0
GROUP BY product_id, product_name, category_name
ORDER BY return_rate_pct DESC;

/* ============================================================================
   Query 4 - Performance (by Category)
   =========================================================================== */
WITH category_perf AS (
    SELECT
        category_name,
        SUM(line_net) AS net_revenue,
        SUM(quantity) AS units_sold,
        ROUND(100 * SUM(returned = 1) / SUM(quantity), 2) AS return_rate_pct
    FROM v_order_items_enriched
    WHERE cancel_at_payment = 0
    GROUP BY category_name
)
SELECT
    category_name,
    net_revenue,
    units_sold,
    return_rate_pct,
    ROUND(100 * net_revenue / SUM(net_revenue) OVER(), 2) AS revenue_contribution_pct
FROM category_perf
ORDER BY net_revenue DESC;

/* ============================================================================
   Query 5 - Hero Pproducts (Products that together represent 80% of total revenue)
   =========================================================================== */
WITH product_revenue AS (
    SELECT
        product_name,
        category_name,
        SUM(line_net) AS net_revenue
    FROM v_order_items_enriched
    WHERE cancel_at_payment = 0
    GROUP BY product_name, category_name
),
ranked AS (
    SELECT
        product_name,
        category_name,
        net_revenue,
        SUM(net_revenue) OVER (ORDER BY net_revenue DESC)
            AS cumulative_revenue,
        SUM(net_revenue) OVER () AS total_revenue
    FROM product_revenue
)
SELECT
    product_name,
    category_name,
    net_revenue,
    ROUND(100 * cumulative_revenue / total_revenue, 2) AS cumulative_pct
FROM ranked
WHERE cumulative_revenue <= 0.80 * total_revenue
ORDER BY net_revenue DESC;