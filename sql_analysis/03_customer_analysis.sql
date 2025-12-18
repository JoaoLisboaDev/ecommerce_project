/* ============================================================================
   03_customer_analysis.sql
   
   Source views: v_orders_enriched, v_order_items_enriched, v_customer_dim
   Grain varies by section (customer / day / cohort)
   MySQL 8.0+
   ============================================================================ */

USE ecommerce_db;

/* ============================================================================
   Customers per Country (Geographical Distribution)
   =========================================================================== */
SELECT
    country_name,
    COUNT(DISTINCT customer_id) AS total_customers,
    ROUND(100 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER (), 2) AS percentage
FROM v_customer_enriched
GROUP BY country_name
ORDER BY total_customers DESC;

/* ============================================================================
   Orders by customer
   =========================================================================== */
SELECT
  orders_count AS order_count,
  COUNT(*) AS number_of_customers,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM v_customer_enriched
GROUP BY orders_count
ORDER BY orders_count;

/* ============================================================================
   Customer Lifetime Value (Top Customers by Net Revenue)
   =========================================================================== */
SELECT
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.country,
    COUNT(o.order_id) AS total_orders,
    SUM(o.net_revenue) AS total_spent,
    ROUND(AVG(o.net_revenue), 2) AS avg_order_value
FROM v_orders_enriched o
JOIN v_customer_enriched c ON c.customer_id = o.customer_id
GROUP BY customer_name, c.country
ORDER BY total_spent DESC
LIMIT 20;

/* ============================================================================
   Customer Segments: One-time Buyers vs Repeat Buyers
   =========================================================================== */
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders
    FROM v_orders_enriched
    GROUP BY customer_id
)
SELECT
    SUM(total_orders = 1) AS one_time_buyers,
    SUM(total_orders > 1) AS repeat_customers,
    ROUND(100 * SUM(total_orders = 1) / COUNT(*), 2) AS pct_one_time,
    ROUND(100 * SUM(total_orders > 1) / COUNT(*), 2) AS pct_repeat
FROM customer_orders;

