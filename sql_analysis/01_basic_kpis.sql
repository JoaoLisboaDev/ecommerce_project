/* =============================================================================
   01_basic_kpis.sql
   
   Basic KPIs (global & monthly) over the full data range
   Depends on: views.sql (v_orders_enriched)
   MySQL 8.0+
   ============================================================================ */
   
USE ecommerce_db;

/* =============================================================================
   Global KPIs
   ============================================================================= */
SELECT
	COUNT(*) AS total_orders,
    SUM(order_items_count) AS total_order_items,
    SUM(units_count) AS total_units,
    COUNT(DISTINCT customer_id) AS total_active_customers,
    SUM(net_revenue) AS total_net_revenue,
    ROUND(SUM(net_revenue)/COUNT(*), 2) AS avg_order_value,
    ROUND(SUM(order_items_count)/COUNT(*), 2) AS avg_order_items_by_order,
    ROUND(100 * SUM(order_status_id = 5)/COUNT(*), 2) AS cancellation_rate,
    ROUND(100 * SUM(has_returns) / SUM(order_status_code = 'delivered'), 2) AS return_rate_delivered_orders
FROM v_orders_enriched
WHERE country = 'PT';

/* =============================================================================
   Mensal KPIs
   ============================================================================= */
SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    COUNT(*) AS total_orders,
	SUM(net_revenue) AS total_net_revenue,
    SUM(units_count) AS total_units,
    ROUND(SUM(net_revenue)/COUNT(*), 2) AS avg_order_value
FROM v_orders_enriched
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month;