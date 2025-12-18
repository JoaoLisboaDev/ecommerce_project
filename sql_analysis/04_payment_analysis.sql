/* ============================================================================
   04_payment_analysis.sql

   Depends on:
   - v_last_payments
   - v_payment_attempts
   - v_orders_enriched
   MySQL 8.0+
   ============================================================================ */

USE ecommerce_db;

/* ============================================================================
   Average Payment Success Rate (based on every payment attempt)
   =========================================================================== */
SELECT 
	ROUND(100 * SUM(pa.payment_status_name = 'Paid') / COUNT(*), 2) AS global_payment_success_rate
FROM v_payment_attempts pa;

/* ============================================================================
   Average Payment Success Rate (based on last payment attempt)
   =========================================================================== */
SELECT
	ROUND(100 * SUM(lp.payment_status_name = 'Paid') / COUNT(*), 2) AS last_attempt_success_rate,
    ROUND(SUM(lp.last_payment_attempt_no) / COUNT(*), 2) AS average_last_attempt_number
FROM v_last_payments lp;

/* ============================================================================
   Payment Method Performance on Last Attempt
   =========================================================================== */
SELECT 
	lp.payment_method_name,
    COUNT(*) number_of_last_attempts,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total_last_attempts,
    ROUND(100 * SUM(lp.payment_status_name = 'Paid') / COUNT(*), 2) AS method_success_rate_on_last_attempt,
    ROUND(100 * SUM(lp.payment_status_name = 'Failed') / COUNT(*), 2) AS method_failure_rate_on_last_attempt
FROM v_last_payments lp
GROUP BY lp.payment_method_name;

/* ============================================================================
   Metrics by Payment Method
   =========================================================================== */
SELECT 
	payment_method_name,
    COUNT(*) AS number_of_attempts,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total_attempts,
    SUM(pa.payment_status_name = 'Paid') AS success_attempts,
    SUM(pa.payment_status_name = 'Failed') AS failed_attempts,
    ROUND(100 * SUM(pa.payment_status_name = 'Paid') / COUNT(*), 2) AS method_success_rate,
    ROUND(100 * SUM(pa.payment_status_name = 'Failed') / COUNT(*), 2) AS method_failure_rate,
    
    -- Percentagem dos sucessos de cada método relativamente aos sucessos totais
    ROUND(100 * SUM(pa.payment_status_name = 'Paid') / SUM(SUM(pa.payment_status_name = 'Paid')) OVER (), 2) AS pct_of_all_successes
FROM v_payment_attempts pa
GROUP BY payment_method_name;

/* ============================================================================
   Payment Attempts needed
   =========================================================================== */
SELECT
  last_payment_attempt_no AS attempts_until_paid,
  COUNT(*) AS number_of_orders
FROM v_last_payments
WHERE payment_status_name = 'Paid'
GROUP BY last_payment_attempt_no
ORDER BY last_payment_attempt_no;

/* ============================================================================
   Revenue by Payment Method
   =========================================================================== */
SELECT
    lp.payment_method_name AS payment_method,
    SUM(o.net_revenue) AS total_net_revenue
FROM v_last_payments lp
JOIN v_orders_enriched o ON o.order_id = lp.order_id
GROUP BY payment_method
ORDER BY total_net_revenue DESC;