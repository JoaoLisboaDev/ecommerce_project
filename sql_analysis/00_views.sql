/* ============================================================================
   02_views_base.sql
   ----------------------------------------------------------------------------
   Requires MySQL 8.0
   ----------------------------------------------------------------------------
   Views list:
	   1 - v_product_enriched
	   2 - v_customer_enriched
	   3 - v_orders_enriched
	   4 - v_order_items_enriched
	   5 - v_returns_enriched
	   6 - v_last_payments
	   7 - v_payment_attempts
	   8 - v_monthly_summary
   ============================================================================ */
   
   USE ecommerce_db;
   
/* ========================================================================================================================================
   1 - Product Enriched
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_product_enriched AS
SELECT
	p.product_id,
	p.name            AS product_name,
	p.price,
	p.stock_quantity,
	p.category_id,
	pc.name           AS category_name,
	p.created_at,
	p.updated_at
FROM products p
JOIN product_categories pc ON p.category_id = pc.category_id;

/* ========================================================================================================================================
   2 - Customer Enriched
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_customer_enriched AS
WITH orders_by_customer AS (
	SELECT 
        customer_id,
        Count(*) AS total_orders
    FROM orders
    GROUP BY customer_id
)
SELECT
	cu.customer_id,
    co.iso_code AS country,
	cu.first_name,
	cu.last_name,
	cu.birth_date,
	cu.city,
	cu.country_id,
    
    -- Orders by customer
    COALESCE(obc.total_orders, 0) orders_count,
    
	cu.created_at,
	cu.updated_at
FROM customers cu
LEFT JOIN orders_by_customer obc ON obc.customer_id = cu.customer_id
LEFT JOIN countries co ON co.country_id = cu.country_id;


/* ========================================================================================================================================
   3 - Orders Enriched
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_orders_enriched AS
WITH
order_items_by_order AS (
	SELECT
		oi.order_id,
        COUNT(*) AS order_items_count,  
        SUM(oi.quantity) AS units_count,
		SUM(oi.quantity * oi.unit_price) AS gross_revenue,
		GROUP_CONCAT(oi.order_item_id ORDER BY oi.order_item_id) AS order_item_ids
	FROM order_items oi
	GROUP BY oi.order_id
),
product_returns_by_order AS (
	SELECT
		o.order_id,
        CASE WHEN COUNT(pr.return_id) > 0 THEN 1 ELSE 0 END AS has_returns,
        COALESCE(GROUP_CONCAT(DISTINCT pr.order_item_id ORDER BY pr.order_item_id), '-') AS returned_order_item_ids,
        COALESCE(SUM(pr.refund_amount), 0) AS refunds_product_returns
    FROM orders o
    LEFT JOIN order_items oi ON oi.order_id = o.order_id
    LEFT JOIN product_returns pr ON pr.order_item_id = oi.order_item_id
    GROUP BY o.order_id
),
paid_payments_by_order AS (
  SELECT
    p.order_id,
    SUM(p.amount_paid) AS amount_paid_total
  FROM payments p
  GROUP BY p.order_id
)
SELECT
	o.order_id,
	o.customer_id,
    co.iso_code AS country,
	o.order_date,
	o.order_status_id,
	os.code AS order_status_code,
    
    COALESCE(oi_agg.order_items_count, 0) AS order_items_count,
    COALESCE(oi_agg.order_item_ids, '') AS order_item_ids,
    COALESCE(oi_agg.units_count, 0) AS units_count,
	COALESCE(oi_agg.gross_revenue, 0.00) AS gross_revenue,
    
     -- Amount paid  
	COALESCE(pay_agg.amount_paid_total, 0) AS amount_paid_total,
    
	-- Order not paied
    CASE WHEN COALESCE(pay_agg.amount_paid_total, 0) = 0 THEN 1 ELSE 0 END AS unpaid_order,
    
    pr_agg.has_returns,
	pr_agg.returned_order_item_ids AS returned_order_item_ids,
	pr_agg.refunds_product_returns AS refunds_product_returns,
    
    -- Refunds on payed orders that were cancelled after payment
	CASE
		WHEN os.code = 'cancelled'
			AND pr_agg.refunds_product_returns = 0
            AND COALESCE(pay_agg.amount_paid_total, 0) > 0
		THEN pay_agg.amount_paid_total
        ELSE 0
	END AS refunds_cancelled_order,
    
    -- Total refunds (returns + cancelled)
    pr_agg.refunds_product_returns 
    +
	CASE
		WHEN os.code = 'cancelled'
			AND pr_agg.refunds_product_returns = 0
			AND COALESCE(pay_agg.amount_paid_total, 0) > 0
		THEN pay_agg.amount_paid_total
		ELSE 0.00
	END AS total_refunds,

    -- Net revenue
    CASE
		WHEN COALESCE(pay_agg.amount_paid_total, 0) = 0
		THEN 0
		ELSE pay_agg.amount_paid_total
		-
		(
			pr_agg.refunds_product_returns
			+
			CASE
				WHEN os.code = 'cancelled'
					 AND pr_agg.refunds_product_returns = 0
					 AND COALESCE(pay_agg.amount_paid_total, 0) > 0
				THEN pay_agg.amount_paid_total
				ELSE 0
			END
		)
	END AS net_revenue

FROM orders o
JOIN order_status os ON os.order_status_id = o.order_status_id
JOIN customers cu ON cu.customer_id = o.customer_id
JOIN countries co ON co.country_id = cu.country_id
LEFT JOIN order_items_by_order AS oi_agg ON oi_agg.order_id  = o.order_id
LEFT JOIN product_returns_by_order AS pr_agg ON pr_agg.order_id  = o.order_id
LEFT JOIN paid_payments_by_order AS pay_agg ON pay_agg.order_id = o.order_id;

/* ========================================================================================================================================
   4 - Order Items Enriched
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_order_items_enriched AS
WITH
paid_payments_by_order AS (
	SELECT
		p.order_id,
		SUM(p.amount_paid) AS amount_paid_total
	FROM payments p
	JOIN payment_status ps ON ps.payment_status_id = p.payment_status_id
	WHERE ps.code = 'paid'
	GROUP BY p.order_id
),
product_returns_by_order AS (
	SELECT
		oi.order_id,
		SUM(pr.refund_amount) AS refunds_product_returns_order
	FROM product_returns pr
	JOIN order_items oi ON oi.order_item_id = pr.order_item_id
	GROUP BY oi.order_id
)
SELECT
	-- Identification
	oi.order_item_id,
	oi.order_id,
	
    -- Order context
    co.iso_code AS country,
	o.order_date,
	o.customer_id,
	o.order_status_id,
	os.code AS order_status_code,
	
    -- Product context
    oi.product_id,
	p.name AS product_name,
	p.category_id,
	pc.name AS category_name,
	
    -- Quantities
    oi.quantity,
	oi.unit_price,
	(oi.quantity * oi.unit_price) AS line_gross,
    
    -- Identificar order_items com devolução
    CASE WHEN pr.order_item_id THEN 1 ELSE 0 END AS returned,

	-- Refunds due to product returns (order_item level)
	COALESCE(pr.refund_amount, 0) AS refunds_product_returns,

  -- Refunds due to cancellation after payment
	CASE
		WHEN os.code = 'cancelled'
			AND COALESCE(pay.amount_paid_total, 0) > 0
			AND COALESCE(pro.refunds_product_returns_order, 0) = 0
		THEN (oi.unit_price * oi.quantity)
		ELSE 0
	END AS refunds_cancelled_line,

  -- Total refunds (order_item level)
	(
	COALESCE(pr.refund_amount, 0.00)
	+
	CASE
		WHEN os.code = 'cancelled'
			AND COALESCE(pay.amount_paid_total,0) > 0
			AND COALESCE(pro.refunds_product_returns_order,0) = 0
		THEN (oi.unit_price * oi.quantity)
		ELSE 0.00
	END
	) AS total_refunds_line,
    
	-- Order_items of orders that were not paied
    CASE
		WHEN os.code = 'cancelled'
			AND COALESCE(pay.amount_paid_total, 0) = 0
        THEN 1
        ELSE 0
	END AS cancel_at_payment,

	-- Net revenue (by order_item)
  (
    (oi.quantity * oi.unit_price)
    -
    (
      COALESCE(pr.refund_amount, 0.00)
      +
      CASE
        WHEN os.code = 'cancelled'
         AND COALESCE(pay.amount_paid_total,0) > 0
         AND COALESCE(pro.refunds_product_returns_order,0) = 0
        THEN (oi.unit_price * oi.quantity)
        ELSE 0.00
      END
    )
  ) AS line_net

FROM order_items oi
JOIN orders              o  ON o.order_id = oi.order_id
JOIN order_status        os ON os.order_status_id = o.order_status_id
JOIN products            p  ON p.product_id = oi.product_id
JOIN product_categories  pc ON pc.category_id = p.category_id
JOIN customers cu ON cu.customer_id = o.customer_id
JOIN countries co ON co.country_id = cu.country_id

LEFT JOIN product_returns pr ON pr.order_item_id = oi.order_item_id
LEFT JOIN paid_payments_by_order   AS pay ON pay.order_id = oi.order_id
LEFT JOIN product_returns_by_order AS pro ON pro.order_id = oi.order_id

ORDER BY order_item_id;

/* ========================================================================================================================================
   5 - Returns Enriched
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_returns_enriched AS
SELECT 
  -- grão: return_id (1 linha por registo em product_returns)
  pr.return_id,
  pr.order_item_id,
  oi.order_id,

  -- contexto da encomenda
  o.order_date,
  o.customer_id,
  os.code AS order_status_code,

  -- contexto do produto
  oi.product_id,
  p.name  AS product_name,
  p.category_id,
  pc.name AS category_name,

  -- devolução
  pr.return_date,
  pr.refund_amount,
  rr.code AS return_code,
  rr.name AS return_reason
FROM product_returns pr
JOIN order_items oi        ON oi.order_item_id    = pr.order_item_id
JOIN orders o              ON o.order_id          = oi.order_id
JOIN order_status os       ON os.order_status_id  = o.order_status_id
JOIN products p            ON p.product_id        = oi.product_id
JOIN product_categories pc ON pc.category_id      = p.category_id
JOIN return_reasons rr     ON rr.return_reason_id = pr.return_reason_id
;

/* ========================================================================================================================================
   6 - Last Payments
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_last_payments AS
WITH payments_w AS (
	SELECT
		p.order_id,
		p.payment_id, 
		p.attempt_no, 
		p.payment_date, 
		p.amount_paid,
		p.payment_status_id,
		p.payment_method_id, 
		pm.name AS payment_method_name, 
		ps.name AS payment_status_name, 

		ROW_NUMBER() OVER (
			PARTITION BY p.order_id
			ORDER BY p.attempt_no DESC, p.payment_date DESC, p.payment_id DESC
		) AS rn
		FROM payments p
		JOIN payment_status  ps ON ps.payment_status_id  = p.payment_status_id
		JOIN payment_methods pm ON pm.payment_method_id = p.payment_method_id
)
SELECT
	order_id,
	amount_paid,
	payment_id AS last_payment_id,
	attempt_no AS last_payment_attempt_no,
	payment_date AS last_payment_date,
	amount_paid AS last_payment_amount,
	payment_method_id AS last_payment_method_id,
	payment_method_name,
	payment_status_id,
	payment_status_name
FROM payments_w
WHERE rn = 1;

/* ========================================================================================================================================
   7 - Payment Attempts
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_payment_attempts AS
SELECT
    p.order_id,
    p.payment_id,
    p.attempt_no,
    p.payment_date,
    p.amount_paid,
    p.payment_status_id,
    ps.name AS payment_status_name,
    p.payment_method_id,
    pm.name AS payment_method_name
FROM payments p
JOIN payment_status ps ON ps.payment_status_id = p.payment_status_id
JOIN payment_methods pm ON pm.payment_method_id = p.payment_method_id;

/* ========================================================================================================================================
   8 - Monthly Summary
   ======================================================================================================================================= */
CREATE OR REPLACE VIEW v_monthly_summary AS
WITH
-- First orders per customer
first_order AS (
	SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM v_orders_enriched
    GROUP BY customer_id
),
-- Monthly new buyers
new_buyers AS (
    SELECT
        DATE_FORMAT(first_order_date, '%Y-%m') AS month,
        COUNT(*) AS new_buyers
    FROM first_order
    GROUP BY month
),
-- Monthly orders
monthly_orders AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        COUNT(*) AS total_orders,
        SUM(net_revenue) AS total_revenue
    FROM v_orders_enriched
    GROUP BY month
),
-- Monthly returning buyers
returning_buyers AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        COUNT(DISTINCT customer_id) AS returning_buyers
    FROM v_orders_enriched
    WHERE customer_id IN (
        SELECT customer_id
        FROM first_order
        WHERE order_date > first_order_date
    )
    GROUP BY month
)
-- FINAL COMBINED TABLE
SELECT
    mo.month,
    mo.total_orders,
    mo.total_revenue,
    nb.new_buyers,
    rb.returning_buyers
FROM monthly_orders mo
LEFT JOIN new_buyers nb ON nb.month = mo.month
LEFT JOIN returning_buyers rb ON rb.month = mo.month
ORDER BY mo.month;

/* ============================================== END ========================================================= */