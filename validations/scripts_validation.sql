/* =====================================================================
Validation: Total number of customers
Expected Result: N_CUSTOMERS (10 000)
Defined at: insert_customers.py
Result: PASSED
======================================================================= */
SELECT
	Count(*) AS number_of_customers
FROM customers;

/* =====================================================================
Validation: Customers Country Distribution
Expected Result: COUNTRY_DISTRIBUTION
Defined at: insert_customers.py
Result: PASSED
======================================================================== */
SELECT
	co.name AS country,
    COUNT(*) AS customers,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_customers
FROM customers cu
JOIN countries co ON co.country_id = cu.country_id
GROUP BY cu.country_id
ORDER BY pct_customers DESC;

/* =====================================================================
Validation: Customers Creation Date Interval
Expected Result: CUSTOMERS_START, CUSTOMERS_END_EXCL
Defined at: insert_customers.py
Result: PASSED
======================================================================== */
SELECT 
	MIN(created_at) AS first_customer_date,
    MAX(created_at) AS last_customer_date
FROM customers;

/* =====================================================================
Validation: Customers Age Group
Expected Result: AGE_GROUPS
Defined at: insert_customers.py
Result: PASSED
======================================================================== */
WITH ages AS (
SELECT 
	cu.customer_id,
    cu.first_name,
    cu.last_name,
    cu.birth_date,
    TIMESTAMPDIFF(YEAR, cu.birth_date, CURRENT_DATE) AS customer_age
FROM customers cu
WHERE cu.birth_date IS NOT NULL
)
SELECT
	CASE 
		WHEN customer_age BETWEEN 18 AND 29 THEN '18-29'
        WHEN customer_age BETWEEN 30 AND 64 THEN '30-64'
        WHEN customer_age BETWEEN 65 AND 80 THEN '65-80'
        WHEN customer_age > 80 THEN '80+'
		ELSE 'Under 18'
	END AS age_range,
    COUNT(*) AS customers,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ages
GROUP BY age_range
ORDER BY
  CASE age_range
    WHEN 'Under 18' THEN 1
    WHEN '18-29' THEN 2
    WHEN '30-64' THEN 3
    WHEN '65-80' THEN 4
    WHEN '80+' THEN 5
  END;

/* =====================================================================
Validation: Customers under age at created_at
Expected Result: 0
Defined at: insert_customers.py
Result: NOT PASSED
======================================================================== */
SELECT 
	COUNT(*) AS invalid_customers
FROM customers
WHERE birth_date IS NOT NULL
AND created_at < DATE_ADD(birth_date, INTERVAL 18 YEAR);


/* =====================================================================
Validation: Customers Without Valid Country
Expected Result: 0
Defined at: insert_customers.py
Result: PASSED
======================================================================== */
SELECT 
	COUNT(*) AS orphan_customers
FROM customers cu
LEFT JOIN countries co ON co.country_id = cu.country_id
WHERE co.country_id IS NULL;







/* =====================================================================
Validation: Repeted customers (equal first_name, last_name and birth_date)
Expected Result: N_CUSTOMERS (10 000)
Defined at: insert_customers.py
Result: PASSED
======================================================================== */
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CONCAT(first_name, '|', last_name, '|', birth_date)) AS logical_uniques
FROM customers;















