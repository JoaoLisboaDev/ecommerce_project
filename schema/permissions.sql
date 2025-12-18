/* ============================================
   File: permissions.sql
   Purpose: role-based, read-only access for ecommerce_db
   Requirements: MySQL 8.0+
   ============================================ */

/* ============================================
   ROLE: Read-only
   ============================================ */

-- Creation of a new Role that will only have read access
CREATE ROLE IF NOT EXISTS readonly;

-- Grant privileges to the created Role
GRANT 
	SELECT,
    SHOW VIEW
ON ecommerce_db.* TO readonly;

/* ============================================
   USERS
   Replace <HOST> and <PASSWORD> before running
   ============================================ */

-- Human analyst account
CREATE USER IF NOT EXISTS 'data_analyst'@'<HOST>'
  IDENTIFIED BY '<STRONG_PASSWORD_DATA_ANALYST>';
GRANT readonly TO 'data_analyst'@'<HOST>';
SET DEFAULT ROLE readonly TO 'data_analyst'@'<HOST>';

-- BI / dashboard service account
CREATE USER IF NOT EXISTS 'dashboard_app'@'<HOST>'
  IDENTIFIED BY '<STRONG_PASSWORD_DASHBOARD_APP>';
GRANT readonly TO 'dashboard_app'@'<HOST>';
SET DEFAULT ROLE readonly TO 'dashboard_app'@'<HOST>';