/* ===========================
   ECOMMERCE DB
   - Requer MySQL 8.0.16+
   =========================== */

/* ===========================
   DATABASE DEFAULTS
   =========================== */
DROP DATABASE IF EXISTS ecommerce_db;

CREATE DATABASE IF NOT EXISTS ecommerce_db
	DEFAULT CHARACTER SET utf8mb4
	DEFAULT COLLATE utf8mb4_unicode_ci;
    
USE ecommerce_db;
  
   /* ===========================
   LOOKUPS
   =========================== */

CREATE TABLE countries (
	country_id SMALLINT UNSIGNED AUTO_INCREMENT,
	name VARCHAR(100) NOT NULL,
	iso_code CHAR(2) NOT NULL,
    CONSTRAINT pk_countries PRIMARY KEY (country_id),
    CONSTRAINT uq_countries_iso UNIQUE (iso_code)
) ENGINE=InnoDB;

CREATE TABLE product_categories (
    category_id INT UNSIGNED AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    CONSTRAINT pk_product_categories PRIMARY KEY (category_id),
    CONSTRAINT uq_product_categories_name UNIQUE (name)
) ENGINE=InnoDB;

CREATE TABLE order_status (
  order_status_id TINYINT UNSIGNED AUTO_INCREMENT,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(100) NOT NULL,
  sort_order TINYINT UNSIGNED NOT NULL,
  is_final BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT pk_order_status PRIMARY KEY (order_status_id),
  CONSTRAINT uq_order_status_code UNIQUE (code)
) ENGINE=InnoDB;
   
CREATE TABLE payment_methods (
  payment_method_id TINYINT UNSIGNED AUTO_INCREMENT,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(100) NOT NULL,
  CONSTRAINT pk_payment_methods PRIMARY KEY (payment_method_id),
  CONSTRAINT uq_payment_methods_code UNIQUE (code)
) ENGINE=InnoDB;

CREATE TABLE payment_status (
  payment_status_id TINYINT UNSIGNED AUTO_INCREMENT,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(100) NOT NULL,
  is_final BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT pk_payment_status PRIMARY KEY (payment_status_id),
  CONSTRAINT uq_payment_status_code UNIQUE (code)
) ENGINE=InnoDB;

CREATE TABLE return_reasons (
  return_reason_id TINYINT UNSIGNED AUTO_INCREMENT,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(150) NOT NULL,
  CONSTRAINT pk_return_reasons PRIMARY KEY (return_reason_id),
  CONSTRAINT uq_return_reasons_code UNIQUE (code)
) ENGINE=InnoDB;

/* ===========================
   PRODUCTS
   =========================== */

CREATE TABLE products(
	product_id INT UNSIGNED AUTO_INCREMENT,
	name VARCHAR(150) NOT NULL,
	price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
	stock_quantity INT UNSIGNED NOT NULL DEFAULT 0,
	category_id INT UNSIGNED NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES product_categories (category_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_products_category (category_id)
) ENGINE=InnoDB;

/* ===========================
   CUSTOMERS
   =========================== */

CREATE TABLE customers (
	customer_id INT UNSIGNED AUTO_INCREMENT,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    city VARCHAR(100),
	country_id SMALLINT UNSIGNED NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT fk_customers_country FOREIGN KEY (country_id) REFERENCES countries(country_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_customers_country (country_id)
) ENGINE=InnoDB;

/* ===========================
   ORDERS & ORDER ITEMS
   =========================== */
   
CREATE TABLE orders (
	order_id INT UNSIGNED AUTO_INCREMENT,
	customer_id INT UNSIGNED NOT NULL,
	order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	order_status_id TINYINT UNSIGNED NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	CONSTRAINT pk_orders PRIMARY KEY (order_id),
	CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	CONSTRAINT fk_orders_status FOREIGN KEY (order_status_id) REFERENCES order_status(order_status_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_orders_customer_date (customer_id, order_date),
	INDEX idx_orders_status (order_status_id),
    INDEX idx_orders_order_date (order_date)
) ENGINE=InnoDB;

CREATE TABLE order_items (
	order_item_id INT UNSIGNED AUTO_INCREMENT,
	order_id INT UNSIGNED NOT NULL,
	product_id INT UNSIGNED NOT NULL,
	quantity INT UNSIGNED NOT NULL CHECK (quantity > 0),
	unit_price DECIMAL(12,2) NOT NULL CHECK (unit_price >= 0),
	CONSTRAINT pk_order_items PRIMARY KEY (order_item_id),
	CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
		ON UPDATE RESTRICT ON DELETE CASCADE,
	CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_order_items_order_product (order_id, product_id)
) ENGINE=InnoDB;

/* ===========================
   PAYMENTS & RETURNS
   =========================== */

CREATE TABLE payments (
	payment_id INT UNSIGNED AUTO_INCREMENT,
	order_id INT UNSIGNED NOT NULL,
	payment_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	amount_paid DECIMAL(12,2) NOT NULL CHECK (amount_paid >= 0),
	payment_method_id TINYINT UNSIGNED NOT NULL,
	payment_status_id TINYINT UNSIGNED NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	CONSTRAINT pk_payments PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
		ON UPDATE RESTRICT ON DELETE CASCADE,
	CONSTRAINT fk_payments_method FOREIGN KEY (payment_method_id) REFERENCES payment_methods(payment_method_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	CONSTRAINT fk_payments_status FOREIGN KEY (payment_status_id) REFERENCES payment_status(payment_status_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_payments_order (order_id),
	INDEX idx_payments_method (payment_method_id),
	INDEX idx_payments_status (payment_status_id),
    INDEX idx_payments_payment_date (payment_date)
) ENGINE=InnoDB;

CREATE TABLE product_returns (
	return_id INT UNSIGNED AUTO_INCREMENT,
	order_item_id INT UNSIGNED NOT NULL,
	return_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	refund_amount DECIMAL(12,2) NOT NULL CHECK (refund_amount >= 0),
	return_reason_id TINYINT UNSIGNED NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	CONSTRAINT pk_product_returns PRIMARY KEY (return_id),
	CONSTRAINT fk_product_returns_order_item FOREIGN KEY (order_item_id) REFERENCES order_items(order_item_id)
		ON UPDATE RESTRICT ON DELETE CASCADE,
	CONSTRAINT fk_product_returns_reason FOREIGN KEY (return_reason_id) REFERENCES return_reasons(return_reason_id)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	INDEX idx_product_returns_order_item (order_item_id),
	INDEX idx_product_returns_reason (return_reason_id)
) ENGINE=InnoDB;




