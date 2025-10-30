INSERT INTO countries (name, iso_code) VALUES
	('Portugal', 'PT'),
	('Spain', 'ES'),
	('France', 'FR'),
    ('Germany', 'DE'),
	('Italy', 'IT'),
	('Netherlands', 'NL'),
	('Belgium', 'BE'),
	('Greece', 'GR'),
	('Croatia', 'HR'),
	('Ireland', 'IE');

INSERT INTO product_categories (name) VALUES
	('Electronics'),
	('Fashion'),
	('Home & Kitchen'),
	('Beauty & Personal Care'),
	('Sports & Fitness'),
	('Books'),
	('Toys'),
	('Gardening'),
	('Automotive'),
	('Pet Supplies');

INSERT INTO order_status (code, name, sort_order, is_final) VALUES
	('placed', 'Placed', 1, FALSE),
	('paid', 'Paid', 2, FALSE),
	('shipped', 'Shipped', 3, FALSE),
	('delivered', 'Delivered', 4, TRUE),
	('cancelled', 'Cancelled', 5, TRUE);
    
INSERT INTO payment_methods (code, name) VALUES
	('card','Credit/debit card'),
    ('paypal','PayPal'),
    ('mbway','MB Way'),
    ('bank_transfer', 'Bank transfer');
    
INSERT INTO payment_status (code, name, is_final) VALUES
	('paid','Paid',TRUE),
	('pending','Pending',FALSE),
	('failed','Failed',TRUE),
    ('refunded','Refunded',TRUE);
    
INSERT INTO return_reasons (code, name) VALUES
	('damaged','Damaged product'),
	('not_as_described','Not as described'),
	('late','Late delivery'),
	('change_of_mind','Change of mind'),
	('other','Other reason');