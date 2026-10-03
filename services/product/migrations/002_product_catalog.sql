ALTER TABLE products ADD COLUMN sku VARCHAR(64) NULL, ADD COLUMN category VARCHAR(120) NOT NULL DEFAULT '', ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'USD', ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE, ADD COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);
UPDATE products SET sku = CONCAT('LEGACY-', id) WHERE sku IS NULL;
UPDATE products SET description = '' WHERE description IS NULL;
ALTER TABLE products MODIFY id CHAR(36) NOT NULL, MODIFY sku VARCHAR(64) NOT NULL, MODIFY price DECIMAL(12,2) NOT NULL, MODIFY description TEXT NOT NULL, ADD UNIQUE KEY uq_products_sku (sku), ADD INDEX idx_products_category_active (category, active), ADD INDEX idx_products_created_at (created_at), ADD CONSTRAINT chk_products_price CHECK (price >= 0);
