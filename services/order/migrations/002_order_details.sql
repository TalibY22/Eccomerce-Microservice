ALTER TABLE orders
    MODIFY id CHAR(36) NOT NULL,
    MODIFY user_id CHAR(36) NOT NULL,
    MODIFY total_amount DECIMAL(12,2) NOT NULL,
    ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'USD',
    ADD COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    ADD INDEX idx_orders_user_created (user_id, created_at),
    ADD INDEX idx_orders_status_created (status, created_at),
    ADD CONSTRAINT chk_orders_total CHECK (total_amount >= 0);

CREATE TABLE order_items (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id CHAR(36) NOT NULL,
    product_id CHAR(36) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT UNSIGNED NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT chk_order_items_quantity CHECK (quantity > 0),
    CONSTRAINT chk_order_items_unit_price CHECK (unit_price >= 0),
    INDEX idx_order_items_product (product_id)
) ENGINE=InnoDB;

INSERT INTO order_items (order_id, product_id, sku, product_name, quantity, unit_price)
SELECT o.id, legacy.product_id, CONCAT('LEGACY-', legacy.product_id), 'Legacy product', legacy.quantity, legacy.unit_price
FROM orders AS o
JOIN JSON_TABLE(o.items, '$[*]' COLUMNS (
    product_id CHAR(36) PATH '$.ProductID',
    quantity INT PATH '$.Quantity',
    unit_price DECIMAL(12,2) PATH '$.UnitPrice'
)) AS legacy;

ALTER TABLE orders DROP COLUMN items;
