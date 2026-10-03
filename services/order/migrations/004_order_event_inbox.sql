CREATE TABLE IF NOT EXISTS order_event_inbox (
    event_id CHAR(36) PRIMARY KEY,
    order_id CHAR(36) NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_order_inbox_order (order_id, processed_at)
) ENGINE=InnoDB;
