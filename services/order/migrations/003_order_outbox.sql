CREATE TABLE IF NOT EXISTS order_outbox_events (
    id CHAR(36) PRIMARY KEY,
    aggregate_id CHAR(36) NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    payload JSON NOT NULL,
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6) NULL,
    last_error VARCHAR(1000) NULL,
    INDEX idx_order_outbox_pending (published_at, created_at)
) ENGINE=InnoDB;
