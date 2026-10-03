CREATE TABLE IF NOT EXISTS event_inbox (
    event_id VARCHAR(128) PRIMARY KEY,
    topic VARCHAR(160) NOT NULL,
    processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_outbox (
    id CHAR(36) PRIMARY KEY,
    topic VARCHAR(160) NOT NULL,
    message_key VARCHAR(160) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6) NULL,
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    INDEX idx_event_outbox_pending (published_at,created_at)
) ENGINE=InnoDB;
