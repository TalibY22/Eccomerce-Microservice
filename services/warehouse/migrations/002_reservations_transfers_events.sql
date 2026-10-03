CREATE TABLE IF NOT EXISTS order_reservations (
    order_id CHAR(36) PRIMARY KEY,
    state ENUM('reserved','committed','released','rejected') NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS order_reservation_items (
    order_id CHAR(36) NOT NULL,
    warehouse_id CHAR(36) NOT NULL,
    product_id CHAR(36) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INT UNSIGNED NOT NULL,
    PRIMARY KEY (order_id, warehouse_id, product_id),
    CONSTRAINT fk_reservation_order FOREIGN KEY (order_id) REFERENCES order_reservations(order_id) ON DELETE CASCADE,
    CONSTRAINT fk_reservation_inventory FOREIGN KEY (warehouse_id, product_id) REFERENCES inventory_balances(warehouse_id, product_id),
    CONSTRAINT chk_reservation_quantity CHECK (quantity > 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stock_transfers (
    id CHAR(36) PRIMARY KEY,
    source_warehouse_id CHAR(36) NOT NULL,
    destination_warehouse_id CHAR(36) NOT NULL,
    status ENUM('in_transit','received','cancelled') NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    received_at TIMESTAMP(6) NULL,
    INDEX idx_transfers_status_time (status, created_at),
    CONSTRAINT fk_transfer_source FOREIGN KEY (source_warehouse_id) REFERENCES warehouses(id),
    CONSTRAINT fk_transfer_destination FOREIGN KEY (destination_warehouse_id) REFERENCES warehouses(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stock_transfer_items (
    transfer_id CHAR(36) NOT NULL,
    product_id CHAR(36) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INT UNSIGNED NOT NULL,
    PRIMARY KEY (transfer_id, product_id),
    CONSTRAINT fk_transfer_item FOREIGN KEY (transfer_id) REFERENCES stock_transfers(id) ON DELETE CASCADE,
    CONSTRAINT chk_transfer_quantity CHECK (quantity > 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS processed_orders (
    order_id CHAR(36) PRIMARY KEY,
    outcome VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS outbox_events (
    id CHAR(36) PRIMARY KEY,
    topic VARCHAR(120) NOT NULL,
    message_key VARCHAR(120) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6) NULL,
    INDEX idx_outbox_pending (published_at, created_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS legacy_inventory_imports (
    product_id CHAR(36) PRIMARY KEY,
    imported_quantity INT UNSIGNED NOT NULL,
    imported_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
