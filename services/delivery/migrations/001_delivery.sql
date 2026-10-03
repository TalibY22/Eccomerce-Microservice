CREATE TABLE IF NOT EXISTS shipments (
 id CHAR(36) PRIMARY KEY, order_id VARCHAR(128) NOT NULL UNIQUE, user_id VARCHAR(128) NULL,
 carrier VARCHAR(80) NULL, tracking_number VARCHAR(160) NULL, status VARCHAR(40) NOT NULL DEFAULT 'awaiting_address',
 shipping_address JSON NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
 INDEX idx_shipments_tracking(tracking_number)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS delivery_timeline (
 id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, shipment_id CHAR(36) NOT NULL, event_key VARCHAR(160) NOT NULL,
 status VARCHAR(40) NOT NULL, description VARCHAR(500) NULL, location VARCHAR(255) NULL, occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY uq_delivery_event(shipment_id,event_key), INDEX idx_delivery_timeline(shipment_id,occurred_at),
 CONSTRAINT fk_delivery_timeline_shipment FOREIGN KEY(shipment_id) REFERENCES shipments(id)
) ENGINE=InnoDB;
