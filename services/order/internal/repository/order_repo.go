package repository

import (
	"context"
	"database/sql"
	"errors"

	"orders/internal/domain"
)

var ErrNotFound = errors.New("order not found")

type OrderRepository struct{ db *sql.DB }

func NewOrderRepository(db *sql.DB) *OrderRepository { return &OrderRepository{db: db} }

func (r *OrderRepository) Create(ctx context.Context, o *domain.Order, eventPayload []byte) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx,
		`INSERT INTO orders (id, user_id, total_amount, currency, status) VALUES (?, ?, ?, ?, ?)`,
		o.ID, o.UserID, o.TotalAmount, o.Currency, o.Status); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO order_items (order_id, product_id, sku, product_name, quantity, unit_price) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, item := range o.Items {
		if _, err := stmt.ExecContext(ctx, o.ID, item.ProductID, item.SKU, item.ProductName, item.Quantity, item.UnitPrice); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO order_outbox_events (id, aggregate_id, event_type, payload) VALUES (?, ?, 'order.created', ?)`, o.ID, o.ID, eventPayload); err != nil {
		return err
	}
	return tx.Commit()
}

type OutboxEvent struct {
	ID          string
	AggregateID string
	Payload     []byte
}

func (r *OrderRepository) PendingEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id, aggregate_id, payload FROM order_outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.Payload); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (r *OrderRepository) MarkEventPublished(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE order_outbox_events SET published_at=CURRENT_TIMESTAMP(6), last_error=NULL WHERE id=?`, id)
	return err
}

func (r *OrderRepository) RecordEventFailure(ctx context.Context, id string, cause error) error {
	_, err := r.db.ExecContext(ctx, `UPDATE order_outbox_events SET attempts=attempts+1, last_error=? WHERE id=?`, cause.Error(), id)
	return err
}

func (r *OrderRepository) ApplyOrderEvent(ctx context.Context, eventID, orderID, eventType, status string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT IGNORE INTO order_event_inbox (event_id, order_id, event_type) VALUES (?, ?, ?)`, eventID, orderID, eventType)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		return tx.Commit()
	}
	update, err := tx.ExecContext(ctx, `UPDATE orders SET status=? WHERE id=? AND status='pending'`, status, orderID)
	if err != nil {
		return err
	}
	changed, err := update.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM orders WHERE id=?`, orderID).Scan(&exists); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *OrderRepository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	var o domain.Order
	err := r.db.QueryRowContext(ctx,
		`SELECT id, user_id, total_amount, currency, status FROM orders WHERE id = ?`, id,
	).Scan(&o.ID, &o.UserID, &o.TotalAmount, &o.Currency, &o.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT product_id, sku, product_name, quantity, unit_price FROM order_items WHERE order_id = ? ORDER BY id`, o.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item domain.OrderItem
		if err := rows.Scan(&item.ProductID, &item.SKU, &item.ProductName, &item.Quantity, &item.UnitPrice); err != nil {
			return nil, err
		}
		o.Items = append(o.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &o, nil
}
