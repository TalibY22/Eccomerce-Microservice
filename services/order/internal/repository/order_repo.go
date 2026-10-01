
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"orders/internal/domain"
)

var ErrNotFound = errors.New("order not found")

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

func (r *OrderRepository) Create(ctx context.Context, o *domain.Order) error {
	itemsJSON, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}

	_, err = r.db.ExecContext(ctx,
		`INSERT INTO orders (id, user_id, items, total_amount, status) VALUES (?, ?, ?, ?, ?)`,
		o.ID, o.UserID, itemsJSON, o.TotalAmount, o.Status,
	)
	return err
}

func (r *OrderRepository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	var o domain.Order
	var itemsJSON []byte

	err := r.db.QueryRowContext(ctx,
		`SELECT id, user_id, items, total_amount, status FROM orders WHERE id = ?`, id,
	).Scan(&o.ID, &o.UserID, &itemsJSON, &o.TotalAmount, &o.Status)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
		return nil, err
	}
	return &o, nil
}