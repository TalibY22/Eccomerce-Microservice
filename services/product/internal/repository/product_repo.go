package repository

import (
	"context"
	"database/sql"
	"errors"
	"products/internal/domain"
)

var ErrNotFound = errors.New("product not found")

type ProductRepository struct {
	db *sql.DB
}

func NewProductRepository(db *sql.DB) *ProductRepository {
	return &ProductRepository{db: db}
}

func (r *ProductRepository) Create(ctx context.Context, p *domain.Product) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO products (id, sku, name, description, category, price, currency, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.SKU, p.Name, p.Description, p.Category, p.Price, p.Currency, p.Active,
	)
	return err
}

func (r *ProductRepository) GetByID(ctx context.Context, id string) (*domain.Product, error) {
	var p domain.Product
	err := r.db.QueryRowContext(ctx,
		`SELECT id, sku, name, description, category, price, currency, active FROM products WHERE id = ?`, id,
	).Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category, &p.Price, &p.Currency, &p.Active)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}

func (r *ProductRepository) List(ctx context.Context) ([]*domain.Product, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, sku, name, description, category, price, currency, active FROM products WHERE active = TRUE ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []*domain.Product
	for rows.Next() {
		var p domain.Product
		if err := rows.Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category, &p.Price, &p.Currency, &p.Active); err != nil {
			return nil, err
		}
		products = append(products, &p)
	}
	return products, rows.Err()
}
