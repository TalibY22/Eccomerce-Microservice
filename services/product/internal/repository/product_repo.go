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
		`INSERT INTO products (id, name, description, price, stock) VALUES (?, ?, ?, ?, ?)`,
		p.ID, p.Name, p.Description, p.Price, p.Stock,
	)
	return err
}

func (r *ProductRepository) GetByID(ctx context.Context, id string) (*domain.Product, error) {
	var p domain.Product
	err := r.db.QueryRowContext(ctx,
		`SELECT id, name, description, price, stock FROM products WHERE id = ?`, id,
	).Scan(&p.ID, &p.Name, &p.Description, &p.Price, &p.Stock)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}

func (r *ProductRepository) List(ctx context.Context) ([]*domain.Product, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, description, price, stock FROM products`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []*domain.Product
	for rows.Next() {
		var p domain.Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Price, &p.Stock); err != nil {
			return nil, err
		}
		products = append(products, &p)
	}
	return products, rows.Err()
}