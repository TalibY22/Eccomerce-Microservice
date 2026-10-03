package service

import (
	"context"
	"errors"
	"strings"

	"products/internal/domain"
	"products/internal/repository"

	"github.com/google/uuid"
)

var ErrInvalidProduct = errors.New("invalid product")

type ProductService struct {
	repo *repository.ProductRepository
}

func NewProductService(repo *repository.ProductRepository) *ProductService {
	return &ProductService{repo: repo}
}

func (s *ProductService) CreateProduct(ctx context.Context, sku, name, description, category, currency string, price float64) (*domain.Product, error) {
	sku = strings.TrimSpace(strings.ToUpper(sku))
	name = strings.TrimSpace(name)
	category = strings.TrimSpace(category)
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if sku == "" || len(sku) > 64 || name == "" || category == "" || price < 0 {
		return nil, ErrInvalidProduct
	}
	if currency == "" {
		currency = "USD"
	}
	if len(currency) != 3 {
		return nil, ErrInvalidProduct
	}
	p := &domain.Product{
		ID:          uuid.NewString(),
		SKU:         sku,
		Category:    category,
		Currency:    currency,
		Active:      true,
		Name:        name,
		Description: description,
		Price:       price,
	}
	if err := s.repo.Create(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *ProductService) GetProduct(ctx context.Context, id string) (*domain.Product, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *ProductService) ListProducts(ctx context.Context) ([]*domain.Product, error) {
	return s.repo.List(ctx)
}
