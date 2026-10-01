package service

import (
	"context"
	"products/internal/repository"

	"github.com/google/uuid"

	"products/internal/domain"
)

type ProductService struct {
	repo *repository.ProductRepository

}

func NewProductService(repo *repository.ProductRepository) *ProductService {
	return &ProductService{repo: repo}
}

func (s *ProductService) CreateProduct(ctx context.Context, name, description string, price float64, stock int32) (*domain.Product, error) {
	p := &domain.Product{
		ID:          uuid.NewString(),
		Name:        name,
		Description: description,
		Price:       price,
		Stock:       stock,
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