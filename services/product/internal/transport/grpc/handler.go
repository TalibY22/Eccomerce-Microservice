package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	productv1 "gen/product/v1"
	"products/internal/repository"
	"products/internal/service"
)

type Handler struct {
	productv1.UnimplementedProductServiceServer
	svc *service.ProductService
}

func NewHandler(svc *service.ProductService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) CreateProduct(ctx context.Context, req *productv1.CreateProductRequest) (*productv1.CreateProductResponse, error) {
	p, err := h.svc.CreateProduct(ctx, req.GetSku(), req.GetName(), req.GetDescription(), req.GetCategory(), req.GetCurrency(), req.GetPrice())
	if err != nil {
		if errors.Is(err, service.ErrInvalidProduct) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to create product")
	}
	return &productv1.CreateProductResponse{Id: p.ID}, nil
}

func (h *Handler) GetProduct(ctx context.Context, req *productv1.GetProductRequest) (*productv1.GetProductResponse, error) {
	p, err := h.svc.GetProduct(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "product not found")
		}
		return nil, status.Error(codes.Internal, "failed to get product")
	}

	return &productv1.GetProductResponse{
		Id:          p.ID,
		Sku:         p.SKU,
		Name:        p.Name,
		Description: p.Description,
		Category:    p.Category,
		Currency:    p.Currency,
		Active:      p.Active,
		Price:       p.Price,
	}, nil
}

func (h *Handler) ListProducts(ctx context.Context, req *productv1.ListProductsRequest) (*productv1.ListProductsResponse, error) {
	products, err := h.svc.ListProducts(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list products")
	}

	pbProducts := make([]*productv1.Product, len(products))
	for i, p := range products {
		pbProducts[i] = &productv1.Product{
			Id:          p.ID,
			Sku:         p.SKU,
			Name:        p.Name,
			Description: p.Description,
			Category:    p.Category,
			Currency:    p.Currency,
			Active:      p.Active,
			Price:       p.Price,
		}
	}

	return &productv1.ListProductsResponse{Products: pbProducts}, nil
}
