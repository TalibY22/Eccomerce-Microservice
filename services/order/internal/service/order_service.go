
package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	productv1 "gen/product/v1"
	userv1 "gen/user/v1"

	"orders/internal/domain"
	"orders/internal/repository"
)

var ErrUserNotFound = errors.New("user not found")
var ErrProductNotFound = errors.New("product not found")
var ErrInsufficientStock = errors.New("insufficient stock")

type OrderService struct {
	repo          *repository.OrderRepository
	userClient    userv1.UserServiceClient
	productClient productv1.ProductServiceClient
}

func NewOrderService(repo *repository.OrderRepository, userClient userv1.UserServiceClient, productClient productv1.ProductServiceClient) *OrderService {
	return &OrderService{repo: repo, userClient: userClient, productClient: productClient}
}

type CreateOrderItem struct {
	ProductID string
	Quantity  int32
}

func (s *OrderService) CreateOrder(ctx context.Context, userID string, items []CreateOrderItem) (*domain.Order, error) {
	// 1. Validate the user actually exists — cross-service gRPC call to User service
	_, err := s.userClient.GetUser(ctx, &userv1.GetUserRequest{Id: userID})
	if err != nil {
		return nil, ErrUserNotFound
	}

	// 2. For each item, fetch the real product (never trust price from the caller)
	var orderItems []domain.OrderItem
	var total float64

	for _, item := range items {
		product, err := s.productClient.GetProduct(ctx, &productv1.GetProductRequest{Id: item.ProductID})
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrProductNotFound, item.ProductID)
		}

		if product.GetStock() < item.Quantity {
			return nil, fmt.Errorf("%w: %s", ErrInsufficientStock, item.ProductID)
		}

		orderItems = append(orderItems, domain.OrderItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			UnitPrice: product.GetPrice(), // snapshot price NOW, at order time
		})

		total += product.GetPrice() * float64(item.Quantity)
	}

	// 3. Persist the order
	o := &domain.Order{
		ID:          uuid.NewString(),
		UserID:      userID,
		Items:       orderItems,
		TotalAmount: total,
		Status:      "pending",
	}

	if err := s.repo.Create(ctx, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	return s.repo.GetByID(ctx, id)
}