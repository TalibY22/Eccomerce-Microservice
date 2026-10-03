package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	productv1 "gen/product/v1"
	userv1 "gen/user/v1"

	"orders/internal/domain"
	"orders/internal/events"
	"orders/internal/repository"
)

var ErrUserNotFound = errors.New("user not found")
var ErrProductNotFound = errors.New("product not found")
var ErrInvalidOrder = errors.New("order must include items with positive quantities")

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
	if userID == "" || len(items) == 0 {
		return nil, ErrInvalidOrder
	}
	for _, item := range items {
		if item.ProductID == "" || item.Quantity <= 0 {
			return nil, ErrInvalidOrder
		}
	}
	_, err := s.userClient.GetUser(ctx, &userv1.GetUserRequest{Id: userID})
	if err != nil {
		return nil, ErrUserNotFound
	}

	var orderItems []domain.OrderItem
	var total float64
	currency := ""

	for _, item := range items {
		product, err := s.productClient.GetProduct(ctx, &productv1.GetProductRequest{Id: item.ProductID})
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrProductNotFound, item.ProductID)
		}
		if !product.GetActive() {
			return nil, fmt.Errorf("%w: %s", ErrProductNotFound, item.ProductID)
		}
		if currency == "" {
			currency = product.GetCurrency()
		} else if currency != product.GetCurrency() {
			return nil, ErrInvalidOrder
		}
		orderItems = append(orderItems, domain.OrderItem{
			ProductID:   item.ProductID,
			SKU:         product.GetSku(),
			ProductName: product.GetName(),
			Quantity:    item.Quantity,
			UnitPrice:   product.GetPrice(),
		})
		total += product.GetPrice() * float64(item.Quantity)
	}

	o := &domain.Order{
		ID:          uuid.NewString(),
		UserID:      userID,
		Currency:    currency,
		Items:       orderItems,
		TotalAmount: total,
		Status:      "pending",
	}

	// Store the order and its event in one transaction; a relay publishes the outbox.
	evtItems := make([]events.OrderItemEvent, len(o.Items))
	for i, it := range o.Items {
		evtItems[i] = events.OrderItemEvent{ProductID: it.ProductID, SKU: it.SKU, ProductName: it.ProductName, Quantity: it.Quantity, UnitPrice: it.UnitPrice}
	}

	payload, err := json.Marshal(events.OrderCreatedEvent{
		EventID:       o.ID,
		EventType:     "order.created",
		SchemaVersion: 1,
		OrderID:       o.ID,
		UserID:        o.UserID,
		Items:         evtItems,
		TotalAmount:   o.TotalAmount,
		Status:        o.Status,
		Currency:      o.Currency,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, o, payload); err != nil {
		return nil, err
	}
	return o, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	return s.repo.GetByID(ctx, id)
}
