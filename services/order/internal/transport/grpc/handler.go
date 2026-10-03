package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderv1 "gen/order/v1"
	"orders/internal/repository"
	"orders/internal/service"
)

type Handler struct {
	orderv1.UnimplementedOrderServiceServer
	svc *service.OrderService
}

func NewHandler(svc *service.OrderService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	var items []service.CreateOrderItem
	for _, i := range req.GetItems() {
		items = append(items, service.CreateOrderItem{
			ProductID: i.GetProductId(),
			Quantity:  i.GetQuantity(),
		})
	}

	o, err := h.svc.CreateOrder(ctx, req.GetUserId(), items)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidOrder):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, service.ErrUserNotFound):
			return nil, status.Error(codes.NotFound, "user not found")
		case errors.Is(err, service.ErrProductNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, "failed to create order")
		}
	}

	return &orderv1.CreateOrderResponse{
		Id:          o.ID,
		TotalAmount: o.TotalAmount,
		Status:      o.Status,
		Currency:    o.Currency,
	}, nil
}

func (h *Handler) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	o, err := h.svc.GetOrder(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		return nil, status.Error(codes.Internal, "failed to get order")
	}

	pbItems := make([]*orderv1.OrderItem, len(o.Items))
	for i, item := range o.Items {
		pbItems[i] = &orderv1.OrderItem{
			ProductId:   item.ProductID,
			Quantity:    item.Quantity,
			Sku:         item.SKU,
			ProductName: item.ProductName,
		}
	}

	return &orderv1.GetOrderResponse{
		Id:          o.ID,
		UserId:      o.UserID,
		TotalAmount: o.TotalAmount,
		Status:      o.Status,
		Items:       pbItems,
		Currency:    o.Currency,
	}, nil
}
