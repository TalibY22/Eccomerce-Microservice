package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type OrderStatusEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
}

func NewStatusConsumer(brokerAddr string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddr},
		GroupID:     "order-status-v1",
		GroupTopics: []string{"order.confirmed", "order.rejected"},
		MinBytes:    1,
		MaxBytes:    10e6,
	})
}

func DecodeOrderStatus(payload []byte) (OrderStatusEvent, error) {
	var event OrderStatusEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return event, err
	}
	if event.EventID == "" || event.OrderID == "" {
		return event, fmt.Errorf("status event is missing event_id or order_id")
	}
	switch event.Status {
	case "confirmed", "rejected":
	default:
		return event, fmt.Errorf("unsupported order status %q", event.Status)
	}
	return event, nil
}

func FetchStatus(ctx context.Context, reader *kafka.Reader) (kafka.Message, error) {
	return reader.FetchMessage(ctx)
}
