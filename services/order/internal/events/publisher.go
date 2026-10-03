package events

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderItemEvent struct {
	ProductID   string  `json:"product_id"`
	SKU         string  `json:"sku"`
	ProductName string  `json:"product_name"`
	Quantity    int32   `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
}

type OrderCreatedEvent struct {
	EventID       string           `json:"event_id"`
	EventType     string           `json:"event_type"`
	SchemaVersion int              `json:"schema_version"`
	OrderID       string           `json:"order_id"`
	UserID        string           `json:"user_id"`
	Items         []OrderItemEvent `json:"items"`
	TotalAmount   float64          `json:"total_amount"`
	Currency      string           `json:"currency"`
	Status        string           `json:"status"`
	CreatedAt     string           `json:"created_at"`
}

type Publisher struct{ writer *kafka.Writer }

func NewPublisher(brokerAddr string) *Publisher {
	return &Publisher{writer: &kafka.Writer{
		Addr: kafka.TCP(brokerAddr), Topic: "order.created", Balancer: &kafka.Hash(),
		RequiredAcks: kafka.RequireAll, Async: false,
	}}
}

func (p *Publisher) PublishOrderCreated(ctx context.Context, evt OrderCreatedEvent) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return p.Publish(ctx, evt.OrderID, payload)
}

func (p *Publisher) Publish(ctx context.Context, key string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload, Time: time.Now().UTC()})
	if err != nil {
		log.Printf("failed to publish Kafka event key=%s: %v", key, err)
	}
	return err
}

func (p *Publisher) Close() error { return p.writer.Close() }
