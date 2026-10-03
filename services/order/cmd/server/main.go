package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orderv1 "gen/order/v1"
	"orders/internal/clients"
	"orders/internal/events"
	"orders/internal/repository"
	"orders/internal/service"
	grpctransport "orders/internal/transport/grpc"
	"orders/migrations"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	dsn := getenv("DB_DSN", "order_svc:orderpass@tcp(localhost:3308)/order_service?parseTime=true")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	if err := migrations.Apply(context.Background(), db); err != nil {
		log.Fatalf("failed to apply database migrations: %v", err)
	}

	userClient, err := clients.NewUserClient(getenv("USER_SERVICE_ADDR", "localhost:50051"))
	if err != nil {
		log.Fatalf("failed to connect to user service: %v", err)
	}

	productClient, err := clients.NewProductClient(getenv("PRODUCT_SERVICE_ADDR", "localhost:50052"))
	if err != nil {
		log.Fatalf("failed to connect to product service: %v", err)
	}

	publisher := events.NewPublisher(getenv("KAFKA_BROKER", "localhost:9094"))
	defer publisher.Close()

	repo := repository.NewOrderRepository(db)
	go publishOutbox(context.Background(), repo, publisher)
	statusReader := events.NewStatusConsumer(getenv("KAFKA_BROKER", "localhost:9094"))
	defer statusReader.Close()
	go consumeOrderStatuses(context.Background(), statusReader, repo)
	svc := service.NewOrderService(repo, userClient, productClient)
	handler := grpctransport.NewHandler(svc)

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	log.Println("order service listening on :50053")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func publishOutbox(ctx context.Context, repo *repository.OrderRepository, publisher *events.Publisher) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		events, err := repo.PendingEvents(ctx, 50)
		if err != nil {
			log.Printf("load order outbox: %v", err)
		} else {
			for _, event := range events {
				if err := publisher.Publish(ctx, event.AggregateID, event.Payload); err != nil {
					_ = repo.RecordEventFailure(ctx, event.ID, err)
					break
				}
				if err := repo.MarkEventPublished(ctx, event.ID); err != nil {
					log.Printf("mark order event %s published: %v", event.ID, err)
					break
				}
			}
		}
		<-ticker.C
	}
}

func consumeOrderStatuses(ctx context.Context, reader *kafka.Reader, repo *repository.OrderRepository) {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("read order status event: %v", err)
			time.Sleep(time.Second)
			continue
		}
		event, err := events.DecodeOrderStatus(message.Value)
		if err != nil {
			log.Printf("invalid order status event at offset %d: %v", message.Offset, err)
			if commitErr := reader.CommitMessages(ctx, message); commitErr != nil {
				log.Printf("commit invalid order event: %v", commitErr)
			}
			continue
		}
		if err := repo.ApplyOrderEvent(ctx, event.EventID, event.OrderID, event.EventType, event.Status); err != nil {
			log.Printf("apply order event %s: %v", event.EventID, err)
			time.Sleep(time.Second)
			continue
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("commit order event %s: %v", event.EventID, err)
		}
	}
}
