package main

import (
	"database/sql"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orderv1 "gen/order/v1"
	"orders/internal/clients"
	"orders/internal/repository"
	"orders/internal/service"
	grpctransport "orders/internal/transport/grpc"
)

func main() {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "order_svc:orderpass@tcp(localhost:3308)/order_service?parseTime=true"
	}

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

	userAddr := os.Getenv("USER_SERVICE_ADDR")
	if userAddr == "" {
		userAddr = "localhost:50051"
	}
	userClient, err := clients.NewUserClient(userAddr)
	if err != nil {
		log.Fatalf("failed to connect to user service: %v", err)
	}

	productAddr := os.Getenv("PRODUCT_SERVICE_ADDR")
	if productAddr == "" {
		productAddr = "localhost:50052"
	}
	productClient, err := clients.NewProductClient(productAddr)
	if err != nil {
		log.Fatalf("failed to connect to product service: %v", err)
	}

	repo := repository.NewOrderRepository(db)
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