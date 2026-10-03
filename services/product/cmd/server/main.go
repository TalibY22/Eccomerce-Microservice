package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	productv1 "gen/product/v1"
	"products/internal/repository"
	"products/internal/service"
	grpctransport "products/internal/transport/grpc"
	"products/migrations"
)

func main() {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "product_svc:productpass@tcp(localhost:3307)/product_service?parseTime=true"
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
	if through := os.Getenv("MIGRATIONS_THROUGH"); through != "" {
		if err := migrations.ApplyThrough(context.Background(), db, through); err != nil {
			log.Fatalf("failed to apply database migrations through %s: %v", through, err)
		}
		return
	}
	if err := migrations.Apply(context.Background(), db); err != nil {
		log.Fatalf("failed to apply database migrations: %v", err)
	}

	repo := repository.NewProductRepository(db)
	svc := service.NewProductService(repo)
	handler := grpctransport.NewHandler(svc)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	productv1.RegisterProductServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	log.Println("product service listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
