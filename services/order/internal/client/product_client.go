package clients

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	productv1 "gen/product/v1"
)

func NewProductClient(addr string) (productv1.ProductServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return productv1.NewProductServiceClient(conn), nil
}