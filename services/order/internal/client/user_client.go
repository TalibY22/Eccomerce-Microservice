package client

import (
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	userv1 "gen/user/v1"
)


func NewuserClient(addr string) (userv1.UserServiceClient,error){
	conn ,err := grpc.NewClient(addr,grpc.WithTransportCredentials(insecure.NewCredentials))
    if err!= nil{
		return nil,err
	}

	return userv1.NewUserClient(conn),nil

}

	
