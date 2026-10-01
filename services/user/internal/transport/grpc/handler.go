package grpc



import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "gen/user/v1"
	"user_service/internal/repository"
	"user_service/internal/service"
)


type Handler struct {
		userv1.UnimplementedUserServiceServer
		svc *service.UserService

}


func NewHandler(svc  * service.UserService) *Handler {
	return &Handler{svc:svc}
}


func (h *Handler) CreateUser(ctx context.Context, req *userv1.CreateUserRequest) (*userv1.CreateUserResponse, error) {
	u, err := h.svc.CreateUser(ctx, req.GetEmail(), req.GetPassword(), req.GetFirstName(), req.GetLastName())
	if err != nil {
		if errors.Is(err, repository.ErrEmailExists) {
			return nil, status.Error(codes.AlreadyExists, "email already registered")
		}
		return nil, status.Error(codes.Internal, "failed to create user")
	}
	return &userv1.CreateUserResponse{Id: u.ID}, nil
}

func (h *Handler) GetUser(ctx context.Context, req *userv1.GetUserRequest) (*userv1.GetUserResponse, error) {
	u, err := h.svc.GetUser(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Error(codes.Internal, "failed to get user")
	}

	return &userv1.GetUserResponse{
		Id:        u.ID,
		Email:     u.Email,
		FirstName: u.FirstName,
		LastName:  u.LastName,
	}, nil
}

func (h *Handler) Login(ctx context.Context, req *userv1.LoginRequest) (*userv1.LoginResponse, error) {
	token, expiresAt, err := h.svc.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		if errors.Is(err, service.ErrInvalidCredentials) {
			return nil, status.Error(codes.Unauthenticated, "invalid email or password")
		}
		return nil, status.Error(codes.Internal, "login failed")
	}

	return &userv1.LoginResponse{
		AccessToken: token,
		ExpiresAt:   expiresAt,
	}, nil
}