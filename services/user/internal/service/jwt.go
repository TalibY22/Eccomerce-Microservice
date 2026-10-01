package service

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TODO: load from environment variable before any real deployment.
var jwtSecret = []byte("dev-secret-change-me")

type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwt.RegisteredClaims
}

func generateToken(userID, email string) (token string, expiresAt int64, err error) {
	exp := time.Now().Add(24 * time.Hour)

	claims := Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := t.SignedString(jwtSecret)
	return signed, exp.Unix(), err
}