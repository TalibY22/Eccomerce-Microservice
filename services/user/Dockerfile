# ---- Build stage ----
FROM golang:1.25 AS builder

WORKDIR /app

# Copy go.work and all modules it references, so cross-module resolution works during build
COPY go.work go.work.sum* ./
COPY gen/ ./gen/
COPY services/product/ ./services/product/
COPY services/user/ ./services/user/
COPY services/order/ ./services/order/

WORKDIR /app/services/user

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /user-service ./cmd/server

# ---- Runtime stage ----
FROM alpine:3.20

RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=builder --chown=app:app /user-service /app/user-service

USER app

EXPOSE 50051

CMD ["/app/user-service"]