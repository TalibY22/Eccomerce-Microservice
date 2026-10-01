FROM golang:1.25 AS builder

WORKDIR /app

COPY go.work go.work.sum* ./
COPY gen/ ./gen/
COPY services/product/ ./services/product/
COPY services/user/ ./services/user/
COPY services/order/ ./services/order/

WORKDIR /app/services/product

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /product-service ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /root/

COPY --from=builder /product-service .

EXPOSE 50052

CMD ["./product-service"]