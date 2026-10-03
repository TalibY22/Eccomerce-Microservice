FROM golang:1.25 AS builder
WORKDIR /app
COPY go.work go.work.sum* ./
COPY gen/ ./gen/
COPY services/order/ ./services/order/
COPY services/product/ ./services/product/
COPY services/user/ ./services/user/

WORKDIR /app/services/order
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=builder --chown=app:app /order-service /app/order-service
USER app
EXPOSE 50053
CMD ["/app/order-service"]