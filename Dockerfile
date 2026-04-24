FROM golang:1.26.1-alpine3.22 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.Version=${VERSION}" \
    -o /out/moe-asset-server \
    -trimpath \
    -tags netgo \
    ./cmd/server

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/moe-asset-server /app/moe-asset-server
COPY config.example.yaml /app/config.example.yaml
RUN mkdir -p /app/data/staging
ENV TZ=Asia/Shanghai \
    MOE_ASSET_SERVER_CONFIG=/app/config.yaml
EXPOSE 8080
CMD ["/app/moe-asset-server", "-config", "/app/config.yaml"]
