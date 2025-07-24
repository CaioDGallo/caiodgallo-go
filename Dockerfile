FROM golang:1.24.4-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata gcc musl-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
  -ldflags="-s -w" \
  -trimpath \
  -o app \
  ./cmd/api/main.go

RUN apk add --no-cache upx && \
  upx --best --lzma app

FROM alpine:3.19

RUN apk add --no-cache sqlite-libs ca-certificates tzdata

COPY --from=builder /etc/passwd /etc/passwd

COPY --from=builder /build/app /app

RUN addgroup -S appuser && adduser -S appuser -G appuser; \
  mkdir -p /data && chown appuser:appuser /data

USER appuser

EXPOSE 8080

ENV GOGC=100 \
  GOMEMLIMIT=90MiB \
  GOMAXPROCS=1

ENTRYPOINT ["/app"]
