FROM golang:1.13.2-alpine3.10 AS build

WORKDIR /go/clickhouse-udp-proxy

COPY *.go ./
COPY go.mod .
COPY go.sum .
COPY listener ./listener

RUN go build -o /go/bin/clickhouse-udp-proxy

FROM alpine:3.10

RUN \
  adduser -DH user

COPY --from=build /go/bin/clickhouse-udp-proxy /usr/local/bin/clickhouse-udp-proxy
COPY etc /etc/

ENV \
  PROXY_REDIS_ADDR= \
  \
  PROXY_SHARDS_COUNT=10 \
  PROXY_PENDING_BUFFER_SIZE=1000000 \
  PROXY_PIPE_BUFFER_SIZE=50000

EXPOSE 9000 9001

USER user

CMD ["/usr/local/bin/clickhouse-udp-proxy"]
