FROM golang:1.10.3-alpine3.8 AS build

WORKDIR /go/src/github.com/kak-tus/clickhouse-udp-proxy

COPY listener ./listener
COPY vendor ./vendor
COPY main.go main.go

RUN go install

FROM alpine:3.8

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
