FROM golang:1.10.3-alpine3.8 AS build

WORKDIR /go/src/clickhouse-udp-proxy

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
  PROXY_RABBITMQ_USER=\
  PROXY_RABBITMQ_PASSWORD=\
  PROXY_RABBITMQ_ADDR=\
  PROXY_RABBITMQ_VHOST=

EXPOSE 9000 9001

USER user

CMD ["/usr/local/bin/clickhouse-udp-proxy"]
