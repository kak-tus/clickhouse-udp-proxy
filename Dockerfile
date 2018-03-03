FROM golang:alpine AS build

COPY proxy.go /go/src/proxy/proxy.go

RUN \
  apk add --no-cache --virtual .build-deps \
    git \
  \
  && cd /go/src/proxy \
  && go get \
  \
  && apk del .build-deps

FROM alpine:3.6

RUN \
  apk add --no-cache \
    su-exec \
    tzdata

COPY --from=build /go/bin/proxy /usr/local/bin/proxy
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENV \
  USER_UID=1000 \
  USER_GID=1000 \
  \
  CLICKHOUSE_ADDR= \
  PROXY_PERIOD=60 \
  PROXY_BATCH=10000

CMD ["/usr/local/bin/entrypoint.sh"]
