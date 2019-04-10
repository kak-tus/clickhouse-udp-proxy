# clickhouse-udp-proxy

Simple UDP proxy for ClickHouse.

It accepts UDP packets, send it to Redis Cluster with [Ruthie](https://github.com/kak-tus/ruthie). Then Ruthie write them to ClickHouse.

Proxy role - run near some writer and accept metrics in UDP packets. That can be added at any code (sync or async) without any code refactoring.

Best idea - write to Ruthie directly, but sometimes (in Perl code) it is not possible.

## Configuration

```
PROXY_REDIS_ADDR=172.17.0.1:7000
PROXY_SHARDS_COUNT=10
PROXY_PENDING_BUFFER_SIZE=1000000
PROXY_PIPE_BUFFER_SIZE=50000
```

## Run

```
docker run --rm -it -p 9001:9001/udp -p 9000:9000/tcp kaktuss/clickhouse-udp-proxy
```

## Packet format

```
{"types":["string","string","int"],"data":["2017-09-09","2017-09-09 12:26:03",3],"query":"INSERT INTO test (dt,dt1,id) VALUES (?,?,?);","version":1}
```

version - protocol version for future changes

query - query to run

data - data to query

types - data in JSON like "1" (integers) golang unpacks as float, and this data
fails to insert in integer columns in ClickHouse, so we need to convert this
values.

If you set int type - value will be converted to int. Any other type will be
ignored (they extracted correctly).

Send example see at dev/test.pl

## Healthcheck

Service listens at 9001 TCP port and response to HTTP queries with code 200 or
code 500.
