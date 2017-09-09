# clickhouse-udp-proxy

Simple UDP proxy for ClickHouse.

It accepts UDP packets, aggregates it and resend to ClickHouse by TCP.

Proxy role - run near some writer and resend data to remote CickHouse.

## Run

```
docker run --rm -it -e CLICKHOUSE_ADDR=127.0.0.1:9000 -p 9001:9001/udp kak-tus/clickhouse-udp-proxy
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
