## Setup

There are two steps involved.

### Start services

```sh
docker-compose up
```

With all the downloads and build steps this might take some time.

To make sure that all services are up and running check `docker-compose ps` output. All services should have 'Up' state.

Some amount of data is generated during start-up which is visible both in `app-db` and in kafka topics used as input (for example `app-db.public.call`).

There is still no data visible in `staging-db` and `dwh`.

### Start connectors

Since kafka-connect runs in distributed mode (although unsuitable to production) this is done with http request where json payload describes configuration.

For conveniece there are 2 bash scripts:

```sh
./start_source_connector.sh
```

and

```sh
./start_sink_connector.sh
```

This should start working right away making data stream right to DWH.

At this point it's possible to run analytical queries on `dwh`.

```sql
SELECT
    avg(end_time::timestamp - start_time::timestamp)
FROM call_fact cf
INNER JOIN date_dim dd ON dd.date_key = cf.start_date_key
WHERE dd.day_of_week = 'Wednesday'
    AND dd."date" > now() - INTERVAL '2 weeks'
```

results in

```
01:12:37.2
```

or something similar.

### Initial historical load

There is no such thing. Instead `data-generator` starts with generating some amount of history and then just emulates events.

### Notes

* JDBC Sink Connector integrates poorly with JSON Schema, thus `varchar` time type is used instead of `timestamp`.
* JSON Schema Converter marks `object` types with `oneOf` which adds to them `Optional` type on code generation and forces to make useless `None` checks.
* At this point there is no available code generation from Avro schemas.
