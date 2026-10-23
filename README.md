# sg-transit-pipeline

A streaming and batch data pipeline built on live Singapore bus data from LTA DataMall.
It tracks real-time bus arrivals, detects bus bunching as it happens, and measures how
accurate the published ETAs turn out to be.

## Architecture

```
LTA DataMall API
      │  (poll every 60s, a set of bus stops)
      ▼
  Producer ──► Kafka: bus.arrivals.raw  (keyed by stop code)
                     │
                     ▼
        Spark Structured Streaming
          ├─► HDFS  /lake/bronze   raw readings, Parquet, partitioned by date/hour
          ├─► HDFS  /lake/silver   cleaned, deduplicated, inferred actual arrivals
          └─► Kafka: bus.alerts    live bunching alerts
                     │
        Spark batch jobs (daily, on HDFS)
          └─► /lake/gold           ETA accuracy, headway reliability by service / hour
                     │
                     ▼
              Serving DB + dashboard
```

## Stack

Kafka 4 (KRaft) · Spark 4 (Structured Streaming + batch) · Hadoop HDFS · Parquet · Docker Compose · GitHub Actions

## Running locally

```bash
cp .env.example .env        # add your LTA DataMall key
docker compose up -d
```

- HDFS NameNode UI: http://localhost:9870
- Kafka UI (optional): `docker compose --profile ui up -d`, then http://localhost:8090
- Kafka from your host: `localhost:9092`; from other containers: `kafka:19092`

## Monitored stops

The producer polls the stops in `BUS_STOPS`. The default is a stretch of services 65 and 123 around
stops 14031 and 14039 (both directions):

```
10379, 10369, 14039, 14029, 14379, 14031, 10361, 10371
```

To change it, set `BUS_STOPS` in `.env` (comma-separated 5-digit codes) and run `docker compose up -d producer`.

## Reference data

On every `docker compose up`, `reference-loader` downloads DataMall's `BusStops`, `BusRoutes` and
`BusServices` and writes them to HDFS as Parquet:

```
/lake/reference/bus_stops/data.parquet
/lake/reference/bus_routes/data.parquet
/lake/reference/bus_services/data.parquet
```

To refresh without restarting the stack: `docker compose run --rm reference-loader`.

## Tests

```bash
cd producer && ./gradlew test
docker compose run --rm --no-deps --entrypoint python reference-loader -m unittest
```

## Status

- [x] Step 1: Kafka (KRaft) + topics + Kafka UI
- [x] Step 2: DataMall producer
- [x] Step 3: HDFS + reference data (bus stops, routes)
- [ ] Step 4: Streaming ingest to the bronze layer
- [ ] Step 5: Cleaning and arrival inference (silver)
- [ ] Step 6: Live bunching alerts
- [ ] Step 7: Daily batch analytics (gold)
- [ ] Step 8: Dashboard
- [ ] Step 9: Tests, CI, data-quality checks, write-up
