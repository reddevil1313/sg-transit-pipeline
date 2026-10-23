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

- Kafka UI: http://localhost:8090
- Kafka from your host: `localhost:9092`; from other containers: `kafka:19092`

## Status

- [x] Step 1: Kafka (KRaft) + topics + Kafka UI
- [ ] Step 2: DataMall producer
- [ ] Step 3: HDFS + reference data (bus stops, routes)
- [ ] Step 4: Streaming ingest to the bronze layer
- [ ] Step 5: Cleaning and arrival inference (silver)
- [ ] Step 6: Live bunching alerts
- [ ] Step 7: Daily batch analytics (gold)
- [ ] Step 8: Dashboard
- [ ] Step 9: Tests, CI, data-quality checks, write-up
