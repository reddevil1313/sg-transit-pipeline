import datetime as dt
import os
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

BUS = (
    "struct<OriginCode:string,DestinationCode:string,EstimatedArrival:string,Monitored:int,"
    "Latitude:string,Longitude:string,VisitNumber:string,Load:string,Feature:string,Type:string>"
)
ARRIVAL = f"struct<Services:array<struct<ServiceNo:string,Operator:string,NextBus:{BUS},NextBus2:{BUS},NextBus3:{BUS}>>>"

MAX_POLL_GAP_SECONDS = 180
DUE_WITHIN_SECONDS = 120
SHIFT_TOLERANCE_SECONDS = 180


def to_predictions(bronze: DataFrame) -> DataFrame:
    # one bronze row per stop per poll and one row per service per poll
    services = (
        bronze.dropDuplicates(["stop_code", "polled_at"])
        .select("stop_code", "polled_at", F.explode(F.from_json("value", ARRIVAL).Services).alias("s"))
    )

    # explode the three bus slots into one row per bus per service per poll
    buses = services.select(
        "stop_code",
        "polled_at",
        F.col("s.ServiceNo").alias("service_no"),
        F.col("s.Operator").alias("operator"),
        F.explode(F.array(*[
            F.struct(F.lit(slot).alias("slot"), F.col(f"s.{field}").alias("bus"))
            for slot, field in [(1, "NextBus"), (2, "NextBus2"), (3, "NextBus3")]
        ])).alias("b"),
    )

    eta = F.try_to_timestamp("b.bus.EstimatedArrival")
    # monitored is a boolean indicating whether the bus is being tracked in real-time (1) or not (0)
    monitored = F.col("b.bus.Monitored") == 1 
    lat = F.col("b.bus.Latitude").try_cast("double")
    lon = F.col("b.bus.Longitude").try_cast("double")
    return buses.where(eta.isNotNull()).select(
        "stop_code",
        "service_no",
        "operator",
        "polled_at",
        F.col("b.slot").alias("slot"),
        eta.alias("estimated_arrival"),
        (F.unix_timestamp(eta) - F.unix_timestamp("polled_at")).alias("lead_seconds"),
        monitored.alias("monitored"),
        F.when(monitored, lat).alias("latitude"),
        F.when(monitored, lon).alias("longitude"),
        F.col("b.bus.VisitNumber").alias("visit_number"),
        F.col("b.bus.Load").alias("load"),
        F.col("b.bus.Type").alias("bus_type"),
        F.col("b.bus.Feature").alias("feature"),
        F.col("b.bus.OriginCode").alias("origin_code"),
        F.col("b.bus.DestinationCode").alias("destination_code"),
        F.to_date("polled_at").alias("date"),
    )


def _seconds(a, b):
    return F.unix_timestamp(a) - F.unix_timestamp(b)


def _mismatch(eta, prev_eta):
    return (
        F.when(eta.isNull() & prev_eta.isNull(), 0)
        .when(eta.isNull() | prev_eta.isNull(), SHIFT_TOLERANCE_SECONDS)
        .otherwise(F.abs(_seconds(eta, prev_eta)))
    )


def to_arrivals(predictions: DataFrame) -> DataFrame:
    polls = predictions.groupBy("stop_code", "service_no", "polled_at").agg(*[
        F.max(F.when(F.col("slot") == slot, F.struct(
            F.col("estimated_arrival").alias("eta"), "monitored", "latitude", "longitude"
        ))).alias(f"bus{slot}")
        for slot in (1, 2, 3)
    ])

    w = Window.partitionBy("stop_code", "service_no").orderBy("polled_at")
    polls = polls.select(
        "*",
        F.lag("polled_at").over(w).alias("prev_polled_at"),
        *[F.lag(f"bus{slot}").over(w).alias(f"prev{slot}") for slot in (1, 2, 3)],
    )

    e1, e2 = F.col("bus1.eta"), F.col("bus2.eta")
    p1, p2, p3 = F.col("prev1.eta"), F.col("prev2.eta"), F.col("prev3.eta")
    no_bus = F.lit(None).cast("timestamp")
    stayed = _mismatch(e1, p1) + _mismatch(e2, p2)
    moved_one = _mismatch(e1, p2) + _mismatch(e2, p3)
    moved_two = _mismatch(e1, p3) + _mismatch(e2, no_bus)
    shift = (
        F.when((moved_one < stayed) & (moved_one <= moved_two) & (_mismatch(e1, p2) <= SHIFT_TOLERANCE_SECONDS), 1)
        .when((moved_two < stayed) & (moved_two < moved_one) & (_mismatch(e1, p3) <= SHIFT_TOLERANCE_SECONDS), 2)
        .otherwise(0)
    )

    departed = polls.where(
        _seconds(F.col("polled_at"), F.col("prev_polled_at")) <= MAX_POLL_GAP_SECONDS
    ).select(
        "stop_code",
        "service_no",
        "polled_at",
        "prev_polled_at",
        F.explode(F.array(
            F.struct(F.lit(1).alias("n"), F.col("prev1").alias("bus")),
            F.struct(F.lit(2).alias("n"), F.col("prev2").alias("bus")),
        )).alias("d"),
        shift.alias("shift"),
    ).where(
        (F.col("d.n") <= F.col("shift")) & (_seconds(F.col("d.bus.eta"), F.col("polled_at")) <= DUE_WITHIN_SECONDS)
    )

    return departed.select(
        "stop_code",
        "service_no",
        F.least(F.col("d.bus.eta"), F.col("polled_at")).alias("arrival_at"),
        F.col("d.bus.eta").alias("last_eta"),
        F.col("prev_polled_at").alias("last_seen_at"),
        F.col("polled_at").alias("disappeared_at"),
        F.col("d.bus.monitored").alias("monitored"),
        F.col("d.bus.latitude").alias("latitude"),
        F.col("d.bus.longitude").alias("longitude"),
    ).withColumn("date", F.to_date("arrival_at"))


def main():
    lake = f"{os.environ['HDFS_URL']}/lake"
    spark = (
        SparkSession.builder.appName("silver")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    today = dt.datetime.now(ZoneInfo("Asia/Singapore")).date()
    dates = [today - dt.timedelta(days=1), today]
    bronze = spark.read.parquet(f"{lake}/bronze/arrivals").where(
        F.col("date").between(dates[0] - dt.timedelta(days=1), today)
    )

    predictions = to_predictions(bronze).cache()
    for name, df in [("predictions", predictions), ("arrivals", to_arrivals(predictions))]:
        (
            df.where(F.col("date").isin(dates))
            .repartition("date")
            .write.mode("overwrite")
            .partitionBy("date")
            .parquet(f"{lake}/silver/{name}")
        )
    print(f"silver rebuilt for {dates[0]} and {dates[1]}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
