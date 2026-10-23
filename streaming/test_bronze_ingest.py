import datetime as dt
import unittest

from pyspark.sql import SparkSession

from bronze_ingest import to_bronze

UTC = dt.timezone.utc


class ToBronzeTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.session.timeZone", "Asia/Singapore")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def kafka_rows(self, *values):
        kafka_ts = dt.datetime(2026, 10, 23, 1, 0, 0, tzinfo=UTC)
        rows = [(b"14031", v.encode(), 2, 100 + i, kafka_ts) for i, v in enumerate(values)]
        return self.spark.createDataFrame(rows, "key binary, value binary, partition int, offset long, timestamp timestamp")

    def test_keeps_raw_value_and_kafka_metadata(self):
        value = '{"BusStopCode":"14031","Services":[],"polled_at":"2026-10-23T06:05:31.493Z"}'
        row = to_bronze(self.kafka_rows(value)).collect()[0]

        self.assertEqual(value, row.value)
        self.assertEqual("14031", row.stop_code)
        self.assertEqual(2, row.kafka_partition)
        self.assertEqual(100, row.kafka_offset)
        self.assertEqual(dt.datetime(2026, 10, 23, 1, 0, 0, tzinfo=UTC), row.kafka_timestamp.astimezone())

    def test_partitions_by_singapore_date_and_hour(self):
        rows = to_bronze(self.kafka_rows(
            '{"polled_at":"2026-10-23T06:05:31.493029882Z"}',
            '{"polled_at":"2026-10-23T16:30:00Z"}',
        )).collect()

        self.assertEqual(dt.datetime(2026, 10, 23, 6, 5, 31, 493029, tzinfo=UTC), rows[0].polled_at.astimezone())
        self.assertEqual((dt.date(2026, 10, 23), 14), (rows[0].date, rows[0].hour))
        self.assertEqual((dt.date(2026, 10, 24), 0), (rows[1].date, rows[1].hour))

    def test_falls_back_to_kafka_timestamp_without_polled_at(self):
        rows = to_bronze(self.kafka_rows("not json", '{"polled_at":"yesterday"}')).collect()

        self.assertEqual("not json", rows[0].value)
        for row in rows:
            self.assertEqual(dt.datetime(2026, 10, 23, 1, 0, 0, tzinfo=UTC), row.polled_at.astimezone())
            self.assertEqual((dt.date(2026, 10, 23), 9), (row.date, row.hour))


if __name__ == "__main__":
    unittest.main()
