import datetime as dt
import json
import unittest

from pyspark.sql import SparkSession

from silver import to_arrivals, to_predictions

SGT = dt.timezone(dt.timedelta(hours=8))
T0 = dt.datetime(2026, 10, 23, 16, 0, 8, tzinfo=SGT)


def at(minutes, seconds=0):
    return T0 + dt.timedelta(minutes=minutes, seconds=seconds)


def bus(eta, monitored=1, lat="1.2743", lon="103.8225"):
    if eta is None:
        return {"OriginCode": "", "DestinationCode": "", "EstimatedArrival": "", "Monitored": 0,
                "Latitude": "", "Longitude": "", "VisitNumber": "", "Load": "", "Feature": "", "Type": ""}
    return {"OriginCode": "10009", "DestinationCode": "10009", "EstimatedArrival": eta.isoformat(),
            "Monitored": monitored, "Latitude": lat if monitored else "0.0", "Longitude": lon if monitored else "0.0",
            "VisitNumber": "1", "Load": "SEA", "Feature": "WAB", "Type": "SD"}


def service(service_no, *etas):
    buses = [bus(e) if not isinstance(e, dict) else e for e in etas] + [bus(None)] * (3 - len(etas))
    return {"ServiceNo": service_no, "Operator": "SBST",
            "NextBus": buses[0], "NextBus2": buses[1], "NextBus3": buses[2]}


def poll(polled_at, *etas, stop="14031", services=None):
    value = {"BusStopCode": stop, "Services": services or [service("65", *etas)]}
    return (json.dumps(value), stop, polled_at)


class SilverTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.session.timeZone", "Asia/Singapore")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def bronze(self, *polls):
        return self.spark.createDataFrame(list(polls), "value string, stop_code string, polled_at timestamp")

    def arrivals(self, *polls):
        rows = to_arrivals(to_predictions(self.bronze(*polls))).orderBy("arrival_at").collect()
        return [(r.arrival_at.astimezone(), r.last_seen_at.astimezone(), r.disappeared_at.astimezone()) for r in rows]

    def test_predictions_flatten_slots_and_drop_empty(self):
        rows = to_predictions(self.bronze(
            poll(at(0), at(2, 18), bus(at(12, 13), monitored=0)),
        )).orderBy("slot").collect()

        self.assertEqual([1, 2], [r.slot for r in rows])
        self.assertEqual(("14031", "65"), (rows[0].stop_code, rows[0].service_no))
        self.assertEqual(at(2, 18), rows[0].estimated_arrival.astimezone())
        self.assertEqual(138, rows[0].lead_seconds)
        self.assertEqual((True, 1.2743), (rows[0].monitored, rows[0].latitude))
        self.assertEqual((False, None, None), (rows[1].monitored, rows[1].latitude, rows[1].longitude))
        self.assertEqual(dt.date(2026, 10, 23), rows[0].date)

    def test_predictions_drop_duplicate_polls_and_bad_values(self):
        broken = {**bus(at(5)), "EstimatedArrival": "soon", "Latitude": "n/a"}
        rows = to_predictions(self.bronze(
            poll(at(0), at(2)),
            poll(at(0), at(2)),
            poll(at(1), broken),
            ("not json", "14031", at(2)),
        )).collect()

        self.assertEqual(1, len(rows))

    def test_arrival_when_front_bus_leaves_and_queue_shifts(self):
        self.assertEqual(
            [(at(3, 48), at(4), at(5))],
            self.arrivals(
                poll(at(3), at(2, 47), at(12, 13), at(23, 13)),
                poll(at(4), at(3, 48), at(12, 13), at(23, 13)),
                poll(at(5), at(12, 13), at(23, 13), at(34, 13)),
            ),
        )

    def test_waiting_bus_counts_once(self):
        self.assertEqual(
            [(at(2), at(4), at(5))],
            self.arrivals(
                poll(at(2), at(2), at(12)),
                poll(at(3), at(2), at(12)),
                poll(at(4), at(2), at(12)),
                poll(at(5), at(12), at(23)),
                poll(at(6), at(12, 30), at(23)),
            ),
        )

    def test_bunched_buses_with_revised_etas_are_not_arrivals(self):
        self.assertEqual([], self.arrivals(
            poll(at(2, 42), at(3, 39), at(3, 54), at(25, 12)),
            poll(at(3, 14), at(4, 11), at(4, 16), at(25, 32)),
            poll(at(4, 13), at(4, 21), at(4, 29), at(24, 39)),
            poll(at(5, 13), at(5, 22), at(5, 30), at(24, 2)),
        ))

    def test_bunched_buses_leaving_together_are_two_arrivals(self):
        self.assertEqual(
            [(at(4, 46), at(6), at(7)), (at(5, 31), at(6), at(7))],
            self.arrivals(
                poll(at(5), at(5, 22), at(5, 30), at(24, 2)),
                poll(at(6), at(4, 46), at(5, 31), at(24, 33)),
                poll(at(7), at(24, 10), at(35), at(46)),
            ),
        )

    def test_last_listed_bus_leaving_is_an_arrival(self):
        self.assertEqual(
            [(at(1), at(0), at(1))],
            self.arrivals(
                poll(at(0), at(1), at(14)),
                poll(at(1), at(14)),
            ),
        )

    def test_bus_inserted_ahead_is_not_an_arrival(self):
        self.assertEqual([], self.arrivals(
            poll(at(36), bus(at(57), monitored=0), bus(at(68), monitored=0)),
            poll(at(37), at(49), bus(at(57), monitored=0), bus(at(68), monitored=0)),
        ))

    def test_countdown_without_departure_is_not_an_arrival(self):
        self.assertEqual([], self.arrivals(
            poll(at(0), at(5), at(15)),
            poll(at(1), at(5, 20), at(15)),
            poll(at(2), at(4, 50), at(15)),
        ))

    def test_gap_between_polls_is_skipped(self):
        self.assertEqual([], self.arrivals(
            poll(at(0), at(1), at(12)),
            poll(at(10), at(12), at(23)),
        ))

    def test_services_and_stops_are_tracked_separately(self):
        result = self.arrivals(
            poll(at(0), services=[service("65", at(1), at(12)), service("123", at(30), at(40))]),
            poll(at(0), at(1), at(12), stop="14039"),
            poll(at(1), services=[service("65", at(12), at(23)), service("123", at(30), at(40))]),
            poll(at(1), at(1), at(12), stop="14039"),
        )
        self.assertEqual([(at(1), at(0), at(1))], result)


if __name__ == "__main__":
    unittest.main()
