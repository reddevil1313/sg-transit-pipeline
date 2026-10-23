import os
import tempfile
import unittest

import pyarrow.parquet as pq

from load_reference import fetch_all, to_parquet


class FakeResponse:
    def __init__(self, rows):
        self.rows = rows

    def raise_for_status(self):
        pass

    def json(self):
        return {"value": self.rows}


class FakeSession:
    def __init__(self, total, page_size=500):
        self.data = [{"BusStopCode": f"{i:05d}"} for i in range(total)]
        self.page_size = page_size
        self.skips = []

    def get(self, url, params, timeout):
        skip = params["$skip"]
        self.skips.append(skip)
        return FakeResponse(self.data[skip:skip + self.page_size])


class LoadReferenceTest(unittest.TestCase):

    def test_fetch_all_pages_until_empty(self):
        session = FakeSession(total=1203)
        rows = fetch_all(session, "BusStops")
        self.assertEqual(1203, len(rows))
        self.assertEqual("01202", rows[-1]["BusStopCode"])
        self.assertEqual([0, 500, 1000, 1203], session.skips)

    def test_to_parquet_round_trips(self):
        rows = [{"BusStopCode": "14031", "Latitude": 1.27}, {"BusStopCode": "14039", "Latitude": 1.28}]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "data.parquet")
            with open(path, "wb") as f:
                f.write(to_parquet(rows))
            self.assertEqual(rows, pq.read_table(path).to_pylist())


if __name__ == "__main__":
    unittest.main()
