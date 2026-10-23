import io
import os

import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATAMALL_URL = "https://datamall2.mytransport.sg/ltaodataservice/"
DATASETS = {"BusStops": "bus_stops", "BusRoutes": "bus_routes", "BusServices": "bus_services"}


def fetch_all(session, dataset):
    rows = []
    while True:
        resp = session.get(DATAMALL_URL + dataset, params={"$skip": len(rows)}, timeout=30)
        resp.raise_for_status()
        page = resp.json()["value"]
        if not page:
            return rows
        rows.extend(page)


def to_parquet(rows):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


def upload(webhdfs_url, path, data):
    params = {"op": "CREATE", "overwrite": "true", "user.name": "hadoop"}
    resp = requests.put(webhdfs_url + path, params=params, allow_redirects=False, timeout=30)
    resp.raise_for_status()
    requests.put(resp.headers["Location"], data=data, timeout=60).raise_for_status()


def main():
    webhdfs_url = os.environ["WEBHDFS_URL"]
    session = requests.Session()
    session.headers.update({"AccountKey": os.environ["LTA_ACCOUNT_KEY"], "Accept": "application/json"})

    for dataset, name in DATASETS.items():
        rows = fetch_all(session, dataset)
        if not rows:
            raise RuntimeError(f"{dataset} returned no rows")
        upload(webhdfs_url, f"/lake/reference/{name}/data.parquet", to_parquet(rows))
        print(f"{dataset}: {len(rows)} rows -> /lake/reference/{name}", flush=True)


if __name__ == "__main__":
    main()
