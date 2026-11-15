"""Seed a local Nessie catalog so iceberg-exporter has something to scrape.

The process writes real Iceberg tables through the Nessie Iceberg REST
endpoint, commits one view through the native API, then appends to main on
a timer. A second branch keeps a different snapshot.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo, In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from schedule import run_for

NESSIE = os.environ.get("NESSIE_BASE_URL", "http://nessie:19120").rstrip("/")
ICEBERG = os.environ.get("NESSIE_ICEBERG_URI", NESSIE + "/iceberg").rstrip("/")
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "20"))
DURATION = int(os.environ.get("DURATION_SECONDS", "300"))
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")

EVENTS = "analytics.events"
ORDERS = "analytics.orders"
EMPTY = "ops.empty"
VIEW_KEY = ["analytics", "orders_view"]


def main() -> None:
    wait_ready()
    catalog = catalog_for(ICEBERG)
    seed(catalog)
    diverge()
    print(f"generator loop every {INTERVAL}s for {DURATION}s", flush=True)

    def append_once():
        table = catalog.load_table(EVENTS)
        append(table, event_rows(day=datetime.now(timezone.utc).replace(tzinfo=None), rows=8, payload_bytes=128))
        print("appended events on main", flush=True)

    run_for(DURATION, INTERVAL, time.monotonic, time.sleep, append_once)
    print("generator finished", flush=True)


def catalog_for(uri: str):
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": uri,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def seed(catalog) -> None:
    ensure_namespace(catalog, "analytics")
    ensure_namespace(catalog, "ops")
    events, created = ensure_table(
        catalog,
        EVENTS,
        event_schema(),
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="event_day")
        ),
    )
    if created or not has_snapshot(events):
        write_event_files(events)
    orders, created = ensure_table(catalog, ORDERS, order_schema(), partition_spec=PartitionSpec())
    if created or not has_snapshot(orders):
        write_orders(catalog)
    ensure_table(catalog, EMPTY, order_schema(), partition_spec=PartitionSpec())
    ensure_view()


def diverge() -> None:
    head = reference("main")
    try:
        nessie(
            "POST",
            "/api/v2/trees?" + urllib.parse.urlencode({"name": "dev", "type": "BRANCH"}),
            {"type": "BRANCH", "name": "main", "hash": head["hash"]},
        )
        print("created branch dev", flush=True)
    except urllib.error.HTTPError as err:
        if err.code != 409:
            raise
        print("branch dev already exists", flush=True)
    dev = catalog_for(ICEBERG + "/dev")
    table = dev.load_table(EVENTS)
    append(table, event_rows(day=datetime(2026, 1, 1), rows=1, payload_bytes=64))
    print("appended events on dev", flush=True)


def write_event_files(table) -> None:
    # Three live files land in different histogram buckets. Nine small days
    # against one heavy day move partition skew off zero.
    base = datetime(2026, 1, 1)
    # 36 MiB of random bytes stays above the exporter's 32 MiB small-file threshold after base64.
    append(table, event_rows(day=base, rows=1, payload_bytes=36_000_000))
    append(table, event_rows(day=base + timedelta(days=1), rows=1, payload_bytes=8_000_000))
    for offset in range(2, 11):
        append(table, event_rows(day=base + timedelta(days=offset), rows=3, payload_bytes=64))
    print("seeded analytics.events", flush=True)


def write_orders(catalog) -> None:
    table = catalog.load_table(ORDERS)
    append(table, [{"id": i, "status": "open"} for i in range(1, 41)])
    table = catalog.load_table(ORDERS)
    deleted = False
    try:
        table.delete(In("id", list(range(1, 9))))
        deleted = True
    except Exception as err:
        print(f"batch delete failed ({err}); deleting one row at a time", flush=True)
    if not deleted:
        for row_id in range(1, 9):
            table.delete(EqualTo("id", row_id))
    print("seeded analytics.orders with position deletes", flush=True)


def ensure_view() -> None:
    if view_exists():
        print("view analytics.orders_view already exists", flush=True)
        return
    try:
        nessie(
            "POST",
            ICEBERG + "/v1/main/namespaces/analytics/views",
            {
                "name": "orders_view",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}],
                },
                "view-version": {
                    "version-id": 1,
                    "schema-id": 0,
                    "timestamp-ms": int(time.time() * 1000),
                    "summary": {"engine-name": "generator"},
                    "representations": [
                        {
                            "type": "sql",
                            "sql": "SELECT * FROM analytics.orders",
                            "dialect": "spark",
                        }
                    ],
                    "default-namespace": ["analytics"],
                },
                "properties": {},
            },
        )
    except urllib.error.HTTPError as err:
        if err.code == 409:
            print("view analytics.orders_view already exists", flush=True)
            return
        raise
    print("created view analytics.orders_view", flush=True)


def view_exists() -> bool:
    query = urllib.parse.urlencode(
        {
            "content": "true",
            "filter": "entry.contentType=='ICEBERG_VIEW'",
        }
    )
    page = nessie("GET", "/api/v2/trees/main/entries?" + query)
    for entry in page.get("entries", []):
        name = entry.get("name", {}).get("elements", [])
        if name == VIEW_KEY:
            return True
    return False


def event_schema() -> Schema:
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "event_time", TimestampType(), required=True),
        NestedField(3, "payload", StringType(), required=False),
    )


def order_schema() -> Schema:
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "status", StringType(), required=False),
    )


def event_rows(day: datetime, rows: int, payload_bytes: int) -> list[dict]:
    stamp = day.replace(microsecond=0, tzinfo=None)
    body = base64.b64encode(os.urandom(payload_bytes)).decode("ascii")
    return [{"id": index, "event_time": stamp, "payload": body} for index in range(rows)]


def append(table, rows: list[dict]) -> None:
    arrow = table.schema().as_arrow()
    table.append(pa.Table.from_pylist(rows, schema=arrow))


def has_snapshot(table) -> bool:
    current = table.current_snapshot()
    return current is not None


def ensure_namespace(catalog, name: str) -> None:
    try:
        catalog.create_namespace(name)
    except NamespaceAlreadyExistsError:
        return


def ensure_table(catalog, identifier: str, schema: Schema, partition_spec: PartitionSpec):
    try:
        return catalog.load_table(identifier), False
    except NoSuchTableError:
        table = catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties={"format-version": "2"},
        )
        return table, True


def reference(name: str) -> dict:
    body = nessie("GET", "/api/v2/trees/" + urllib.parse.quote(name))
    if "reference" in body:
        return body["reference"]
    return body


def wait_ready() -> None:
    deadline = time.time() + 180
    last = "nessie is not ready"
    while time.time() < deadline:
        try:
            nessie("GET", "/api/v2/config")
            print("nessie is ready", flush=True)
            return
        except Exception as err:
            last = str(err)
            time.sleep(2)
    raise SystemExit(last)


def nessie(method: str, path: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    url = path if path.startswith("http://") or path.startswith("https://") else NESSIE + path
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as err:
        detail = err.read().decode("utf-8", errors="replace")[:500]
        raise urllib.error.HTTPError(err.url, err.code, f"{err.reason}: {detail}", err.headers, None) from None
    if not raw:
        return {}
    return json.loads(raw)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"generator failed: {err}", file=sys.stderr, flush=True)
        raise
