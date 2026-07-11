"""Chunk 11 test isolation: captures the "transactions" hub's current
per-partition sequence numbers so a load test can measure only events at
or after that marker -- immune to any pre-existing backlog, regardless of
its source (old validation runs, a prior test's own leftovers, etc.).
Replaces relying on 1-day retention to naturally age old events out
(decided in the Chunk 10 addendum session).

Usage:
    python eventhub_marker.py capture > marker.json
    python eventhub_marker.py count marker.json   # events at/after marker, per partition
"""
import json
import sys

from azure.eventhub import EventHubConsumerClient
from azure.identity import AzureCliCredential

NAMESPACE = "evhns-argus-dev-to614f.servicebus.windows.net"
EVENTHUB = "transactions"


def _client():
    return EventHubConsumerClient(
        fully_qualified_namespace=NAMESPACE,
        eventhub_name=EVENTHUB,
        consumer_group="$Default",
        credential=AzureCliCredential(),
    )


def capture():
    with _client() as client:
        props = client.get_eventhub_properties()
        marker = {}
        for pid in props["partition_ids"]:
            p = client.get_partition_properties(pid)
            marker[pid] = {
                "last_enqueued_sequence_number": p["last_enqueued_sequence_number"],
                "last_enqueued_time_utc": p["last_enqueued_time_utc"].isoformat()
                if p["last_enqueued_time_utc"]
                else None,
            }
        print(json.dumps(marker, indent=2))


def count(marker_path):
    marker = json.load(open(marker_path, encoding="utf-8"))
    with _client() as client:
        total = 0
        per_partition = {}
        for pid, m in marker.items():
            p = client.get_partition_properties(pid)
            baseline = m["last_enqueued_sequence_number"]
            current = p["last_enqueued_sequence_number"]
            # Sequence numbers strictly increase per partition; the gap is
            # exactly the count of events enqueued after the marker.
            new_events = max(current - baseline, 0) if not p["is_empty"] else 0
            per_partition[pid] = new_events
            total += new_events
        print(json.dumps({"per_partition": per_partition, "total": total}, indent=2))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] == "capture":
        capture()
    elif sys.argv[1] == "count":
        count(sys.argv[2])
    else:
        print(__doc__)
        sys.exit(1)
