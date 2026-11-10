import os
import sys
import time
import subprocess
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import kvstore_pb2
from client import KeyValueClient


def start_server(port, replica_id):
    cmd = [sys.executable, os.path.join(PROJECT_ROOT, "server.py"), "--port", str(port), "--replica-id", replica_id]
    proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT)
    time.sleep(1.2)
    return proc


@pytest.fixture
def cluster():
    p1 = start_server(50051, "replica-A")
    p2 = start_server(50052, "replica-B")
    p3 = start_server(50053, "replica-C")
    yield
    for p in (p1, p2, p3):
        if p.poll() is None:
            p.kill()


def test_cp_majority_write_success(cluster):
    client_dc1 = KeyValueClient(["localhost:50051", "localhost:50052"])
    success, acks, err = client_dc1.put("k1", "v1", strategy=kvstore_pb2.WriteStrategy.QUORUM_CP)
    assert success is True
    assert acks == 2


def test_cp_minority_write_failure(cluster):
    client_dc2 = KeyValueClient(["localhost:50053"])
    success, acks, err = client_dc2.put("k1", "v2", strategy=kvstore_pb2.WriteStrategy.QUORUM_CP)
    assert success is False
    assert "Quorum loss" in err


def test_ap_minority_write_success(cluster):
    client_dc2 = KeyValueClient(["localhost:50053"])
    success, acks, err = client_dc2.put("k1", "ap_val", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP)
    assert success is True
    assert acks == 1


def test_ap_conflict_resolution_lww(cluster):
    client_a = KeyValueClient(["localhost:50051"])
    client_c = KeyValueClient(["localhost:50053"])

    ts_old = 1000
    ts_new = 2000

    client_a.put("item", "old_value", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP, ts=ts_old)
    client_c.put("item", "new_value_lww", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP, ts=ts_new)

    # Reconnect sync
    client_a.put("item", "new_value_lww", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP, ts=ts_new)

    reply = client_a.get("item", target_addr="localhost:50051")
    assert reply.value == "new_value_lww"


def test_follower_stale_read_within_bound(cluster):
    client = KeyValueClient(["localhost:50051", "localhost:50052"])
    now_ms = int(time.time() * 1000)
    client.put("fresh_key", "fresh_val", strategy=kvstore_pb2.WriteStrategy.QUORUM_CP, ts=now_ms)

    reply = client.get(
        "fresh_key",
        target_addr="localhost:50052",
        consistency=kvstore_pb2.ReadConsistency.FOLLOWER_STALE,
        max_staleness_ms=5000
    )
    assert reply.found is True
    assert reply.value == "fresh_val"
    assert reply.is_stale is False


def test_follower_stale_read_exceeds_bound(cluster):
    client_b = KeyValueClient(["localhost:50052"])
    old_ts = int(time.time() * 1000) - 10000

    client_b.put("old_key", "old_val", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP, ts=old_ts)

    reply = client_b.get(
        "old_key",
        target_addr="localhost:50052",
        consistency=kvstore_pb2.ReadConsistency.FOLLOWER_STALE,
        max_staleness_ms=2000
    )
    assert reply.is_stale is True
    assert reply.error_message == "STALE_READ_EXCEEDED"
