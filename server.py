import argparse
import concurrent.futures
import logging
import threading
import time
import sys
import grpc

import kvstore_pb2
import kvstore_pb2_grpc


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, replica_id: str = "replica-A", port: int = 50051, leaseholder_port: int = 50051):
        self.replica_id = replica_id
        self.port = port
        self.leaseholder_port = leaseholder_port
        self.is_leaseholder = (port == leaseholder_port)

        self._lock = threading.Lock()
        self._store = {}
        self._seen_keys = set()

    def Get(self, request, context):
        current_time_ms = int(time.time() * 1000)

        # 1. LINEARIZABLE Read Path: Redirect to Leaseholder if necessary
        if request.consistency == kvstore_pb2.ReadConsistency.LINEARIZABLE:
            if not self.is_leaseholder:
                try:
                    target_addr = f"localhost:{self.leaseholder_port}"
                    with grpc.insecure_channel(target_addr) as channel:
                        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                        return stub.Get(request, timeout=2.0)
                except Exception as e:
                    return kvstore_pb2.GetReply(
                        found=False,
                        error_message=f"Leaseholder unreachable: {str(e)}"
                    )

        # 2. FOLLOWER_STALE Read Path: Read from local state machine
        with self._lock:
            if request.key not in self._store:
                return kvstore_pb2.GetReply(found=False)

            data = self._store[request.key]
            val = data["value"]
            key_ts = data["timestamp"]

        # Bounded Staleness Check
        is_stale = False
        staleness_ms = current_time_ms - key_ts

        if request.consistency == kvstore_pb2.ReadConsistency.FOLLOWER_STALE:
            if request.max_staleness_ms > 0 and staleness_ms > request.max_staleness_ms:
                return kvstore_pb2.GetReply(
                    value=val,
                    found=True,
                    read_timestamp=key_ts,
                    is_stale=True,
                    error_message="STALE_READ_EXCEEDED"
                )

        return kvstore_pb2.GetReply(
            value=val,
            found=True,
            read_timestamp=key_ts,
            is_stale=is_stale
        )

    def Put(self, request, context):
        with self._lock:
            if request.idempotency_key in self._seen_keys:
                return kvstore_pb2.PutReply(success=True, acks_received=1, error_message="Duplicate request")

            current_entry = self._store.get(request.key)
            if current_entry is None or request.timestamp >= current_entry["timestamp"]:
                self._store[request.key] = {
                    "value": request.value,
                    "timestamp": request.timestamp
                }

            self._seen_keys.add(request.idempotency_key)

        return kvstore_pb2.PutReply(success=True, acks_received=1)


def serve(port: int, replica_id: str):
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
    servicer = KeyValueStoreServicer(replica_id=replica_id, port=port)
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info(f"Server {replica_id} listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAP KV-Store Server")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica-id", type=str, default=None)
    args = parser.parse_args()

    rid = args.replica_id or f"replica-{args.port}"
    serve(args.port, rid)
