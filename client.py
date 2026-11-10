import time
import uuid
import grpc

import kvstore_pb2
import kvstore_pb2_grpc


class KeyValueClient:
    def __init__(self, addresses: list[str], timeout: float = 2.0, cluster_size: int = 3):
        self.addresses = addresses
        self.timeout = timeout
        self.cluster_size = max(len(addresses), cluster_size)

    def put(self, key: str, value: str, strategy: int = kvstore_pb2.WriteStrategy.QUORUM_CP, key_id: str = None, ts: int = None):
        idempotency_key = key_id if key_id else str(uuid.uuid4())
        timestamp = ts if ts is not None else int(time.time() * 1000)

        successful_acks = 0
        errors = []

        for addr in self.addresses:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                    req = kvstore_pb2.PutRequest(
                        key=key,
                        value=value,
                        idempotency_key=idempotency_key,
                        strategy=strategy,
                        timestamp=timestamp
                    )
                    reply = stub.Put(req, timeout=self.timeout)
                    if reply.success:
                        successful_acks += 1
            except Exception as e:
                errors.append(str(e))

        majority = (self.cluster_size // 2) + 1

        if strategy == kvstore_pb2.WriteStrategy.QUORUM_CP:
            if successful_acks >= majority:
                return True, successful_acks, ""
            else:
                return False, successful_acks, f"Quorum loss: got {successful_acks}/{self.cluster_size} ACKs"

        elif strategy == kvstore_pb2.WriteStrategy.ALWAYS_AP:
            if successful_acks > 0:
                return True, successful_acks, ""
            else:
                return False, 0, "All nodes unavailable"

    def get(self, key: str, target_addr: str = None, consistency: int = kvstore_pb2.ReadConsistency.LINEARIZABLE, max_staleness_ms: int = 0):
        addr = target_addr if target_addr else self.addresses[0]
        with grpc.insecure_channel(addr) as channel:
            stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
            req = kvstore_pb2.GetRequest(
                key=key,
                consistency=consistency,
                max_staleness_ms=max_staleness_ms
            )
            return stub.Get(req, timeout=self.timeout)
