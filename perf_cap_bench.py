import concurrent.futures
import math
import subprocess
import sys
import time

import kvstore_pb2
from client import KeyValueClient


def run_cap_benchmark(consistency: int, target_port: int, num_requests: int = 1000):
    all_addrs = ["localhost:50051", "localhost:50052", "localhost:50053"]
    client = KeyValueClient(all_addrs)

    # Write key across cluster so Leaseholder also receives it
    client.put("bench_key", "bench_val", strategy=kvstore_pb2.WriteStrategy.ALWAYS_AP)

    latencies = []
    successful_reads = 0

    for _ in range(num_requests):
        start = time.perf_counter()
        try:
            reply = client.get(
                "bench_key",
                target_addr=f"localhost:{target_port}",
                consistency=consistency,
                max_staleness_ms=5000
            )
            end = time.perf_counter()
            if reply.found and not reply.error_message:
                successful_reads += 1
            latencies.append((end - start) * 1000.0)
        except Exception:
            latencies.append(5000.0)

    latencies.sort()
    n = len(latencies)
    median = latencies[n // 2]
    p95_idx = math.ceil(0.95 * n) - 1
    p95 = latencies[min(p95_idx, n - 1)]
    availability = (successful_reads / num_requests) * 100.0

    return median, p95, availability


def main():
    print("Starting CAP Benchmark Servers...")
    p1 = subprocess.Popen([sys.executable, "server.py", "--port", "50051", "--replica-id", "replica-A"])
    p2 = subprocess.Popen([sys.executable, "server.py", "--port", "50052", "--replica-id", "replica-B"])
    p3 = subprocess.Popen([sys.executable, "server.py", "--port", "50053", "--replica-id", "replica-C"])
    time.sleep(1.5)

    try:
        scenarios = [
            ("Linearizable (Remote Leaseholder)", kvstore_pb2.ReadConsistency.LINEARIZABLE, 50052),
            ("Follower Stale (Local Replica)", kvstore_pb2.ReadConsistency.FOLLOWER_STALE, 50052),
        ]

        print("\n| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |")
        print("|---|---|---|---|---|")

        for title, mode, port in scenarios:
            med, p95, avail = run_cap_benchmark(mode, port, 1000)
            target = "Leaseholder (Remote)" if mode == kvstore_pb2.ReadConsistency.LINEARIZABLE else "Local Follower"
            print(f"| {title:<33} | {target:<20} | {med:<19.2f} | {p95:<16.2f} | {avail:<21.1f} |")

    finally:
        p1.kill()
        p2.kill()
        p3.kill()


if __name__ == "__main__":
    main()
