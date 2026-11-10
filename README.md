# Distributed Key-Value Store: CAP & PACELC Lab

## Overview
This laboratory assignment explores the CAP Theorem and PACELC trade-offs by building a replicated Key-Value store with configurable consistency levels (`LINEARIZABLE` vs `FOLLOWER_STALE`) and write strategies (`QUORUM_CP` vs `ALWAYS_AP`).

## Project Structure
```
dcc-lab-cap/
├── kvstore.proto         # Protocol Buffers definition
├── kvstore_pb2.py        # Generated Protobuf serialization classes
├── kvstore_pb2_grpc.py   # Generated gRPC stubs & servicers
├── server.py             # KV-store replica implementation (Leaseholder & Stale Read)
├── client.py             # Client with Quorum & AP write logic
├── perf_cap_bench.py     # PACELC latency & availability benchmark
├── README.md             # Project documentation
└── tests/
    └── test_cap.py       # Automated test cases (CP/AP modes & network partitions)
```

## Setup & Running

1. **Activate virtual environment:**
   ```cmd
   .venv\Scripts\activate.bat
   ```

2. **Generate Protobuf Stubs (if compiling from .proto directly):**
   ```cmd
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
   ```

3. **Run Automated Test Suite (6 test cases):**
   ```cmd
   pytest -v tests/test_cap.py
   ```

4. **Run Performance Evaluation (PACELC Benchmark):**
   ```cmd
   python perf_cap_bench.py
   ```
