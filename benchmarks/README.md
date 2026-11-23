# `brpc-fast` Core Performance Benchmark

Lightweight regression benchmark covering `brpc-fast`'s core codec and end-to-end loopback RPC workloads:

- **Codec**: `Ping(small)`, `BatchGet(<batch_size>)`, and `BatchPutStart(<batch_size>)` (both `fresh` and `reuse` decode modes).
- **End-to-End RPC**: concurrent `BatchGet(<batch_size>)` and mixed `80% small + 20% BatchGet(<batch_size>)` (reporting overall, small-request, and large-batch latencies separately).

## Build & Run

```bash
cmake -B build -G Ninja -DBRPC_FAST_BUILD_BENCHMARK=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --target brpc_fast_codec_benchmark -j$(nproc)

# Default run (~4s): codec + E2E RPC with batch_size=10000
./build/brpc_fast_codec_benchmark

# Test other batch sizes or export CSV for before/after comparison
./build/brpc_fast_codec_benchmark \
  --batch_size=30000 --csv_path=results.csv --label=after
```

## Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--mode` | `all` | `all`, `codec`, or `rpc` |
| `--batch_size` | `10000` | Batch size for large batch scenarios |
| `--key_len` | `128` | Object key length in bytes |
| `--replicas_per_key` | `2` | Replica descriptors per key |
| `--concurrency` | `16` | Concurrent client bthreads for RPC benchmarks |
| `--warmup_ms` | `200` | Warmup duration before RPC measurement |
| `--duration_ms` | `1500` | Measurement duration per RPC scenario |
| `--csv_path` | `""` | Optional CSV output path |
| `--label` | `run` | Label column written to CSV output |
