# Performance measurements

These measurements used an Apple M1 computer with macOS 26.5.
The server and the load generator ran on the same computer through a loopback address.
Each HTTP object contained 5,760 bytes.
The HTTP tests used `wrk` with four threads and 64 connections.
The direct API tests used a release build and counted complete `get` calls.

| Test | Duration | Rate | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Original HTTP GET | 10 s | 55,847/s | 442 µs | 121 ms |
| Current HTTP GET | 10 s | 164,057/s | 258 µs | 1.35 ms |
| Current HTTP GET | 30 s | 151,478/s | 268 µs | 3.62 ms |
| HTTP overwrite before checksum change | 10 s | 45,094/s | 1.38 ms | 3.59 ms |
| Current HTTP overwrite | 10 s | 98,802/s | 554 µs | 6.10 ms |

The current GET test returned 1,641,436 responses in 10 seconds.
The 30-second GET test returned 4,551,282 responses.
The current overwrite test returned 988,753 responses in 10 seconds.
Short runs on this computer had different rates and tail latencies.
These figures are diagnostic measurements, not service-level guarantees.

The direct API test used two million reads per result after a 100,000-read warmup.
A shared-key test made all workers read one object handle.
An independent-key test gave each worker its own object handle.

| Direct API test | Workers | Rate |
| --- | ---: | ---: |
| Handle, shared key | 1 | 51.9 million/s |
| Handle, shared key | 8 | 7.9 million/s |
| Handle, independent keys | 8 | 43.3 million/s |
| Key lookup, shared key | 8 | 3.2 million/s |

The handle avoids a bucket-map lookup.
Shared-key reads still contend on the object's reference count.
HTTP measurements include request parsing, response headers, and transfer of the payload.
Direct API measurements do not include these operations.

## Reproduce the tests

Build the server and run it in one terminal:

```sh
cargo build --release --bin s3-memory
target/release/s3-memory --listen 127.0.0.1:19000
```

In a second terminal, create the bucket and object:

```sh
python3 -c 'import sys; sys.stdout.buffer.write(bytes(5760))' > /tmp/s3-memory-object.bin
curl -X PUT http://127.0.0.1:19000/bench
curl -X PUT --data-binary @/tmp/s3-memory-object.bin http://127.0.0.1:19000/bench/hot
```

Run the HTTP and direct API tests:

```sh
wrk -t4 -c64 -d10s --latency http://127.0.0.1:19000/bench/hot
wrk -t4 -c64 -d30s --latency http://127.0.0.1:19000/bench/hot
wrk -t4 -c64 -d10s --latency -s bench/put.lua http://127.0.0.1:19000/bench/hot-write
cargo run --release --example bench
```

The overwrite test sends a 5,760-byte payload to one key.
The benchmark example also tests reads from independent keys.
