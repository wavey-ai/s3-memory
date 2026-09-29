# S3 Memory

`s3-memory` is an in-memory store and test server for a defined subset of the Amazon S3 HTTP API.
It stores buckets, objects, and multipart uploads in memory.
The server listens on a loopback address and accepts test credentials without signature checks.
All state disappears when the process stops.

## Run the server

```sh
cargo run --bin s3-memory -- --listen 127.0.0.1:9000
```

Set an S3 client endpoint to `http://127.0.0.1:9000`.
Use path-style requests and any access key and secret key.

## Use the crate in tests

```rust
use s3_memory::MemoryS3;

let server = MemoryS3::new().start().await?;
let endpoint = server.endpoint();
// Configure your S3 client with endpoint and path-style requests.
server.stop().await;
```

`start` selects an available loopback port.
`listen` accepts a specified loopback address.
`with_max_request_bytes` sets the maximum size of one PUT or POST request.
The default limit is 64 MiB.
`with_max_stored_bytes` sets the maximum total size of stored object payloads and multipart parts.
The default storage limit is 512 MiB.
Writes that exceed this limit return `StorageFull` with HTTP status 507.
The rejected write keeps the previous object or part.

## Direct store API

```rust
use bytes::Bytes;
use s3_memory::MemoryS3;

let store = MemoryS3::new();
store.create_bucket("test");
store.put_object("test", "key", Bytes::from_static(b"value"))?;
let snapshot = store.get_object("test", "key").unwrap();
let handle = store.object_handle("test", "key").unwrap();
assert_eq!(snapshot, "value");
assert_eq!(handle.get().unwrap(), "value");
```

`get_object` returns a `Bytes` snapshot that remains valid after replacement or deletion.
`object_handle` gives repeated reads of one key without a bucket-map lookup.
The handle sees replacement objects and returns `None` after deletion.
Get a new handle if you create the key again after deletion.
`stored_bytes` reports the total payload size that counts against the storage limit.
This total counts each copied object and each multipart part.
Active requests and retained snapshots can make process memory exceed the storage limit.

`store-stream::Storage::new` can use `server.endpoint()` with test credentials.
The `store-stream` resumable API can also use `object_store::memory::InMemory` directly.

## S3 operations

| Area | Operations |
| --- | --- |
| Buckets | CreateBucket, HeadBucket, ListBuckets, DeleteBucket |
| Objects | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject |
| Listing | ListObjects, ListObjectsV2 with prefix, delimiter, and pagination |
| Multipart | CreateMultipartUpload, UploadPart, ListParts, ListMultipartUploads, CompleteMultipartUpload, AbortMultipartUpload |

GetObject and HeadObject accept one HTTP byte range.
GetObject and HeadObject apply the `response-*` query parameters, such as `response-content-disposition`, as response headers.
PutObject stores content type and user metadata.
Unsupported API operations return `NotImplemented`.

This server does not apply IAM, ACL, versioning, bucket configuration, or retention rules.
It is a test service, and it accepts requests only on loopback addresses.

See [BENCHMARK.md](BENCHMARK.md) for load-test commands and measurements.
