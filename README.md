# S3 Memory

`s3-memory` is an in-memory store and test server for a defined subset of the Amazon S3 HTTP API.
It stores buckets, objects, and multipart uploads in memory.
The server listens on a loopback address and accepts test credentials without signature checks.
Set `S3_MEMORY_CONTAINER=1` to let the binary listen on `0.0.0.0` or `::` inside a container.
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
`with_container_listener` also lets `listen` accept an unspecified address, such as `0.0.0.0`.
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
GetObject and HeadObject apply these query parameters as response headers on a complete (200) response:
`response-cache-control`, `response-content-disposition`, `response-content-encoding`,
`response-content-language`, `response-content-type`, and `response-expires`.
An override value that is not a valid header value returns `InvalidArgument` with HTTP status 400.
Other query parameters on an object request return `NotImplemented`.

Routing ignores the `x-id` query parameter and every query parameter whose name starts with `x-amz-`, in any case.
A presigned URL selects the same operation as a request that carries its signature in headers.

PutObject stores content type, user metadata, and `x-amz-server-side-encryption`.
CreateMultipartUpload stores the same values for the completed object.
CopyObject keeps the source values; a `x-amz-server-side-encryption` header on the copy request replaces the stored value.
GetObject and HeadObject return `x-amz-server-side-encryption` when the object has it.

PutObject and UploadPart decode `aws-chunked` bodies to the length in `x-amz-decoded-content-length` and discard trailers.
A malformed body or a body of a different length returns `InvalidRequest` with HTTP status 400.

PutObject and CopyObject apply these preconditions:

| Header | Result |
| --- | --- |
| `If-None-Match: *` | 412 `PreconditionFailed` when the key exists |
| `If-Match: <etag>` | 412 `PreconditionFailed` when the key is missing or its ETag differs; quotes are ignored |
| `If-None-Match` with a value other than `*` | 400 `InvalidArgument` |
| `If-Match` with a value that is not valid text | 400 `InvalidArgument` |

GetObject and HeadObject apply these preconditions:

| Header | Result |
| --- | --- |
| `If-Match` | 412 `PreconditionFailed` when no listed ETag matches; HEAD returns an empty body |
| `If-None-Match` | 304 Not Modified with the `ETag` header when a listed ETag matches |

Both headers accept `*`, a comma-separated list, and weak `W/` tags.
The server evaluates `If-None-Match` only when `If-Match` is absent.

Unsupported API operations return `NotImplemented`.

## Test control and browser access

| Request | Result |
| --- | --- |
| `GET /__health` | 200 with an empty body |
| `POST /__reset` | 204; removes all objects and multipart uploads, keeps the buckets, and sets `stored_bytes` to 0 |
| `OPTIONS` on any path | 204 with an empty body |

Object handles return `None` after a reset.

Each response to a request from an allowed `Origin` has these headers:

- `Access-Control-Allow-Origin`, with the request origin
- `Access-Control-Allow-Methods: GET, HEAD, PUT, POST, DELETE`
- `Access-Control-Allow-Headers`, with the value of `Access-Control-Request-Headers` when the request has it
- `Access-Control-Expose-Headers: ETag, Content-Length, x-amz-server-side-encryption`
- `Vary: Origin`

The default allowed origins are `http://localhost:3000`, `http://127.0.0.1:3000`, `http://localhost:3001`, and `http://127.0.0.1:3001`.
`with_cors_origins` replaces this list.

This server does not apply IAM, ACL, versioning, bucket configuration, or retention rules.
It is a test service, and it accepts requests only on loopback addresses unless you enable the container listener.

See [BENCHMARK.md](BENCHMARK.md) for load-test commands and measurements.
