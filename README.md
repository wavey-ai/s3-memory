# S3 Memory

`s3-memory` is an in-memory test server for a defined subset of the Amazon S3 HTTP API.
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
PutObject stores content type and user metadata.
Unsupported API operations return `NotImplemented`.

This server does not apply IAM, ACL, versioning, bucket configuration, or retention rules.
It is a test service, and it accepts requests only on loopback addresses.
