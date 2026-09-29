use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    ServerSideEncryption,
};
use aws_sdk_s3::Client;
use s3_memory::MemoryS3;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

fn client(endpoint: String) -> Client {
    Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("test", "test", None, None, "tests"))
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .build(),
    )
}

/// Sends one HTTP/1.1 request and returns the status line, lowercase headers, and body.
fn raw_request(endpoint: &str, method: &str, target: &str, headers: &str, body: &[u8]) -> String {
    let address = endpoint.strip_prefix("http://").unwrap();
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    write!(
        stream,
        "{method} {target} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn status(response: &str) -> u16 {
    response[9..12].parse().unwrap()
}

fn failure<T, E>(result: Result<T, aws_sdk_s3::error::SdkError<E>>) -> u16 {
    let Err(error) = result else {
        panic!("request succeeded");
    };
    error.raw_response().unwrap().status().as_u16()
}

fn path_and_query(uri: &str) -> &str {
    let rest = uri.strip_prefix("http://").unwrap();
    &rest[rest.find('/').unwrap()..]
}

#[tokio::test]
async fn bucket_objects_ranges_copy_listing_and_deletion() {
    let server = MemoryS3::new().start().await.unwrap();
    let s3 = client(server.endpoint());
    assert!(s3.head_bucket().bucket("testing").send().await.is_err());
    s3.create_bucket().bucket("testing").send().await.unwrap();
    s3.head_bucket().bucket("testing").send().await.unwrap();
    assert_eq!(s3.list_buckets().send().await.unwrap().buckets().len(), 1);

    for key in ["a/one", "a/two", "b/three"] {
        s3.put_object()
            .bucket("testing")
            .key(key)
            .body(ByteStream::from_static(b"abcdef"))
            .send()
            .await
            .unwrap();
    }
    assert_eq!(
        s3.list_objects()
            .bucket("testing")
            .send()
            .await
            .unwrap()
            .contents()
            .len(),
        3
    );
    let object = s3
        .get_object()
        .bucket("testing")
        .key("a/one")
        .range("bytes=2-4")
        .send()
        .await
        .unwrap();
    assert_eq!(
        object.body.collect().await.unwrap().into_bytes().as_ref(),
        b"cde"
    );
    assert_eq!(
        s3.head_object()
            .bucket("testing")
            .key("a/one")
            .send()
            .await
            .unwrap()
            .content_length(),
        Some(6)
    );

    let first = s3
        .list_objects_v2()
        .bucket("testing")
        .prefix("a/")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.contents().len(), 1);
    assert_eq!(first.is_truncated(), Some(true));
    let second = s3
        .list_objects_v2()
        .bucket("testing")
        .prefix("a/")
        .continuation_token(first.next_continuation_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(second.contents().len(), 1);
    let grouped = s3
        .list_objects_v2()
        .bucket("testing")
        .delimiter("/")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(grouped.common_prefixes()[0].prefix(), Some("a/"));
    let next_group = s3
        .list_objects_v2()
        .bucket("testing")
        .delimiter("/")
        .continuation_token(grouped.next_continuation_token().unwrap())
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(next_group.common_prefixes()[0].prefix(), Some("b/"));
    s3.copy_object()
        .bucket("testing")
        .key("copied")
        .copy_source("testing/a/one")
        .send()
        .await
        .unwrap();
    assert_eq!(
        s3.get_object()
            .bucket("testing")
            .key("copied")
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .as_ref(),
        b"abcdef"
    );

    let all = s3.list_objects_v2().bucket("testing").send().await.unwrap();
    let keys = all
        .contents()
        .iter()
        .map(|o| o.key().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["a/one", "a/two", "b/three", "copied"]);
    s3.put_object()
        .bucket("testing")
        .key("special & <key>")
        .body(ByteStream::from_static(b"special"))
        .send()
        .await
        .unwrap();
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("a/one").build().unwrap())
        .objects(ObjectIdentifier::builder().key("a/two").build().unwrap())
        .objects(
            ObjectIdentifier::builder()
                .key("special & <key>")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    s3.delete_objects()
        .bucket("testing")
        .delete(delete)
        .send()
        .await
        .unwrap();
    s3.delete_object()
        .bucket("testing")
        .key("b/three")
        .send()
        .await
        .unwrap();
    s3.delete_object()
        .bucket("testing")
        .key("copied")
        .send()
        .await
        .unwrap();
    s3.delete_bucket().bucket("testing").send().await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn get_applies_response_header_overrides() {
    let server = MemoryS3::new().start().await.unwrap();
    let s3 = client(server.endpoint());
    s3.create_bucket().bucket("testing").send().await.unwrap();
    s3.put_object()
        .bucket("testing")
        .key("brief.txt")
        .content_type("text/plain")
        .body(ByteStream::from_static(b"brief"))
        .send()
        .await
        .unwrap();
    let object = s3
        .get_object()
        .bucket("testing")
        .key("brief.txt")
        .response_content_disposition("attachment; filename=\"brief.txt\"")
        .response_content_type("application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(
        object.content_disposition(),
        Some("attachment; filename=\"brief.txt\"")
    );
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(
        object.body.collect().await.unwrap().into_bytes().as_ref(),
        b"brief"
    );
}

#[tokio::test]
async fn multipart_upload_and_abort() {
    let store = MemoryS3::new();
    let server = store.start().await.unwrap();
    let s3 = client(server.endpoint());
    s3.create_bucket().bucket("multipart").send().await.unwrap();
    let upload = s3
        .create_multipart_upload()
        .bucket("multipart")
        .key("large")
        .content_type("text/plain")
        .metadata("purpose", "test")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    assert_eq!(
        s3.list_multipart_uploads()
            .bucket("multipart")
            .send()
            .await
            .unwrap()
            .uploads()
            .len(),
        1
    );
    let mut parts = Vec::new();
    for (number, body) in [(1, b"hello".as_slice()), (2, b"world".as_slice())] {
        let part = s3
            .upload_part()
            .bucket("multipart")
            .key("large")
            .upload_id(&upload)
            .part_number(number)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .e_tag(part.e_tag().unwrap())
                .build(),
        );
    }
    assert_eq!(store.stored_bytes(), 10);
    assert_eq!(
        s3.list_parts()
            .bucket("multipart")
            .key("large")
            .upload_id(&upload)
            .send()
            .await
            .unwrap()
            .parts()
            .len(),
        2
    );
    s3.complete_multipart_upload()
        .bucket("multipart")
        .key("large")
        .upload_id(&upload)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 10);
    let head = s3
        .head_object()
        .bucket("multipart")
        .key("large")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(
        head.metadata()
            .and_then(|m| m.get("purpose"))
            .map(String::as_str),
        Some("test")
    );
    assert!(head.e_tag().unwrap().ends_with("-2\""));
    assert_eq!(
        s3.get_object()
            .bucket("multipart")
            .key("large")
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .as_ref(),
        b"helloworld"
    );
    let abandoned = s3
        .create_multipart_upload()
        .bucket("multipart")
        .key("abandoned")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    s3.abort_multipart_upload()
        .bucket("multipart")
        .key("abandoned")
        .upload_id(abandoned)
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 10);
    s3.delete_object()
        .bucket("multipart")
        .key("large")
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 0);
    s3.delete_bucket().bucket("multipart").send().await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn capacity_applies_to_http_objects_and_multipart_parts() {
    let store = MemoryS3::new().with_max_stored_bytes(8);
    let server = store.start().await.unwrap();
    let s3 = client(server.endpoint());
    s3.create_bucket().bucket("limited").send().await.unwrap();
    s3.put_object()
        .bucket("limited")
        .key("keep")
        .body(ByteStream::from_static(b"123456"))
        .send()
        .await
        .unwrap();
    assert!(s3
        .put_object()
        .bucket("limited")
        .key("keep")
        .body(ByteStream::from_static(b"123456789"))
        .send()
        .await
        .is_err());
    assert_eq!(store.stored_bytes(), 6);
    assert_eq!(store.get_object("limited", "keep").unwrap(), "123456");
    assert!(s3
        .copy_object()
        .bucket("limited")
        .key("copy")
        .copy_source("limited/keep")
        .send()
        .await
        .is_err());
    assert_eq!(store.stored_bytes(), 6);
    let id = s3
        .create_multipart_upload()
        .bucket("limited")
        .key("pending")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    assert!(s3
        .upload_part()
        .bucket("limited")
        .key("pending")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"123"))
        .send()
        .await
        .is_err());
    let part = s3
        .upload_part()
        .bucket("limited")
        .key("pending")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"12"))
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 8);
    s3.abort_multipart_upload()
        .bucket("limited")
        .key("pending")
        .upload_id(id)
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 6);
    assert!(part.e_tag().is_some());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn health_reset_and_container_listener() {
    let unspecified = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
    assert!(MemoryS3::new().listen(unspecified).await.is_err());
    let store = MemoryS3::new().with_container_listener();
    let server = store.listen(unspecified).await.unwrap();
    let endpoint = server.endpoint().replace("0.0.0.0", "127.0.0.1");
    let health = raw_request(&endpoint, "GET", "/__health", "", b"");
    assert_eq!(status(&health), 200, "{health}");
    assert!(health.ends_with("\r\n\r\n"), "{health}");

    let s3 = client(endpoint.clone());
    s3.create_bucket().bucket("reset").send().await.unwrap();
    s3.put_object()
        .bucket("reset")
        .key("kept")
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let handle = store.object_handle("reset", "kept").unwrap();
    let upload = s3
        .create_multipart_upload()
        .bucket("reset")
        .key("pending")
        .send()
        .await
        .unwrap();
    s3.upload_part()
        .bucket("reset")
        .key("pending")
        .upload_id(upload.upload_id().unwrap())
        .part_number(1)
        .body(ByteStream::from_static(b"de"))
        .send()
        .await
        .unwrap();
    assert_eq!(store.stored_bytes(), 5);
    let reset = raw_request(&endpoint, "POST", "/__reset", "", b"");
    assert_eq!(status(&reset), 204, "{reset}");
    assert_eq!(store.stored_bytes(), 0);
    assert!(handle.get().is_none());
    s3.head_bucket().bucket("reset").send().await.unwrap();
    let listing = s3.list_objects_v2().bucket("reset").send().await.unwrap();
    assert!(listing.contents().is_empty());
    let uploads = s3
        .list_multipart_uploads()
        .bucket("reset")
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn presigned_requests_and_response_overrides() {
    let server = MemoryS3::new().start().await.unwrap();
    let endpoint = server.endpoint();
    let s3 = client(endpoint.clone());
    s3.create_bucket().bucket("signed").send().await.unwrap();
    let expiry = PresigningConfig::expires_in(Duration::from_secs(600)).unwrap();
    let put = s3
        .put_object()
        .bucket("signed")
        .key("report.txt")
        .content_type("text/plain")
        .presigned(expiry.clone())
        .await
        .unwrap();
    assert!(put.uri().contains("X-Amz-Signature="));
    let stored = raw_request(
        &endpoint,
        "PUT",
        path_and_query(put.uri()),
        "Content-Type: text/plain\r\n",
        b"report",
    );
    assert_eq!(status(&stored), 200, "{stored}");
    let get = s3
        .get_object()
        .bucket("signed")
        .key("report.txt")
        .presigned(expiry.clone())
        .await
        .unwrap();
    let read = raw_request(&endpoint, "GET", path_and_query(get.uri()), "", b"");
    assert_eq!(status(&read), 200, "{read}");
    assert!(read.ends_with("\r\n\r\nreport"), "{read}");

    let overridden = s3
        .get_object()
        .bucket("signed")
        .key("report.txt")
        .response_cache_control("no-cache")
        .response_content_type("application/octet-stream")
        .presigned(expiry)
        .await
        .unwrap();
    let target = path_and_query(overridden.uri());
    let full = raw_request(&endpoint, "GET", target, "", b"");
    assert!(full.contains("cache-control: no-cache\r\n"), "{full}");
    assert!(
        full.contains("content-type: application/octet-stream\r\n"),
        "{full}"
    );
    let partial = raw_request(&endpoint, "GET", target, "Range: bytes=0-2\r\n", b"");
    assert_eq!(status(&partial), 206, "{partial}");
    assert!(
        partial.contains("content-type: text/plain\r\n"),
        "{partial}"
    );
    assert!(!partial.contains("cache-control"), "{partial}");
    let invalid = raw_request(
        &endpoint,
        "GET",
        "/signed/report.txt?response-expires=%0A",
        "",
        b"",
    );
    assert_eq!(status(&invalid), 400, "{invalid}");
    let unsupported = raw_request(
        &endpoint,
        "GET",
        "/signed/report.txt?response-x-custom=1",
        "",
        b"",
    );
    assert_eq!(status(&unsupported), 501, "{unsupported}");
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_writes_and_reads() {
    let server = MemoryS3::new().start().await.unwrap();
    let endpoint = server.endpoint();
    let s3 = client(endpoint.clone());
    s3.create_bucket().bucket("cond").send().await.unwrap();
    let put = |key: &'static str, body: &'static [u8]| {
        s3.put_object()
            .bucket("cond")
            .key(key)
            .body(ByteStream::from_static(body))
    };
    let etag = put("doc", b"one")
        .if_none_match("*")
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    assert_eq!(
        failure(put("doc", b"two").if_none_match("*").send().await),
        412
    );
    assert_eq!(
        failure(put("doc", b"two").if_match("\"other\"").send().await),
        412
    );
    assert_eq!(
        failure(put("absent", b"two").if_match(&etag).send().await),
        412
    );
    assert_eq!(
        failure(put("doc", b"two").if_none_match(&etag).send().await),
        400
    );
    let copy = raw_request(
        &endpoint,
        "PUT",
        "/cond/doc",
        "x-amz-copy-source: cond/doc\r\nIf-None-Match: *\r\n",
        b"",
    );
    assert_eq!(status(&copy), 412, "{copy}");
    put("doc", b"two")
        .if_match(etag.trim_matches('"'))
        .send()
        .await
        .unwrap();
    let current = s3
        .head_object()
        .bucket("cond")
        .key("doc")
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();

    let stale = raw_request(
        &endpoint,
        "GET",
        "/cond/doc",
        &format!("If-Match: {etag}\r\n"),
        b"",
    );
    assert_eq!(status(&stale), 412, "{stale}");
    let stale_head = raw_request(
        &endpoint,
        "HEAD",
        "/cond/doc",
        &format!("If-Match: {etag}\r\n"),
        b"",
    );
    assert_eq!(status(&stale_head), 412, "{stale_head}");
    let any = raw_request(&endpoint, "GET", "/cond/doc", "If-Match: *\r\n", b"");
    assert_eq!(status(&any), 200, "{any}");
    let unchanged = raw_request(
        &endpoint,
        "GET",
        "/cond/doc",
        &format!("If-None-Match: \"other\", W/{current}\r\n"),
        b"",
    );
    assert_eq!(status(&unchanged), 304, "{unchanged}");
    assert!(
        unchanged.contains(&format!("etag: {current}\r\n")),
        "{unchanged}"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aws_chunked_uploads_store_decoded_bytes() {
    let store = MemoryS3::new();
    let server = store.start().await.unwrap();
    let endpoint = server.endpoint();
    let s3 = client(endpoint.clone());
    s3.create_bucket().bucket("chunked").send().await.unwrap();
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("aws-chunked-body");
    std::fs::write(&path, b"streamed body").unwrap();
    s3.put_object()
        .bucket("chunked")
        .key("file")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from_path(&path).await.unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        store.get_object("chunked", "file").unwrap(),
        "streamed body"
    );
    let malformed = raw_request(
        &endpoint,
        "PUT",
        "/chunked/file",
        "Content-Encoding: aws-chunked\r\nx-amz-decoded-content-length: 9\r\n",
        b"3\r\nabc\r\n0\r\n\r\n",
    );
    assert_eq!(status(&malformed), 400, "{malformed}");
    assert_eq!(
        store.get_object("chunked", "file").unwrap(),
        "streamed body"
    );
    server.stop().await;
}

#[tokio::test]
async fn server_side_encryption_is_stored_and_returned() {
    let server = MemoryS3::new().start().await.unwrap();
    let s3 = client(server.endpoint());
    s3.create_bucket().bucket("sse").send().await.unwrap();
    s3.put_object()
        .bucket("sse")
        .key("plain")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(b"secret"))
        .send()
        .await
        .unwrap();
    let get = s3
        .get_object()
        .bucket("sse")
        .key("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    s3.copy_object()
        .bucket("sse")
        .key("copy")
        .copy_source("sse/plain")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await
        .unwrap();
    let copy = s3
        .head_object()
        .bucket("sse")
        .key("copy")
        .send()
        .await
        .unwrap();
    assert_eq!(
        copy.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    let upload = s3
        .create_multipart_upload()
        .bucket("sse")
        .key("parts")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let part = s3
        .upload_part()
        .bucket("sse")
        .key("parts")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .unwrap();
    s3.complete_multipart_upload()
        .bucket("sse")
        .key("parts")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let head = s3
        .head_object()
        .bucket("sse")
        .key("parts")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cors_headers_follow_allowed_origins() {
    let server = MemoryS3::new().start().await.unwrap();
    let endpoint = server.endpoint();
    let allowed = "Origin: http://localhost:3000\r\n";
    let preflight = raw_request(
        &endpoint,
        "OPTIONS",
        "/web/file",
        &format!("{allowed}Access-Control-Request-Headers: content-type, x-amz-date\r\n"),
        b"",
    );
    assert_eq!(status(&preflight), 204, "{preflight}");
    for header in [
        "access-control-allow-origin: http://localhost:3000\r\n",
        "access-control-allow-methods: GET, HEAD, PUT, POST, DELETE\r\n",
        "access-control-allow-headers: content-type, x-amz-date\r\n",
        "access-control-expose-headers: ETag, Content-Length, x-amz-server-side-encryption\r\n",
        "vary: Origin\r\n",
    ] {
        assert!(
            preflight.contains(header),
            "missing {header:?}: {preflight}"
        );
    }
    raw_request(&endpoint, "PUT", "/web", "", b"");
    let put = raw_request(&endpoint, "PUT", "/web/file", allowed, b"data");
    assert!(
        put.contains("access-control-allow-origin: http://localhost:3000\r\n"),
        "{put}"
    );
    let get = raw_request(&endpoint, "GET", "/web/file", allowed, b"");
    assert!(
        get.contains("access-control-allow-origin: http://localhost:3000\r\n"),
        "{get}"
    );
    let other = raw_request(
        &endpoint,
        "GET",
        "/web/file",
        "Origin: http://example.com\r\n",
        b"",
    );
    assert_eq!(status(&other), 200, "{other}");
    assert!(!other.contains("access-control-"), "{other}");
    server.stop().await;

    let server = MemoryS3::new()
        .with_cors_origins(["http://example.com"])
        .start()
        .await
        .unwrap();
    let endpoint = server.endpoint();
    let custom = raw_request(
        &endpoint,
        "GET",
        "/__health",
        "Origin: http://example.com\r\n",
        b"",
    );
    assert!(
        custom.contains("access-control-allow-origin: http://example.com\r\n"),
        "{custom}"
    );
    let default = raw_request(&endpoint, "GET", "/__health", allowed, b"");
    assert!(!default.contains("access-control-"), "{default}");
    server.stop().await;
}
