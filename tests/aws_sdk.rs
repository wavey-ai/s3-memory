use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use s3_memory::MemoryS3;

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
async fn multipart_upload_and_abort() {
    let server = MemoryS3::new().start().await.unwrap();
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
    s3.delete_object()
        .bucket("multipart")
        .key("large")
        .send()
        .await
        .unwrap();
    s3.delete_bucket().bucket("multipart").send().await.unwrap();
    server.stop().await;
}
