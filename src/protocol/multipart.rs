use super::{
    empty, error, escape, header_string, make_object, remove_object, store_object_bounded,
    timestamp, xml, Query,
};
use crate::{Reply, State, Upload};
use bytes::Bytes;
use http_body_util::Full;
use hyper::header::ETAG;
use hyper::{Method, StatusCode};
use md5::{Digest, Md5};
use quick_xml::events::Event;
use quick_xml::Reader;
use uuid::Uuid;

pub(super) fn delete_objects(state: &mut State, bucket: &str, payload: &[u8]) -> Reply {
    let Some(keys) = xml_values(payload, "Key") else {
        return error(StatusCode::BAD_REQUEST, "MalformedXML", bucket);
    };
    let Some(bucket_state) = state.buckets.get_mut(bucket) else {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    };
    let mut result = String::from("<DeleteResult>");
    for key in keys {
        if let Some(size) = remove_object(bucket_state, &key) {
            state.stored_bytes -= size;
        }
        result.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", escape(&key)));
    }
    result.push_str("</DeleteResult>");
    xml(StatusCode::OK, result)
}

pub(super) fn create_upload(
    state: &mut State,
    bucket: &str,
    key: &str,
    headers: &hyper::HeaderMap,
) -> Reply {
    let id = Uuid::new_v4().to_string();
    let metadata = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            name.starts_with("x-amz-meta-")
                .then(|| (name.to_owned(), value.to_str().unwrap_or("").to_owned()))
        })
        .collect();
    state.uploads.insert(
        id.clone(),
        Upload {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            parts: Default::default(),
            content_type: header_string(headers, hyper::header::CONTENT_TYPE.as_str()),
            metadata,
            encryption: header_string(headers, "x-amz-server-side-encryption"),
        },
    );
    xml(StatusCode::OK, format!(
        "<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
        escape(bucket), escape(key),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn upload_request(
    state: &mut State,
    method: &Method,
    bucket: &str,
    key: &str,
    id: &str,
    query: &Query,
    payload: Bytes,
    max_stored_bytes: usize,
) -> Reply {
    let Some(upload) = state.uploads.get_mut(id) else {
        return error(StatusCode::NOT_FOUND, "NoSuchUpload", id);
    };
    if upload.bucket != bucket || upload.key != key {
        return error(StatusCode::NOT_FOUND, "NoSuchUpload", id);
    }
    match *method {
        Method::PUT => {
            let Some(number) = query
                .get("partNumber")
                .and_then(|n| n.parse::<u32>().ok())
                .filter(|n| (1..=10000).contains(n))
            else {
                return error(StatusCode::BAD_REQUEST, "InvalidArgument", key);
            };
            let object = make_object(payload, &hyper::HeaderMap::new());
            let etag = object.etag.clone();
            let old_size = upload.parts.get(&number).map_or(0, |part| part.bytes.len());
            let Some(new_size) = state
                .stored_bytes
                .checked_sub(old_size)
                .and_then(|size| size.checked_add(object.bytes.len()))
            else {
                return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", key);
            };
            if new_size > max_stored_bytes {
                return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", key);
            }
            upload.parts.insert(number, object);
            state.stored_bytes = new_size;
            hyper::Response::builder()
                .status(StatusCode::OK)
                .header(ETAG, etag)
                .body(Full::new(Bytes::new()))
                .unwrap()
        }
        Method::GET => {
            let mut result = format!("<ListPartsResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><IsTruncated>false</IsTruncated>",
                escape(bucket), escape(key), escape(id));
            for (number, part) in &upload.parts {
                result.push_str(&format!("<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag><Size>{}</Size><LastModified>{}</LastModified></Part>",
                    part.etag, part.bytes.len(), timestamp(part.modified)));
            }
            result.push_str("</ListPartsResult>");
            xml(StatusCode::OK, result)
        }
        Method::DELETE => {
            let released = upload
                .parts
                .values()
                .map(|part| part.bytes.len())
                .sum::<usize>();
            state.uploads.remove(id);
            state.stored_bytes -= released;
            empty(StatusCode::NO_CONTENT)
        }
        Method::POST => {
            let Some(numbers) = xml_values(&payload, "PartNumber") else {
                return error(StatusCode::BAD_REQUEST, "MalformedXML", key);
            };
            let Some(etags) = xml_values(&payload, "ETag") else {
                return error(StatusCode::BAD_REQUEST, "MalformedXML", key);
            };
            if numbers.is_empty() || numbers.len() != etags.len() {
                return error(StatusCode::BAD_REQUEST, "InvalidPart", key);
            }
            let mut bytes = Vec::new();
            let mut previous = 0;
            let mut combined_digest = Md5::new();
            let part_count = etags.len();
            for (raw, etag) in numbers.into_iter().zip(etags) {
                let Ok(number) = raw.parse::<u32>() else {
                    return error(StatusCode::BAD_REQUEST, "InvalidPart", key);
                };
                if number <= previous {
                    return error(StatusCode::BAD_REQUEST, "InvalidPartOrder", key);
                }
                let Some(part) = upload.parts.get(&number) else {
                    return error(StatusCode::BAD_REQUEST, "InvalidPart", key);
                };
                if part.etag.trim_matches('"') != etag.trim_matches('"') {
                    return error(StatusCode::BAD_REQUEST, "InvalidPart", key);
                }
                bytes.extend_from_slice(&part.bytes);
                combined_digest.update(Md5::digest(&part.bytes));
                previous = number;
            }
            let mut object = make_object(Bytes::from(bytes), &hyper::HeaderMap::new());
            object.etag = format!("\"{:x}-{part_count}\"", combined_digest.finalize());
            object.content_type = upload.content_type.clone();
            object.metadata = upload.metadata.clone();
            object.encryption = upload.encryption.clone();
            let etag = object.etag.clone();
            let released = upload
                .parts
                .values()
                .map(|part| part.bytes.len())
                .sum::<usize>();
            let old_size = state.buckets[bucket]
                .objects
                .get(key)
                .map_or(0, |entry| entry.object.bytes.len());
            let Some(new_size) = state
                .stored_bytes
                .checked_sub(released)
                .and_then(|size| size.checked_sub(old_size))
                .and_then(|size| size.checked_add(object.bytes.len()))
            else {
                return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", key);
            };
            if new_size > max_stored_bytes {
                return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", key);
            }
            state.stored_bytes -= released;
            store_object_bounded(state, bucket, key.to_owned(), object, max_stored_bytes).unwrap();
            state.uploads.remove(id);
            xml(StatusCode::OK, format!("<CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>{etag}</ETag></CompleteMultipartUploadResult>",
                escape(bucket), escape(key)))
        }
        _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", key),
    }
}

pub(super) fn list_uploads(state: &State, bucket: &str) -> Reply {
    if !state.buckets.contains_key(bucket) {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    }
    let mut result = format!(
        "<ListMultipartUploadsResult><Bucket>{}</Bucket><IsTruncated>false</IsTruncated>",
        escape(bucket)
    );
    for (id, upload) in &state.uploads {
        if upload.bucket == bucket {
            result.push_str(&format!(
                "<Upload><Key>{}</Key><UploadId>{}</UploadId></Upload>",
                escape(&upload.key),
                escape(id)
            ));
        }
    }
    result.push_str("</ListMultipartUploadsResult>");
    xml(StatusCode::OK, result)
}

fn xml_values(payload: &[u8], name: &str) -> Option<Vec<String>> {
    let mut reader = Reader::from_reader(payload);
    let mut values = Vec::new();
    loop {
        match reader.read_event().ok()? {
            Event::Start(event) if event.name().as_ref() == name.as_bytes() => {
                let content = reader.read_text(event.name()).ok()?;
                let decoded = content.decode().ok()?;
                values.push(quick_xml::escape::unescape(&decoded).ok()?.into_owned());
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use super::xml_values;

    #[test]
    fn xml_text_preserves_escaped_key_characters() {
        assert_eq!(
            xml_values(
                b"<Delete><Object><Key>special &amp; &lt;key&gt;</Key></Object></Delete>",
                "Key"
            ),
            Some(vec!["special & <key>".to_owned()]),
        );
    }
}
