use crate::{Bucket, Object, Reply, State};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http_body_util::Full;
use hyper::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE};
use hyper::{Method, StatusCode};
use md5::{Digest, Md5};
use percent_encoding::percent_decode_str;
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;
use url::form_urlencoded;

mod multipart;

type Query = HashMap<String, String>;

pub(crate) fn route(
    state: &mut State,
    parts: &hyper::http::request::Parts,
    payload: Bytes,
) -> Reply {
    let path = percent_decode_str(parts.uri.path()).decode_utf8_lossy();
    let mut path_parts = path.trim_start_matches('/').splitn(2, '/');
    let bucket = path_parts.next().unwrap_or("");
    let key = path_parts.next().filter(|key| !key.is_empty());
    let mut query: Query = form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect();
    query.remove("x-id");

    if bucket.is_empty() {
        return if parts.method == Method::GET {
            list_buckets(state)
        } else {
            error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", &path)
        };
    }
    let Some(key) = key else {
        return bucket_request(state, &parts.method, bucket, &query, &payload);
    };
    object_request(
        state,
        &parts.method,
        bucket,
        key,
        &query,
        &parts.headers,
        payload,
    )
}

fn bucket_request(
    state: &mut State,
    method: &Method,
    bucket: &str,
    query: &Query,
    payload: &[u8],
) -> Reply {
    match *method {
        Method::PUT if query.is_empty() => {
            if state.buckets.contains_key(bucket) {
                return error(StatusCode::CONFLICT, "BucketAlreadyOwnedByYou", bucket);
            }
            state.buckets.insert(
                bucket.to_owned(),
                Bucket {
                    created: SystemTime::now(),
                    ..Bucket::default()
                },
            );
            empty(StatusCode::OK)
        }
        Method::HEAD if query.is_empty() => empty(if state.buckets.contains_key(bucket) {
            StatusCode::OK
        } else {
            StatusCode::NOT_FOUND
        }),
        Method::DELETE if query.is_empty() => {
            let Some(existing) = state.buckets.get(bucket) else {
                return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
            };
            if !existing.objects.is_empty() || state.uploads.values().any(|u| u.bucket == bucket) {
                return error(StatusCode::CONFLICT, "BucketNotEmpty", bucket);
            }
            state.buckets.remove(bucket);
            empty(StatusCode::NO_CONTENT)
        }
        Method::POST if query.contains_key("delete") => {
            multipart::delete_objects(state, bucket, payload)
        }
        Method::GET if query.contains_key("uploads") => multipart::list_uploads(state, bucket),
        Method::GET
            if query.is_empty()
                || query.contains_key("list-type")
                || query.contains_key("prefix")
                || query.contains_key("delimiter")
                || query.contains_key("marker") =>
        {
            list_objects(state, bucket, query)
        }
        _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", bucket),
    }
}

fn object_request(
    state: &mut State,
    method: &Method,
    bucket: &str,
    key: &str,
    query: &Query,
    headers: &hyper::HeaderMap,
    payload: Bytes,
) -> Reply {
    if !state.buckets.contains_key(bucket) {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    }
    if query.contains_key("uploads") && *method == Method::POST {
        return multipart::create_upload(state, bucket, key, headers);
    }
    if let Some(id) = query.get("uploadId") {
        return multipart::upload_request(state, method, bucket, key, id, query, payload);
    }
    if !query.is_empty() {
        return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", key);
    }
    if *method == Method::PUT {
        if let Some(source) = headers.get("x-amz-copy-source") {
            let source = source.to_str().unwrap_or("");
            let source = percent_decode_str(source.trim_start_matches('/')).decode_utf8_lossy();
            let Some((source_bucket, source_key)) = source.split_once('/') else {
                return error(StatusCode::BAD_REQUEST, "InvalidArgument", key);
            };
            let source_key = source_key.split('?').next().unwrap_or(source_key);
            let Some(original) = state
                .buckets
                .get(source_bucket)
                .and_then(|b| b.objects.get(source_key))
                .cloned()
            else {
                return error(StatusCode::NOT_FOUND, "NoSuchKey", source_key);
            };
            let copied = Object {
                modified: SystemTime::now(),
                ..original
            };
            state
                .buckets
                .get_mut(bucket)
                .unwrap()
                .objects
                .insert(key.to_owned(), copied.clone());
            return xml(StatusCode::OK, format!(
                "<CopyObjectResult><LastModified>{}</LastModified><ETag>{}</ETag></CopyObjectResult>",
                timestamp(copied.modified), copied.etag,
            ));
        }
        let object = make_object(payload, headers);
        let etag = object.etag.clone();
        state
            .buckets
            .get_mut(bucket)
            .unwrap()
            .objects
            .insert(key.to_owned(), object);
        return hyper::Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, etag)
            .body(Full::new(Bytes::new()))
            .unwrap();
    }
    if *method == Method::DELETE {
        state.buckets.get_mut(bucket).unwrap().objects.remove(key);
        return empty(StatusCode::NO_CONTENT);
    }
    if *method == Method::GET || *method == Method::HEAD {
        let Some(object) = state.buckets.get(bucket).and_then(|b| b.objects.get(key)) else {
            return if *method == Method::HEAD {
                empty(StatusCode::NOT_FOUND)
            } else {
                error(StatusCode::NOT_FOUND, "NoSuchKey", key)
            };
        };
        return read_object(object, method, headers);
    }
    error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", key)
}

pub(crate) fn make_object(bytes: Bytes, headers: &hyper::HeaderMap) -> Object {
    let digest = Md5::digest(&bytes);
    let metadata = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            name.starts_with("x-amz-meta-")
                .then(|| (name.to_owned(), value.to_str().unwrap_or("").to_owned()))
        })
        .collect();
    Object {
        bytes,
        etag: format!("\"{digest:x}\""),
        modified: SystemTime::now(),
        content_type: headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        metadata,
    }
}

fn read_object(object: &Object, method: &Method, headers: &hyper::HeaderMap) -> Reply {
    let len = object.bytes.len();
    let range = headers.get(RANGE).and_then(|value| value.to_str().ok());
    let (status, start, end) = match range {
        Some(range) => match parse_range(range, len) {
            Some((start, end)) => (StatusCode::PARTIAL_CONTENT, start, end),
            None => {
                return hyper::Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(CONTENT_RANGE, format!("bytes */{len}"))
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            }
        },
        None => (StatusCode::OK, 0, len),
    };
    let mut response = hyper::Response::builder()
        .status(status)
        .header(ETAG, object.etag.as_str())
        .header(LAST_MODIFIED, httpdate::fmt_http_date(object.modified))
        .header(CONTENT_LENGTH, (end - start).to_string());
    if status == StatusCode::PARTIAL_CONTENT {
        response = response.header(CONTENT_RANGE, format!("bytes {start}-{}/{len}", end - 1));
    }
    if let Some(content_type) = &object.content_type {
        response = response.header(CONTENT_TYPE, content_type);
    }
    for (name, value) in &object.metadata {
        response = response.header(name, value);
    }
    let body = if *method == Method::HEAD {
        Bytes::new()
    } else {
        object.bytes.slice(start..end)
    };
    response.body(Full::new(body)).unwrap()
}

fn parse_range(value: &str, len: usize) -> Option<(usize, usize)> {
    let range = value.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    if range.contains(',') || len == 0 {
        return None;
    }
    if start.is_empty() {
        let suffix: usize = end.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        return Some((len.saturating_sub(suffix), len));
    }
    let start: usize = start.parse().ok()?;
    if start >= len {
        return None;
    }
    let end = if end.is_empty() {
        len
    } else {
        end.parse::<usize>().ok()?.saturating_add(1).min(len)
    };
    (end > start).then_some((start, end))
}

fn list_buckets(state: &State) -> Reply {
    let mut result = String::from("<ListAllMyBucketsResult><Buckets>");
    for (name, bucket) in &state.buckets {
        result.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            escape(name),
            timestamp(bucket.created)
        ));
    }
    result.push_str("</Buckets></ListAllMyBucketsResult>");
    xml(StatusCode::OK, result)
}

fn list_objects(state: &State, bucket: &str, query: &Query) -> Reply {
    let Some(bucket_state) = state.buckets.get(bucket) else {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    };
    let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
    let delimiter = query.get("delimiter").map(String::as_str).unwrap_or("");
    let after = query
        .get("continuation-token")
        .or_else(|| query.get("start-after"))
        .or_else(|| query.get("marker"))
        .map(String::as_str)
        .unwrap_or("");
    let max = query
        .get("max-keys")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);
    let mut entries: BTreeMap<String, (String, Option<&Object>)> = BTreeMap::new();
    for (key, object) in &bucket_state.objects {
        if !key.starts_with(prefix) || (!after.is_empty() && key.as_str() <= after) {
            continue;
        }
        if !delimiter.is_empty() {
            if let Some(index) = key[prefix.len()..].find(delimiter) {
                let common_prefix = &key[..prefix.len() + index + delimiter.len()];
                entries.insert(common_prefix.to_owned(), (key.clone(), None));
                continue;
            }
        }
        entries.insert(key.clone(), (key.clone(), Some(object)));
    }
    let entries = entries.into_iter().collect::<Vec<_>>();
    let truncated = max > 0 && entries.len() > max;
    let selected = &entries[..entries.len().min(max)];
    let v2 = query.get("list-type").is_some_and(|value| value == "2");
    let mut result = format!("<ListBucketResult><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        escape(bucket), escape(prefix));
    if v2 {
        result.push_str(&format!("<KeyCount>{}</KeyCount>", selected.len()));
    }
    for (key, (_, object)) in selected {
        if let Some(object) = object {
            result.push_str(&format!("<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                escape(key), timestamp(object.modified), object.etag, object.bytes.len()));
        } else {
            result.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                escape(key)
            ));
        }
    }
    if truncated {
        if let Some((_, (last, _))) = selected.last() {
            let tag = if v2 {
                "NextContinuationToken"
            } else {
                "NextMarker"
            };
            result.push_str(&format!("<{tag}>{}</{tag}>", escape(last)));
        }
    }
    result.push_str("</ListBucketResult>");
    xml(StatusCode::OK, result)
}

pub(crate) fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub(crate) fn timestamp(time: SystemTime) -> String {
    DateTime::<Utc>::from(time)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

pub(crate) fn xml(status: StatusCode, body: String) -> Reply {
    hyper::Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

pub(crate) fn error(status: StatusCode, code: &str, resource: &str) -> Reply {
    xml(
        status,
        format!(
            "<Error><Code>{code}</Code><Message>{code}</Message><Resource>{}</Resource></Error>",
            escape(resource)
        ),
    )
}

pub(crate) fn empty(status: StatusCode) -> Reply {
    hyper::Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_boundaries() {
        assert_eq!(parse_range("bytes=2-4", 10), Some((2, 5)));
        assert_eq!(parse_range("bytes=-3", 10), Some((7, 10)));
        assert_eq!(parse_range("bytes=10-", 10), None);
        assert_eq!(parse_range("bytes=4-2", 10), None);
    }
}
