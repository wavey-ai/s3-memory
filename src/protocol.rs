use crate::{Bucket, Object, ObjectEntry, Reply, State};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http_body_util::Full;
use hyper::header::{
    HeaderName, HeaderValue, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING,
    CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, IF_MATCH,
    IF_NONE_MATCH, LAST_MODIFIED, RANGE,
};
use hyper::{Method, StatusCode};
use md5::{Digest, Md5};
use percent_encoding::percent_decode_str;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;
use url::form_urlencoded;

mod multipart;

type Query = HashMap<String, String>;

pub(crate) enum HotRead {
    Found(Arc<Object>),
    MissingBucket(String),
    MissingKey(String),
}

pub(crate) fn hot_lookup(state: &State, parts: &hyper::http::request::Parts) -> Option<HotRead> {
    if parts.method != Method::GET && parts.method != Method::HEAD {
        return None;
    }
    if parts.headers.contains_key(IF_MATCH) || parts.headers.contains_key(IF_NONE_MATCH) {
        return None;
    }
    if parts.uri.query().is_some_and(|query| {
        form_urlencoded::parse(query.as_bytes()).any(|(name, _)| !ignored_parameter(&name))
    }) {
        return None;
    }
    let path = percent_decode_str(parts.uri.path()).decode_utf8_lossy();
    let (bucket, key) = path.trim_start_matches('/').split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some(match state.buckets.get(bucket) {
        None => HotRead::MissingBucket(bucket.to_owned()),
        Some(bucket_state) => match bucket_state.objects.get(key) {
            Some(entry) => HotRead::Found(Arc::clone(&entry.object)),
            None => HotRead::MissingKey(key.to_owned()),
        },
    })
}

/// Presigned URLs carry their signature and checksum settings in `x-amz-*` parameters,
/// and SDKs add `x-id`. None of these parameters selects an operation.
fn ignored_parameter(name: &str) -> bool {
    name == "x-id"
        || name
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-amz-"))
}

pub(crate) fn basic_put_path(parts: &hyper::http::request::Parts) -> Option<(String, String)> {
    if parts.method != Method::PUT || parts.headers.contains_key("x-amz-copy-source") {
        return None;
    }
    if parts.uri.query().is_some_and(|query| {
        form_urlencoded::parse(query.as_bytes()).any(|(name, _)| !ignored_parameter(&name))
    }) {
        return None;
    }
    let path = percent_decode_str(parts.uri.path()).decode_utf8_lossy();
    let (bucket, key) = path.trim_start_matches('/').split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_owned(), key.to_owned()))
}

pub(crate) fn write_object(
    state: &mut State,
    bucket: &str,
    key: String,
    object: Object,
    headers: &hyper::HeaderMap,
    max_stored_bytes: usize,
) -> Reply {
    let Some(bucket_state) = state.buckets.get(bucket) else {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    };
    if let Some((status, code)) = put_precondition(bucket_state, &key, headers) {
        return error(status, code, &key);
    }
    let etag = object.etag.clone();
    if store_object_bounded(state, bucket, key.clone(), object, max_stored_bytes).is_err() {
        return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", &key);
    }
    hyper::Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, etag)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

/// Applies `If-None-Match: *` and `If-Match: <etag>` to a write and returns the failure.
fn put_precondition(
    bucket: &Bucket,
    key: &str,
    headers: &hyper::HeaderMap,
) -> Option<(StatusCode, &'static str)> {
    let if_match = headers.get(IF_MATCH);
    let if_none_match = headers.get(IF_NONE_MATCH);
    if if_match.is_none() && if_none_match.is_none() {
        return None;
    }
    let if_match = match if_match.map(HeaderValue::to_str) {
        Some(Err(_)) => return Some((StatusCode::BAD_REQUEST, "InvalidArgument")),
        Some(Ok(value)) => Some(value.trim().trim_matches('"')),
        None => None,
    };
    if if_none_match.is_some_and(|value| value != "*") {
        return Some((StatusCode::BAD_REQUEST, "InvalidArgument"));
    }
    let existing = bucket.objects.get(key).map(|entry| &entry.object);
    let failed = if_none_match.is_some() && existing.is_some()
        || if_match.is_some_and(|expected| {
            existing.is_none_or(|object| object.etag.trim_matches('"') != expected)
        });
    failed.then_some((StatusCode::PRECONDITION_FAILED, "PreconditionFailed"))
}

/// Applies `If-Match` and `If-None-Match` to a read and returns the reply that replaces it.
fn read_precondition(
    object: &Object,
    method: &Method,
    key: &str,
    headers: &hyper::HeaderMap,
) -> Option<Reply> {
    if let Some(expected) = headers.get(IF_MATCH) {
        if !etag_matches(expected, &object.etag) {
            return Some(if *method == Method::HEAD {
                empty(StatusCode::PRECONDITION_FAILED)
            } else {
                error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", key)
            });
        }
    } else if let Some(expected) = headers.get(IF_NONE_MATCH) {
        if etag_matches(expected, &object.etag) {
            return Some(
                hyper::Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header(ETAG, object.etag.as_str())
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            );
        }
    }
    None
}

/// Compares a read precondition with an ETag: `*`, or any listed tag, weak or strong.
fn etag_matches(expected: &HeaderValue, etag: &str) -> bool {
    let Ok(expected) = expected.to_str() else {
        return false;
    };
    expected
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate.trim_start_matches("W/") == etag)
}

/// Decodes an `aws-chunked` body to its `x-amz-decoded-content-length` bytes and drops trailers.
pub(crate) fn decode_aws_chunked(headers: &hyper::HeaderMap, payload: Bytes) -> Result<Bytes, ()> {
    let encoded = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|part| part.trim() == "aws-chunked"));
    if !encoded {
        return Ok(payload);
    }
    let expected: usize = headers
        .get("x-amz-decoded-content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(())?;
    if expected > payload.len() {
        return Err(());
    }
    let line_end = |from: usize| {
        payload[from..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|offset| from + offset)
            .ok_or(())
    };
    let mut decoded = Vec::with_capacity(expected);
    let mut position = 0;
    loop {
        let end = line_end(position)?;
        let line = std::str::from_utf8(&payload[position..end]).map_err(|_| ())?;
        let size = usize::from_str_radix(line.split(';').next().ok_or(())?, 16).map_err(|_| ())?;
        position = end + 2;
        if size == 0 {
            loop {
                let end = line_end(position)?;
                let trailer = &payload[position..end];
                position = end + 2;
                if trailer.is_empty() {
                    break;
                }
                if !trailer.contains(&b':') {
                    return Err(());
                }
            }
            break;
        }
        let data_end = position.checked_add(size).ok_or(())?;
        let terminator_end = data_end.checked_add(2).ok_or(())?;
        if terminator_end > payload.len() || &payload[data_end..terminator_end] != b"\r\n" {
            return Err(());
        }
        decoded.extend_from_slice(&payload[position..data_end]);
        if decoded.len() > expected {
            return Err(());
        }
        position = terminator_end;
    }
    if decoded.len() != expected || position != payload.len() {
        return Err(());
    }
    Ok(Bytes::from(decoded))
}

pub(crate) fn render_hot(snapshot: HotRead, parts: &hyper::http::request::Parts) -> Reply {
    match snapshot {
        HotRead::Found(object) => read_object(&object, &parts.method, &parts.headers, &[]),
        HotRead::MissingBucket(bucket) => {
            if parts.method == Method::HEAD {
                empty(StatusCode::NOT_FOUND)
            } else {
                error(StatusCode::NOT_FOUND, "NoSuchBucket", &bucket)
            }
        }
        HotRead::MissingKey(key) => {
            if parts.method == Method::HEAD {
                empty(StatusCode::NOT_FOUND)
            } else {
                error(StatusCode::NOT_FOUND, "NoSuchKey", &key)
            }
        }
    }
}

pub(crate) fn route(
    state: &mut State,
    parts: &hyper::http::request::Parts,
    payload: Bytes,
    max_stored_bytes: usize,
) -> Reply {
    if parts.method == Method::POST && parts.uri.path() == "/__reset" {
        reset(state);
        return empty(StatusCode::NO_CONTENT);
    }
    let path = percent_decode_str(parts.uri.path()).decode_utf8_lossy();
    let mut path_parts = path.trim_start_matches('/').splitn(2, '/');
    let bucket = path_parts.next().unwrap_or("");
    let key = path_parts.next().filter(|key| !key.is_empty());
    let mut query: Query = form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect();
    query.retain(|name, _| !ignored_parameter(name));

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
        max_stored_bytes,
    )
}

/// Removes every object and multipart upload and keeps the buckets.
fn reset(state: &mut State) {
    for bucket in state.buckets.values_mut() {
        for (_, entry) in bucket.objects.drain() {
            entry.slot.store(None);
        }
    }
    state.uploads.clear();
    state.stored_bytes = 0;
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

#[allow(clippy::too_many_arguments)]
fn object_request(
    state: &mut State,
    method: &Method,
    bucket: &str,
    key: &str,
    query: &Query,
    headers: &hyper::HeaderMap,
    payload: Bytes,
    max_stored_bytes: usize,
) -> Reply {
    if !state.buckets.contains_key(bucket) {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket", bucket);
    }
    if query.contains_key("uploads") && *method == Method::POST {
        return multipart::create_upload(state, bucket, key, headers);
    }
    if let Some(id) = query.get("uploadId") {
        return multipart::upload_request(
            state,
            method,
            bucket,
            key,
            id,
            query,
            payload,
            max_stored_bytes,
        );
    }
    let reads = *method == Method::GET || *method == Method::HEAD;
    let mut overrides = Vec::new();
    for (name, value) in query {
        let Some(header) = reads.then(|| override_header(name)).flatten() else {
            return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", key);
        };
        let Ok(value) = HeaderValue::from_str(value) else {
            return error(StatusCode::BAD_REQUEST, "InvalidArgument", name);
        };
        overrides.push((header, value));
    }
    if *method == Method::PUT {
        if !headers.contains_key("x-amz-copy-source") {
            let object = make_object(payload, headers);
            return write_object(
                state,
                bucket,
                key.to_owned(),
                object,
                headers,
                max_stored_bytes,
            );
        }
        if let Some((status, code)) = put_precondition(&state.buckets[bucket], key, headers) {
            return error(status, code, key);
        }
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
                .map(|entry| Arc::clone(&entry.object))
            else {
                return error(StatusCode::NOT_FOUND, "NoSuchKey", source_key);
            };
            let mut copied = Object {
                modified: SystemTime::now(),
                ..(*original).clone()
            };
            if let Some(encryption) = header_string(headers, "x-amz-server-side-encryption") {
                copied.encryption = Some(encryption);
            }
            if store_object_bounded(
                state,
                bucket,
                key.to_owned(),
                copied.clone(),
                max_stored_bytes,
            )
            .is_err()
            {
                return error(StatusCode::INSUFFICIENT_STORAGE, "StorageFull", key);
            }
            return xml(StatusCode::OK, format!(
                "<CopyObjectResult><LastModified>{}</LastModified><ETag>{}</ETag></CopyObjectResult>",
                timestamp(copied.modified), copied.etag,
            ));
        }
    }
    if *method == Method::DELETE {
        if let Some(size) = remove_object(state.buckets.get_mut(bucket).unwrap(), key) {
            state.stored_bytes -= size;
        }
        return empty(StatusCode::NO_CONTENT);
    }
    if *method == Method::GET || *method == Method::HEAD {
        let Some(object) = state
            .buckets
            .get(bucket)
            .and_then(|b| b.objects.get(key))
            .map(|entry| Arc::clone(&entry.object))
        else {
            return if *method == Method::HEAD {
                empty(StatusCode::NOT_FOUND)
            } else {
                error(StatusCode::NOT_FOUND, "NoSuchKey", key)
            };
        };
        return read_precondition(&object, method, key, headers)
            .unwrap_or_else(|| read_object(&object, method, headers, &overrides));
    }
    error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", key)
}

/// Maps an S3 response override parameter to the header it sets.
fn override_header(parameter: &str) -> Option<HeaderName> {
    Some(match parameter {
        "response-cache-control" => CACHE_CONTROL,
        "response-content-disposition" => CONTENT_DISPOSITION,
        "response-content-encoding" => CONTENT_ENCODING,
        "response-content-language" => CONTENT_LANGUAGE,
        "response-content-type" => CONTENT_TYPE,
        "response-expires" => EXPIRES,
        _ => return None,
    })
}

pub(crate) fn store_object(bucket: &mut Bucket, key: String, object: Object) {
    let object = Arc::new(object);
    match bucket.objects.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            entry.get_mut().slot.store(Some(Arc::clone(&object)));
            entry.get_mut().object = object;
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(ObjectEntry {
                object: Arc::clone(&object),
                slot: Arc::new(ArcSwapOption::new(Some(object))),
            });
        }
    }
}

pub(crate) fn store_object_bounded(
    state: &mut State,
    bucket: &str,
    key: String,
    object: Object,
    max_stored_bytes: usize,
) -> Result<(), ()> {
    let old_size = state.buckets[bucket]
        .objects
        .get(&key)
        .map_or(0, |entry| entry.object.bytes.len());
    let new_size = state
        .stored_bytes
        .checked_sub(old_size)
        .and_then(|size| size.checked_add(object.bytes.len()))
        .ok_or(())?;
    if new_size > max_stored_bytes {
        return Err(());
    }
    store_object(state.buckets.get_mut(bucket).unwrap(), key, object);
    state.stored_bytes = new_size;
    Ok(())
}

pub(crate) fn remove_object(bucket: &mut Bucket, key: &str) -> Option<usize> {
    if let Some(entry) = bucket.objects.remove(key) {
        let size = entry.object.bytes.len();
        entry.slot.store(None);
        Some(size)
    } else {
        None
    }
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
        content_type: header_string(headers, CONTENT_TYPE.as_str()),
        metadata,
        encryption: header_string(headers, "x-amz-server-side-encryption"),
    }
}

pub(crate) fn header_string(headers: &hyper::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn read_object(
    object: &Object,
    method: &Method,
    headers: &hyper::HeaderMap,
    overrides: &[(HeaderName, HeaderValue)],
) -> Reply {
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
    // S3 applies response overrides to complete responses only.
    let overrides = if status == StatusCode::OK {
        overrides
    } else {
        &[]
    };
    if let Some(content_type) = &object.content_type {
        if !overrides.iter().any(|(header, _)| header == CONTENT_TYPE) {
            response = response.header(CONTENT_TYPE, content_type);
        }
    }
    for (header, value) in overrides {
        response = response.header(header, value);
    }
    if let Some(encryption) = &object.encryption {
        response = response.header("x-amz-server-side-encryption", encryption);
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
    let mut entries: BTreeMap<String, (String, Option<Arc<Object>>)> = BTreeMap::new();
    for (key, entry) in &bucket_state.objects {
        if !key.starts_with(prefix) || (!after.is_empty() && key.as_str() <= after) {
            continue;
        }
        if !delimiter.is_empty() {
            if let Some(index) = key[prefix.len()..].find(delimiter) {
                let common_prefix = &key[..prefix.len() + index + delimiter.len()];
                entries
                    .entry(common_prefix.to_owned())
                    .and_modify(|(last, _)| {
                        if key > last {
                            *last = key.clone();
                        }
                    })
                    .or_insert_with(|| (key.clone(), None));
                continue;
            }
        }
        entries.insert(key.clone(), (key.clone(), Some(Arc::clone(&entry.object))));
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

    #[test]
    fn aws_chunked_payload_excludes_metadata_and_rejects_wrong_length() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "aws-chunked".parse().unwrap());
        headers.insert("x-amz-decoded-content-length", "5".parse().unwrap());
        let encoded = Bytes::from_static(
            b"2;chunk-signature=a\r\nhe\r\n3;chunk-signature=b\r\nllo\r\n0;chunk-signature=c\r\nx-amz-checksum-crc32:abc=\r\n\r\n",
        );
        assert_eq!(
            decode_aws_chunked(&headers, encoded.clone()),
            Ok(Bytes::from_static(b"hello"))
        );
        headers.insert("x-amz-decoded-content-length", "4".parse().unwrap());
        assert_eq!(decode_aws_chunked(&headers, encoded), Err(()));
    }
}
