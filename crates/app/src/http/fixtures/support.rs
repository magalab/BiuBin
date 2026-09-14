use crate::http::response::{is_sensitive_header, response_with_request_id};
use crate::state::MAX_HEADER_VALUE_LEN;
use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use std::collections::BTreeMap;
use url::form_urlencoded;

pub(super) fn redacted_headers(headers: &HeaderMap) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in headers {
        values
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(if is_sensitive_header(name.as_str()) {
                "[REDACTED]".to_owned()
            } else {
                header_preview(value)
            });
    }
    values
}

pub(super) fn query_pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .map(|(name, value)| (name.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn query_values(uri: &Uri) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in query_pairs(uri) {
        values.entry(name).or_insert_with(Vec::new).push(value);
    }
    values
}

pub(super) type StructuredBody = (
    serde_json::Value,
    BTreeMap<String, Vec<String>>,
    BTreeMap<String, Vec<String>>,
);

pub(super) fn parse_structured_body(headers: &HeaderMap, body: &Bytes) -> StructuredBody {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        return (
            serde_json::from_slice(body).unwrap_or(serde_json::Value::Null),
            BTreeMap::new(),
            BTreeMap::new(),
        );
    }
    if content_type.starts_with("application/x-www-form-urlencoded") {
        return (serde_json::Value::Null, form_values(body), BTreeMap::new());
    }
    if content_type.starts_with("multipart/form-data")
        && let Some(boundary) = multipart_boundary(content_type)
    {
        let (form, files) = multipart_values(body, &boundary);
        return (serde_json::Value::Null, form, files);
    }
    (serde_json::Value::Null, BTreeMap::new(), BTreeMap::new())
}

pub(super) fn form_values(body: &[u8]) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in form_urlencoded::parse(body) {
        values
            .entry(name.into_owned())
            .or_insert_with(Vec::new)
            .push(value.into_owned());
    }
    values
}

pub(super) fn multipart_boundary(content_type: &str) -> Option<String> {
    content_type
        .split(';')
        .skip(1)
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"').to_owned())
        .filter(|boundary| !boundary.is_empty())
}

pub(super) fn multipart_values(
    body: &[u8],
    boundary: &str,
) -> (BTreeMap<String, Vec<String>>, BTreeMap<String, Vec<String>>) {
    let marker = format!("--{boundary}").into_bytes();
    let mut form = BTreeMap::new();
    let mut files = BTreeMap::new();
    let mut cursor = 0;
    while let Some(relative_start) = find_bytes(&body[cursor..], &marker) {
        let marker_start = cursor + relative_start;
        let mut part_start = marker_start + marker.len();
        if body.get(part_start..part_start + 2) == Some(b"--") {
            break;
        }
        if body.get(part_start..part_start + 2) == Some(b"\r\n") {
            part_start += 2;
        }
        let Some(relative_end) = find_bytes(&body[part_start..], &marker) else {
            break;
        };
        let mut part = &body[part_start..part_start + relative_end];
        if part.ends_with(b"\r\n") {
            part = &part[..part.len() - 2];
        }
        if let Some(separator) = find_bytes(part, b"\r\n\r\n") {
            let header_bytes = &part[..separator];
            let value_bytes = &part[separator + 4..];
            let part_headers = String::from_utf8_lossy(header_bytes);
            if let Some(disposition) = part_headers
                .lines()
                .find_map(|line| line.strip_prefix("Content-Disposition:"))
            {
                let name = disposition_parameter(disposition, "name");
                let filename = disposition_parameter(disposition, "filename");
                if let Some(name) = name {
                    let value = String::from_utf8_lossy(value_bytes).into_owned();
                    if filename.is_some() {
                        files.entry(name).or_insert_with(Vec::new).push(value);
                    } else {
                        form.entry(name).or_insert_with(Vec::new).push(value);
                    }
                }
            }
        }
        cursor = part_start + relative_end;
    }
    (form, files)
}

pub(super) fn disposition_parameter(disposition: &str, parameter: &str) -> Option<String> {
    disposition.split(';').skip(1).find_map(|piece| {
        let (name, value) = piece.trim().split_once('=')?;
        (name.trim() == parameter).then(|| value.trim().trim_matches('"').to_owned())
    })
}

pub(super) fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub(super) fn cookie_values(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for header_value in headers.get_all(header::COOKIE) {
        let Ok(header_value) = header_value.to_str() else {
            continue;
        };
        for cookie in header_value.split(';') {
            let Some((name, value)) = cookie.trim().split_once('=') else {
                continue;
            };
            values.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    values
}

pub(super) fn valid_cookie_piece(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte > 0x20 && byte != b';' && byte != b'=')
        && !value.contains('\r')
        && !value.contains('\n')
}

pub(super) fn forbidden_response_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "content-length"
            | "content-type"
            | "host"
            | "set-cookie"
            | "transfer-encoding"
            | "x-request-id"
    )
}

pub(super) fn deterministic_bytes(count: usize) -> Vec<u8> {
    (0..count).map(|index| (index % 251) as u8).collect()
}

pub(super) fn deterministic_bytes_from(offset: usize, count: usize) -> Vec<u8> {
    (offset..offset + count)
        .map(|index| (index % 251) as u8)
        .collect()
}

pub(super) fn binary_response(
    status: StatusCode,
    request_id: String,
    content_type: &str,
    body: &[u8],
) -> Response {
    let mut response = (status, body.to_vec()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type).expect("static content type is valid"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length is valid"),
    );
    response_with_request_id(response, request_id)
}

pub(super) fn ranged_response(
    request_headers: &HeaderMap,
    request_id: String,
    content_type: &str,
    body: &[u8],
    etag: Option<&str>,
) -> Response {
    let range = request_headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let parsed_range = match range {
        None => Ok(None),
        Some(value) => parse_range(value, body.len()),
    };
    let Ok(parsed_range) = parsed_range else {
        let mut response = binary_response(
            StatusCode::RANGE_NOT_SATISFIABLE,
            request_id,
            content_type,
            &[],
        );
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes */{}", body.len()))
                .expect("content range is valid"),
        );
        response
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        if let Some(etag) = etag {
            response.headers_mut().insert(
                header::ETAG,
                HeaderValue::from_str(etag).expect("static etag is valid"),
            );
        }
        return response;
    };
    let (status, selected) = match parsed_range {
        None => (StatusCode::OK, body.to_vec()),
        Some((start, end)) => (StatusCode::PARTIAL_CONTENT, body[start..=end].to_vec()),
    };
    let mut response = binary_response(status, request_id, content_type, &selected);
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some((start, end)) = parsed_range {
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{}", body.len()))
                .expect("content range is valid"),
        );
    }
    if let Some(etag) = etag {
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(etag).expect("static etag is valid"),
        );
    }
    response
}

pub(super) fn parse_range(value: &str, length: usize) -> Result<Option<(usize, usize)>, ()> {
    let Some(value) = value.strip_prefix("bytes=") else {
        return Err(());
    };
    if value.contains(',') || length == 0 {
        return Err(());
    }
    let Some((start, end)) = value.split_once('-') else {
        return Err(());
    };
    if start.is_empty() {
        let suffix = end.parse::<usize>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let start = length.saturating_sub(suffix);
        return Ok(Some((start, length - 1)));
    }
    let start = start.parse::<usize>().map_err(|_| ())?;
    if start >= length {
        return Err(());
    }
    let end = if end.is_empty() {
        length - 1
    } else {
        end.parse::<usize>().map_err(|_| ())?.min(length - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some((start, end)))
}

pub(super) fn strong_etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag
    })
}

pub(super) fn weak_etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
    })
}

pub(super) fn deterministic_unit(seed: u64) -> f64 {
    let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    (value >> 11) as f64 / ((1_u64 << 53) as f64)
}
pub(super) fn header_preview(value: &HeaderValue) -> String {
    let value = value.to_str().unwrap_or("[INVALID_UTF8]");
    if value.len() <= MAX_HEADER_VALUE_LEN {
        value.to_owned()
    } else {
        let preview: String = value.chars().take(MAX_HEADER_VALUE_LEN).collect();
        format!("{preview}…")
    }
}
