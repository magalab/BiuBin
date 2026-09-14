use crate::http::{
    middleware::request_id,
    response::{is_sensitive_header, json_response, response_with_request_id, text_response},
};
use crate::state::{AppState, MAX_HEADER_VALUE_LEN, STREAM_BYTES_CHUNK_SIZE};
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Extension, OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use url::{Url, form_urlencoded};

const MAX_REDIRECT_URL_LEN: usize = 2048;
const CACHE_ETAG: &str = "\"biubin-cache-v1\"";
const CACHE_LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

pub(crate) async fn http_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(code) = code.parse::<u16>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid status code"}),
        );
    };
    let Ok(status) = StatusCode::from_u16(code) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid status code"}),
        );
    };
    state
        .events
        .push("http", "response", format!("status {code}"));
    json_response(status, request_id, json!({"status": code}))
}

pub(crate) async fn http_delay(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(seconds): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(seconds) = seconds.parse::<u64>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid delay"}),
        );
    };
    let seconds = seconds.min(30);
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    state
        .events
        .push("http", "response", format!("delay {seconds}s"));
    json_response(
        StatusCode::OK,
        request_id,
        json!({"delay_seconds": seconds}),
    )
}

pub(crate) async fn http_redirect(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(count): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(count) = count.parse::<u16>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid redirect count"}),
        );
    };
    let count = count.min(20);
    if count == 0 {
        return json_response(StatusCode::OK, request_id, json!({"redirects": 0}));
    }
    let location = format!("/redirect/{}", count - 1);
    let mut response = StatusCode::FOUND.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) async fn http_bytes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(count): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(count) = count.parse::<usize>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid byte count"}),
        );
    };
    if count > state.config.max_bytes_response {
        return json_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            request_id,
            json!({"error": "byte count too large"}),
        );
    }
    let body: Vec<u8> = deterministic_bytes(count);
    ranged_response(
        &headers,
        request_id,
        "application/octet-stream",
        &body,
        None,
    )
}

pub(crate) async fn http_stream_bytes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(count): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(count) = count.parse::<usize>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid byte count"}),
        );
    };
    if count > state.config.max_bytes_response {
        return json_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            request_id,
            json!({"error": "byte count too large"}),
        );
    }
    let stream = async_stream::stream! {
        let mut offset = 0;
        while offset < count {
            let end = (offset + STREAM_BYTES_CHUNK_SIZE).min(count);
            let chunk = (offset..end).map(|index| (index % 251) as u8).collect::<Vec<_>>();
            yield Ok::<Bytes, Infallible>(Bytes::from(chunk));
            offset = end;
        }
    };
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) async fn http_gzip(State(state): State<AppState>, headers: HeaderMap) -> Response {
    compressed_http_response(state, headers, "gzip")
}

pub(crate) async fn http_deflate(State(state): State<AppState>, headers: HeaderMap) -> Response {
    compressed_http_response(state, headers, "deflate")
}

fn compressed_http_response(state: AppState, headers: HeaderMap, encoding: &str) -> Response {
    let request_id = request_id(&headers, &state);
    let source = br#"{"message":"biubin compressed response"}"#;
    let compressed = if encoding == "gzip" {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(source).and_then(|()| encoder.finish())
    } else {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(source).and_then(|()| encoder.finish())
    };
    let Ok(compressed) = compressed else {
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            json!({"error": "compression failed"}),
        );
    };
    let mut response = compressed.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        header::CONTENT_ENCODING,
        HeaderValue::from_str(encoding).expect("static encoding is valid"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) async fn http_basic_auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((user, password)): Path<(String, String)>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
        .is_some_and(|value| value == format!("{user}:{password}"));
    if !valid {
        let mut response = json_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            json!({"authenticated": false}),
        );
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=biubin"),
        );
        return response;
    }
    json_response(
        StatusCode::OK,
        request_id,
        json!({"authenticated": true, "user": user}),
    )
}

/// The root request-echo contract uses an empty `path` for `/anything` and `/anything/`.
pub(crate) async fn http_anything_root(
    State(state): State<AppState>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    client: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    http_anything_impl(
        state,
        method,
        uri,
        headers,
        client.map(|Extension(ConnectInfo(address))| address),
        body,
        String::new(),
    )
    .await
}

pub(crate) async fn http_anything_path(
    State(state): State<AppState>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    Path(path): Path<String>,
    client: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    http_anything_impl(
        state,
        method,
        uri,
        headers,
        client.map(|Extension(ConnectInfo(address))| address),
        body,
        path,
    )
    .await
}

async fn http_anything_impl(
    state: AppState,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    client: Option<SocketAddr>,
    body: Bytes,
    path: String,
) -> Response {
    let request_id = request_id(&headers, &state);
    let mut header_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in &headers {
        header_values.entry(name.to_string()).or_default().push(
            if is_sensitive_header(name.as_str()) {
                "[REDACTED]".to_owned()
            } else {
                header_preview(value)
            },
        );
    }
    let body_preview = String::from_utf8_lossy(&body[..body.len().min(4096)]).into_owned();
    let args = query_values(&uri.0);
    let (json_body, form_values, file_values) = parse_structured_body(&headers, &body);
    state.events.push(
        "http",
        "request_received",
        format!("{} {}", method, uri.path()),
    );
    json_response(
        StatusCode::OK,
        request_id.clone(),
        json!({
            "request_id": request_id,
            "method": method.as_str(),
            "path": path,
            "uri": uri.0.to_string(),
            "query": uri.0.query().unwrap_or_default(),
            "args": args,
            "headers": header_values,
            "json": json_body,
            "form": form_values,
            "files": file_values,
            "data": body_preview,
            "body": body_preview,
            "body_length": body.len(),
            "client": client.map(|address| json!({"ip": address.ip().to_string(), "port": address.port()})),
        }),
    )
}

pub(crate) async fn http_headers(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let values = redacted_headers(&headers);
    json_response(StatusCode::OK, request_id, json!({"headers": values}))
}

pub(crate) async fn http_user_agent(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let value = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok());
    json_response(StatusCode::OK, request_id, json!({"user_agent": value}))
}

pub(crate) async fn http_ip(
    State(state): State<AppState>,
    headers: HeaderMap,
    client: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let origin = client
        .map(|Extension(ConnectInfo(address))| address.ip().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_owned());
    json_response(StatusCode::OK, request_id, json!({"origin": origin}))
}

pub(crate) async fn http_image_default(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    image_response(state, headers, "png")
}

pub(crate) async fn http_image_format(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(format): Path<String>,
) -> Response {
    image_response(state, headers, &format)
}

fn image_response(state: AppState, headers: HeaderMap, format: &str) -> Response {
    let request_id = request_id(&headers, &state);
    let Some((content_type, body, etag)) = image_asset(format) else {
        return json_response(
            StatusCode::NOT_FOUND,
            request_id,
            json!({"error": "unsupported image format"}),
        );
    };
    let mut response = ranged_response(&headers, request_id, content_type, body, Some(etag));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=3600"),
    );
    response
}

pub(crate) async fn http_video_default(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    video_response(state, headers, "mp4")
}

pub(crate) async fn http_video_format(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(format): Path<String>,
) -> Response {
    video_response(state, headers, &format)
}

pub(crate) async fn http_audio_default(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    audio_response(state, headers, "wav")
}

pub(crate) async fn http_audio_format(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(format): Path<String>,
) -> Response {
    audio_response(state, headers, &format)
}

fn audio_response(state: AppState, headers: HeaderMap, format: &str) -> Response {
    let request_id = request_id(&headers, &state);
    let Some((content_type, body, etag)) = audio_asset(format) else {
        return json_response(
            StatusCode::NOT_FOUND,
            request_id,
            json!({"error": "unsupported audio format"}),
        );
    };
    let mut response = ranged_response(&headers, request_id, content_type, body, Some(etag));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=3600"),
    );
    response
}

fn video_response(state: AppState, headers: HeaderMap, format: &str) -> Response {
    let request_id = request_id(&headers, &state);
    let Some((content_type, body, etag)) = video_asset(format) else {
        return json_response(
            StatusCode::NOT_FOUND,
            request_id,
            json!({"error": "unsupported video format"}),
        );
    };
    let mut response = ranged_response(&headers, request_id, content_type, body, Some(etag));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=3600"),
    );
    response
}

pub(crate) async fn http_response_headers(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: OriginalUri,
) -> Response {
    let current_request_id = request_id(&headers, &state);
    let mut response = json_response(
        StatusCode::OK,
        current_request_id.clone(),
        json!({"headers": query_values(&uri.0)}),
    );
    for (name, value) in query_pairs(&uri.0) {
        if forbidden_response_header(&name) {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": format!("header {name:?} cannot be set")}),
            );
        }
        let Ok(name) = header::HeaderName::from_bytes(name.as_bytes()) else {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": "invalid response header name"}),
            );
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": "invalid response header value"}),
            );
        };
        response.headers_mut().append(name, value);
    }
    response
}

pub(crate) async fn http_cookies(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    json_response(
        StatusCode::OK,
        request_id,
        json!({"cookies": cookie_values(&headers)}),
    )
}

pub(crate) async fn http_cookies_set(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, value)): Path<(String, String)>,
) -> Response {
    let request_id = request_id(&headers, &state);
    if !valid_cookie_piece(&name) || !valid_cookie_piece(&value) {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cookie name or value"}),
        );
    }
    cookie_redirect(request_id, &name, &format!("{name}={value}; Path=/"))
}

pub(crate) async fn http_cookies_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    if !valid_cookie_piece(&name) {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cookie name"}),
        );
    }
    cookie_redirect(request_id, &name, &format!("{name}=; Max-Age=0; Path=/"))
}

fn cookie_redirect(request_id: String, _name: &str, set_cookie: &str) -> Response {
    let mut response = StatusCode::FOUND.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("/cookies"));
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(set_cookie).expect("validated cookie is a valid header"),
    );
    response_with_request_id(response, request_id)
}

pub(crate) async fn http_cache(State(state): State<AppState>, headers: HeaderMap) -> Response {
    cache_response(state, headers, None)
}

pub(crate) async fn http_cache_with_max_age(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(seconds): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(seconds) = seconds.parse::<u64>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cache max-age"}),
        );
    };
    cache_response(state, headers, Some(seconds.min(86_400)))
}

fn cache_response(state: AppState, headers: HeaderMap, max_age: Option<u64>) -> Response {
    let request_id = request_id(&headers, &state);
    let not_modified = if let Some(if_none_match) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        // RFC 9110 gives If-None-Match precedence over If-Modified-Since.
        weak_etag_matches(if_none_match, CACHE_ETAG)
    } else {
        headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .zip(httpdate::parse_http_date(CACHE_LAST_MODIFIED).ok())
            .is_some_and(|(if_modified_since, last_modified)| last_modified <= if_modified_since)
    };
    let mut response = if not_modified {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        json_response(StatusCode::OK, request_id.clone(), json!({"cache": true}))
    };
    response
        .headers_mut()
        .insert(header::ETAG, HeaderValue::from_static(CACHE_ETAG));
    response.headers_mut().insert(
        header::LAST_MODIFIED,
        HeaderValue::from_static(CACHE_LAST_MODIFIED),
    );
    if let Some(max_age) = max_age {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_str(&format!("public, max-age={max_age}"))
                .expect("numeric cache control is valid"),
        );
    }
    if !response.headers().contains_key("x-request-id") {
        response
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    }
    response
}

pub(crate) async fn http_etag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(value): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let etag = format!("\"{}\"", value.replace('"', ""));
    if let Some(if_match) = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        && !strong_etag_matches(if_match, &etag)
    {
        let mut response = StatusCode::PRECONDITION_FAILED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
        );
        return response_with_request_id(response, request_id);
    }
    if let Some(if_none_match) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        && weak_etag_matches(if_none_match, &etag)
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
        );
        return response_with_request_id(response, request_id);
    }
    let mut response = json_response(StatusCode::OK, request_id, json!({"etag": value}));
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
    );
    response
}

#[derive(Debug, Deserialize)]
pub(crate) struct RedirectToQuery {
    url: Option<String>,
    status_code: Option<u16>,
}

pub(crate) async fn http_redirect_to(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RedirectToQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Some(url) = query.url.filter(|url| !url.is_empty()) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "url query parameter is required"}),
        );
    };
    let status = match query.status_code.unwrap_or(302) {
        301 => StatusCode::MOVED_PERMANENTLY,
        302 => StatusCode::FOUND,
        303 => StatusCode::SEE_OTHER,
        307 => StatusCode::TEMPORARY_REDIRECT,
        308 => StatusCode::PERMANENT_REDIRECT,
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                request_id,
                json!({"error": "status_code must be 301, 302, 303, 307 or 308"}),
            );
        }
    };
    if let Some(error) = validate_redirect_target(&url, state.config.http_allow_external_redirects)
    {
        return json_response(StatusCode::BAD_REQUEST, request_id, json!({"error": error}));
    }
    redirect_response(status, request_id, &url)
}

fn validate_redirect_target(url: &str, allow_external: bool) -> Option<&'static str> {
    if url.len() > MAX_REDIRECT_URL_LEN {
        return Some("redirect URL is too long");
    }
    if url.contains('\r') || url.contains('\n') || url.contains('\\') {
        return Some("url contains invalid characters");
    }

    if let Ok(parsed) = Url::parse(url) {
        if !allow_external {
            return Some("absolute external redirects are disabled");
        }
        if !matches!(parsed.scheme(), "http" | "https") {
            return Some("redirect URL scheme must be http or https");
        }
        let Some(host) = parsed.host_str() else {
            return Some("redirect URL must include a host");
        };
        if host.eq_ignore_ascii_case("localhost")
            || host.to_ascii_lowercase().ends_with(".localhost")
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .is_ok_and(is_restricted_ip)
        {
            return Some("redirect URL points to a local address");
        }
        return None;
    }

    let base = Url::parse("http://biubin.invalid").expect("static redirect base is valid");
    let Ok(resolved) = base.join(url) else {
        return Some("invalid redirect URL");
    };
    if resolved.host_str() != base.host_str() {
        return Some("external redirects require an absolute http(s) URL");
    }
    None
}

fn is_restricted_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_loopback()
                || address.is_private()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_multicast()
        }
        IpAddr::V6(address) => {
            if address.is_loopback()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                || address.is_unspecified()
                || address.is_multicast()
            {
                return true;
            }
            address
                .to_ipv4_mapped()
                .or_else(|| address.to_ipv4())
                .is_some_and(|address| is_restricted_ip(IpAddr::V4(address)))
        }
    }
}

/*
    The target has been validated above, but keep HeaderValue validation as the
    final guard before putting user input into Location.
*/
fn redirect_response(status: StatusCode, request_id: String, url: &str) -> Response {
    let Ok(location) = HeaderValue::from_str(url) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid redirect URL"}),
        );
    };
    let mut response = status.into_response();
    response.headers_mut().insert(header::LOCATION, location);
    response_with_request_id(response, request_id)
}

pub(crate) async fn http_json(State(state): State<AppState>, headers: HeaderMap) -> Response {
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        json!({"message": "biubin json fixture", "ok": true}),
    )
}

pub(crate) async fn http_html(State(state): State<AppState>, headers: HeaderMap) -> Response {
    text_response(
        StatusCode::OK,
        request_id(&headers, &state),
        "<!doctype html><html><body><h1>biubin html fixture</h1></body></html>",
        "text/html; charset=utf-8",
    )
}

pub(crate) async fn http_xml(State(state): State<AppState>, headers: HeaderMap) -> Response {
    text_response(
        StatusCode::OK,
        request_id(&headers, &state),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><biubin><ok>true</ok></biubin>",
        "application/xml; charset=utf-8",
    )
}

pub(crate) async fn http_utf8(State(state): State<AppState>, headers: HeaderMap) -> Response {
    text_response(
        StatusCode::OK,
        request_id(&headers, &state),
        "BiuBin UTF-8 fixture · 你好，世界 · café",
        "text/plain; charset=utf-8",
    )
}

pub(crate) async fn http_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(count): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(count) = count.parse::<usize>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid byte count"}),
        );
    };
    if count > state.config.max_bytes_response {
        return json_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            request_id,
            json!({"error": "byte count too large"}),
        );
    }
    let body = deterministic_bytes(count);
    ranged_response(
        &headers,
        request_id,
        "application/octet-stream",
        &body,
        Some("\"biubin-range-v1\""),
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct DripQuery {
    numbytes: Option<usize>,
    duration: Option<f64>,
    delay: Option<f64>,
    chunk_size: Option<usize>,
    code: Option<u16>,
}

pub(crate) async fn http_drip(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DripQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let count = query.numbytes.unwrap_or(10);
    let duration = query.duration.unwrap_or(0.0);
    let delay = query.delay.unwrap_or(0.0);
    let requested_chunk_size = query
        .chunk_size
        .unwrap_or(1)
        .clamp(1, STREAM_BYTES_CHUNK_SIZE);
    if count > state.config.max_bytes_response
        || !duration.is_finite()
        || !delay.is_finite()
        || duration < 0.0
        || delay < 0.0
    {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid drip parameters"}),
        );
    }
    let status = match StatusCode::from_u16(query.code.unwrap_or(200)) {
        Ok(status) => status,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                request_id,
                json!({"error": "invalid drip status code"}),
            );
        }
    };
    let delay = delay.min(30.0);
    let duration = duration.min(30.0);
    // A zero-duration drip is an immediate response. Coalesce it into one
    // chunk so a small requested chunk_size cannot cause millions of tiny
    // allocations when the caller asks for a large body.
    let chunk_size = if duration == 0.0 {
        count.max(1)
    } else {
        requested_chunk_size
    };
    let chunks = count.div_ceil(chunk_size).max(1);
    let interval = if duration == 0.0 {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_secs_f64(duration / chunks as f64)
    };
    let stream = async_stream::stream! {
        if delay > 0.0 {
            tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
        }
        let mut offset = 0;
        while offset < count {
            let end = (offset + chunk_size).min(count);
            yield Ok::<Bytes, Infallible>(Bytes::from(deterministic_bytes_from(offset, end - offset)));
            offset = end;
            if offset < count && !interval.is_zero() {
                tokio::time::sleep(interval).await;
            }
        }
    };
    let mut response = (status, Body::from_stream(stream)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

#[derive(Debug, Deserialize)]
pub(crate) struct UnstableQuery {
    failure_rate: Option<f64>,
    seed: Option<u64>,
}

pub(crate) async fn http_unstable(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<UnstableQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let rate = query.failure_rate.unwrap_or(0.5);
    let seed = query.seed.unwrap_or(1);
    if !rate.is_finite() || !(0.0..=1.0).contains(&rate) {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "failure_rate must be between 0 and 1"}),
        );
    }
    let failed = deterministic_unit(seed) < rate;
    let status = if failed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    json_response(
        status,
        request_id,
        json!({"unstable": true, "failed": failed, "failure_rate": rate, "seed": seed}),
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct BearerQuery {
    token: Option<String>,
}

pub(crate) async fn http_bearer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BearerQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let expected = query.token.unwrap_or_else(|| "biubin".to_owned());
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if supplied != Some(expected.as_str()) {
        let mut response = json_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            json!({"authenticated": false}),
        );
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    json_response(
        StatusCode::OK,
        request_id,
        json!({"authenticated": true, "token": expected}),
    )
}

fn redacted_headers(headers: &HeaderMap) -> BTreeMap<String, Vec<String>> {
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

fn query_pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .map(|(name, value)| (name.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn query_values(uri: &Uri) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in query_pairs(uri) {
        values.entry(name).or_insert_with(Vec::new).push(value);
    }
    values
}

type StructuredBody = (
    serde_json::Value,
    BTreeMap<String, Vec<String>>,
    BTreeMap<String, Vec<String>>,
);

fn parse_structured_body(headers: &HeaderMap, body: &Bytes) -> StructuredBody {
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

fn form_values(body: &[u8]) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in form_urlencoded::parse(body) {
        values
            .entry(name.into_owned())
            .or_insert_with(Vec::new)
            .push(value.into_owned());
    }
    values
}

fn multipart_boundary(content_type: &str) -> Option<String> {
    content_type
        .split(';')
        .skip(1)
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"').to_owned())
        .filter(|boundary| !boundary.is_empty())
}

fn multipart_values(
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

fn disposition_parameter(disposition: &str, parameter: &str) -> Option<String> {
    disposition.split(';').skip(1).find_map(|piece| {
        let (name, value) = piece.trim().split_once('=')?;
        (name.trim() == parameter).then(|| value.trim().trim_matches('"').to_owned())
    })
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn cookie_values(headers: &HeaderMap) -> BTreeMap<String, String> {
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

fn valid_cookie_piece(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte > 0x20 && byte != b';' && byte != b'=')
        && !value.contains('\r')
        && !value.contains('\n')
}

fn forbidden_response_header(name: &str) -> bool {
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

fn deterministic_bytes(count: usize) -> Vec<u8> {
    (0..count).map(|index| (index % 251) as u8).collect()
}

fn deterministic_bytes_from(offset: usize, count: usize) -> Vec<u8> {
    (offset..offset + count)
        .map(|index| (index % 251) as u8)
        .collect()
}

fn binary_response(
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

fn ranged_response(
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

fn parse_range(value: &str, length: usize) -> Result<Option<(usize, usize)>, ()> {
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

fn strong_etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag
    })
}

fn weak_etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
    })
}

fn deterministic_unit(seed: u64) -> f64 {
    let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    (value >> 11) as f64 / ((1_u64 << 53) as f64)
}

fn image_asset(format: &str) -> Option<(&'static str, &'static [u8], &'static str)> {
    match format.to_ascii_lowercase().as_str() {
        "png" => Some(("image/png", PNG_ASSET, "\"biubin-png-v2\"")),
        "jpeg" | "jpg" => Some(("image/jpeg", JPEG_ASSET, "\"biubin-jpeg-v2\"")),
        "svg" => Some(("image/svg+xml", SVG_IMAGE.as_bytes(), "\"biubin-svg-v2\"")),
        "webp" => Some(("image/webp", WEBP_ASSET, "\"biubin-webp-v2\"")),
        _ => None,
    }
}

fn video_asset(format: &str) -> Option<(&'static str, &'static [u8], &'static str)> {
    match format.to_ascii_lowercase().as_str() {
        "mp4" => Some(("video/mp4", MP4_ASSET, "\"biubin-mp4-v2\"")),
        "webm" => Some(("video/webm", WEBM_ASSET, "\"biubin-webm-v2\"")),
        _ => None,
    }
}

fn audio_asset(format: &str) -> Option<(&'static str, &'static [u8], &'static str)> {
    match format.to_ascii_lowercase().as_str() {
        "wav" => Some(("audio/wav", WAV_ASSET, "\"biubin-wav-v1\"")),
        "mp3" => Some(("audio/mpeg", MP3_ASSET, "\"biubin-mp3-v1\"")),
        _ => None,
    }
}

const PNG_ASSET: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.png"));
const JPEG_ASSET: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.jpg"));
const WEBP_ASSET: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.webp"));
const MP4_ASSET: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.mp4"));
const WEBM_ASSET: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.webm"));
const WAV_ASSET: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.wav"));
const MP3_ASSET: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fixture.mp3"));

const SVG_IMAGE: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360"><rect width="640" height="360" fill="#111827"/><rect width="106" height="260" fill="#ef4444"/><rect x="106" width="106" height="260" fill="#f97316"/><rect x="212" width="106" height="260" fill="#facc15"/><rect x="318" width="106" height="260" fill="#22c55e"/><rect x="424" width="106" height="260" fill="#06b6d4"/><rect x="530" width="110" height="260" fill="#6366f1"/><rect y="260" width="640" height="100" fill="#1f2937"/><circle cx="80" cy="310" r="28" fill="#7ee0c4"/><circle cx="160" cy="310" r="28" fill="#f472b6"/><circle cx="240" cy="310" r="28" fill="#fb923c"/><path d="M330 310h240" stroke="#e5e7eb" stroke-width="8" stroke-dasharray="18 12"/><text x="24" y="44" fill="white" font-family="sans-serif" font-size="28" font-weight="700">BiuBin image fixture</text></svg>"##;

fn header_preview(value: &HeaderValue) -> String {
    let value = value.to_str().unwrap_or("[INVALID_UTF8]");
    if value.len() <= MAX_HEADER_VALUE_LEN {
        value.to_owned()
    } else {
        let preview: String = value.chars().take(MAX_HEADER_VALUE_LEN).collect();
        format!("{preview}…")
    }
}
