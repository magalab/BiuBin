use crate::http::{
    middleware::request_id,
    response::{is_sensitive_header, json_response},
};
use crate::state::{AppState, MAX_HEADER_VALUE_LEN, STREAM_BYTES_CHUNK_SIZE};
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Extension, OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use serde_json::json;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;

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
    let location = format!("/http/redirect/{}", count - 1);
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
    let body: Vec<u8> = (0..count).map(|index| (index % 251) as u8).collect();
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
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
            "headers": header_values,
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
    ConnectInfo(address): ConnectInfo<SocketAddr>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let origin = address.ip().to_string();
    json_response(StatusCode::OK, request_id, json!({"origin": origin}))
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

fn header_preview(value: &HeaderValue) -> String {
    let value = value.to_str().unwrap_or("[INVALID_UTF8]");
    if value.len() <= MAX_HEADER_VALUE_LEN {
        value.to_owned()
    } else {
        let preview: String = value.chars().take(MAX_HEADER_VALUE_LEN).collect();
        format!("{preview}…")
    }
}
