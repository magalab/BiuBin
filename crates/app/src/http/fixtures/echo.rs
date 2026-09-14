use crate::http::{
    middleware::request_id,
    response::{is_sensitive_header, json_response},
};
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Extension, OriginalUri, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;

use super::support::{header_preview, parse_structured_body, query_values, redacted_headers};

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
