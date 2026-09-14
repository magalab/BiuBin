use crate::generated::{INDEX_HTML, embedded_web};
use crate::http::{
    middleware::request_id,
    openapi,
    response::{json_response, text_response},
};
use crate::state::{AppState, EventsResponse};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

pub(crate) async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    text_response(
        StatusCode::OK,
        request_id(&headers, &state),
        INDEX_HTML,
        "text/html; charset=utf-8",
    )
}

pub(crate) async fn web_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let asset_name = format!("assets/{path}");
    let Some((_, bytes)) = embedded_web::WEB_ASSETS
        .iter()
        .find(|(name, _)| *name == asset_name)
    else {
        return text_response(
            StatusCode::NOT_FOUND,
            request_id,
            "asset not found",
            "text/plain; charset=utf-8",
        );
    };
    let content_type = match path.rsplit('.').next().unwrap_or_default() {
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    };
    let mut response = (
        [(header::CONTENT_TYPE, content_type)],
        Body::from(bytes.to_vec()),
    )
        .into_response();
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) async fn healthz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        json!({"status": "ok", "service": "biubin"}),
    )
}

pub(crate) async fn readyz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let ready = state.readiness.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(
        status,
        request_id(&headers, &state),
        json!({"ready": ready, "listeners": state.readiness.snapshot()}),
    )
}

pub(crate) async fn info_api(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    json_response(
        StatusCode::OK,
        request_id,
        json!({
            "name": "biubin",
            "version": env!("CARGO_PKG_VERSION"),
            "bind_host": state.config.bind_host,
            "advertise_host": state.config.advertise_host,
            "ports": state.config.ports,
            "bound_addresses": state.bound.bound_json(),
            "advertised_addresses": state.bound.advertised_json(&state.config.advertise_host),
            "mqtt_enabled": state.config.mqtt_enabled,
            "mqtt_tls_enabled": state.config.mqtt_tls_enabled,
            "mqtt_tls_mode": state.config.mqtt_tls_mode,
            "graphql_introspection_enabled": state.config.graphql_introspection_enabled,
            "http_allow_external_redirects": state.config.http_allow_external_redirects,
            "http_external_redirect_host_allowlist":
                state.config.http_external_redirect_hosts.len(),
            "mqtt_topic_acl": false,
            "mqtt_session_persistence": "process",
            "mqtt_graceful_drain": false,
            "grpc_tls_enabled": state.config.grpc_tls_enabled,
            "grpc_tls_mode": state.config.grpc_tls_mode,
            "limits": {
                "http_body_bytes": state.config.http_body_limit,
                "max_response_bytes": state.config.max_bytes_response,
                "max_connections": state.config.max_connections,
                "event_capacity": state.config.event_capacity,
            },
            "ready": state.readiness.is_ready(),
        }),
    )
}

pub(crate) async fn capabilities_api(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let mut endpoints = openapi::http_capabilities();
    endpoints.extend([
        json!({"protocol": "websocket", "methods": ["GET"], "path": "/ws/echo"}),
        json!({"protocol": "websocket", "methods": ["GET"], "path": "/ws/room/{name}"}),
        json!({"protocol": "websocket", "methods": ["GET"], "path": "/ws/ticker"}),
        json!({"protocol": "websocket", "methods": ["GET"], "path": "/ws/close"}),
        json!({"protocol": "sse", "methods": ["GET"], "path": "/sse/events"}),
        json!({"protocol": "sse", "methods": ["GET"], "path": "/sse/ticker"}),
        json!({"protocol": "sse", "methods": ["GET"], "path": "/api/v1/events/stream"}),
        json!({"protocol": "graphql", "methods": ["GET", "POST"], "path": "/graphql"}),
        json!({"protocol": "graphql", "methods": ["WS"], "path": "/graphql/ws", "subprotocols": ["graphql-transport-ws", "graphql-ws"], "introspection": state.config.graphql_introspection_enabled}),
        json!({"protocol": "tcp", "methods": ["line"], "path": "tcp://{host}:{tcp_port}"}),
        json!({"protocol": "tcp", "methods": ["length-prefixed"], "path": "tcp://{host}:{tcp_port}"}),
        json!({"protocol": "udp", "methods": ["echo"], "path": "udp://{host}:{udp_port}"}),
        json!({"protocol": "thrift", "methods": ["binary"], "path": "thrift://{host}:{thrift_port}"}),
        json!({"protocol": "mqtt", "methods": ["publish/subscribe"], "path": "mqtt://{host}:{mqtt_tcp_port}", "enabled": state.config.mqtt_enabled}),
        json!({"protocol": "mqtt", "methods": ["publish/subscribe"], "path": "mqtt://{host}:{mqtt_auth_tcp_port}", "auth": "username/password", "enabled": state.config.mqtt_enabled}),
        json!({"protocol": "mqtt", "methods": ["publish/subscribe"], "path": "mqtt://{host}:{mqtt_v5_port}", "version": 5, "auth": "username/password", "enabled": state.config.mqtt_enabled}),
        json!({"protocol": "mqtt", "methods": ["publish/subscribe"], "path": "mqtts://{host}:{mqtt_tls_port}", "tls": state.config.mqtt_tls_mode, "enabled": state.config.mqtt_enabled && state.config.mqtt_tls_enabled}),
        json!({"protocol": "mqtt", "methods": ["publish/subscribe"], "path": "ws://{host}:{mqtt_ws_port}", "transport": "websocket", "enabled": state.config.mqtt_enabled}),
    ]);
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        json!({
            "schema_version": 1,
            "service": "biubin",
            "addresses": {
                "bound": state.bound.bound_json(),
                "advertised": state.bound.advertised_json(&state.config.advertise_host),
            },
            "features": {
                "graphql_introspection": state.config.graphql_introspection_enabled,
                "mqtt_topic_acl": false,
                "mqtt_session_persistence": "process",
                "mqtt_graceful_drain": false,
            },
            "endpoints": endpoints,
            "listeners": state.readiness.snapshot(),
        }),
    )
}

#[derive(serde::Deserialize)]
pub(crate) struct EventsQuery {
    limit: Option<usize>,
    protocol: Option<String>,
}

pub(crate) async fn events_api(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let events = state.events.list(limit, query.protocol.as_deref());
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        serde_json::to_value(EventsResponse {
            events,
            dropped: state.events.dropped(),
        })
        .expect("event serialization cannot fail"),
    )
}
