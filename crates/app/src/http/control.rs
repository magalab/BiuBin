use crate::generated::{INDEX_HTML, embedded_web};
use crate::http::{
    middleware::request_id,
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
            "endpoints": [
                {"protocol": "http", "method": "*", "path": "/http/anything/{*path}"},
                {"protocol": "http", "method": "*", "path": "/http/status/{code}"},
                {"protocol": "http", "method": "*", "path": "/http/delay/{seconds}"},
                {"protocol": "http", "method": "*", "path": "/http/redirect/{count}"},
                {"protocol": "http", "method": "GET", "path": "/http/bytes/{count}"},
                {"protocol": "http", "method": "GET", "path": "/http/stream-bytes/{count}"},
                {"protocol": "http", "method": "*", "path": "/http/gzip"},
                {"protocol": "http", "method": "*", "path": "/http/deflate"},
                {"protocol": "http", "method": "*", "path": "/http/basic-auth/{user}/{password}"},
                {"protocol": "http", "method": "*", "path": "/http/headers"},
                {"protocol": "http", "method": "*", "path": "/http/ip"},
                {"protocol": "http", "method": "*", "path": "/http/user-agent"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/echo"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/room/{name}"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/ticker"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/close"},
                {"protocol": "sse", "method": "GET", "path": "/sse/events"},
                {"protocol": "sse", "method": "GET", "path": "/sse/ticker"},
                {"protocol": "sse", "method": "GET", "path": "/api/v1/events/stream"},
                {"protocol": "graphql", "method": "GET/POST", "path": "/graphql"},
                {"protocol": "graphql", "method": "WS", "path": "/graphql/ws", "subprotocols": ["graphql-transport-ws", "graphql-ws"], "introspection": state.config.graphql_introspection_enabled},
                {"protocol": "tcp", "method": "line", "path": "tcp://{host}:{tcp_port}"},
                {"protocol": "tcp", "method": "length-prefixed", "path": "tcp://{host}:{tcp_port}"},
                {"protocol": "udp", "method": "echo", "path": "udp://{host}:{udp_port}"},
                {"protocol": "thrift", "method": "binary", "path": "thrift://{host}:{thrift_port}"},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_tcp_port}", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_auth_tcp_port}", "auth": "username/password", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_v5_port}", "version": 5, "auth": "username/password", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtts://{host}:{mqtt_tls_port}", "tls": state.config.mqtt_tls_mode, "enabled": state.config.mqtt_enabled && state.config.mqtt_tls_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "ws://{host}:{mqtt_ws_port}", "transport": "websocket", "enabled": state.config.mqtt_enabled},
            ],
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
