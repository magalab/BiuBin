use crate::{graphql, state::AppState};
use axum::Router;
use axum::middleware as axum_middleware;
use axum::routing::{any, delete, get, patch, post, put};

pub(crate) fn router(state: AppState) -> Router {
    let body_limit = state.config.http_body_limit;
    Router::new()
        .route("/", get(control::index))
        .route("/assets/{*path}", get(control::web_asset))
        .route("/healthz", get(control::healthz))
        .route("/readyz", get(control::readyz))
        .route("/api/v1/info", get(control::info_api))
        .route("/api/v1/capabilities", get(control::capabilities_api))
        .route("/api/v1/events", get(control::events_api))
        .route(
            "/graphql",
            get(graphql::graphql_handler).post(graphql::graphql_handler),
        )
        .route("/graphql/ws", get(graphql::graphql_ws_handler))
        // Root request-echo aliases. Keep the /http namespace below as the
        // stable BiuBin-specific fixture API.
        .route("/get", get(fixtures::http_anything_root))
        .route("/post", post(fixtures::http_anything_root))
        .route("/put", put(fixtures::http_anything_root))
        .route("/patch", patch(fixtures::http_anything_root))
        .route("/delete", delete(fixtures::http_anything_root))
        .route("/anything", any(fixtures::http_anything_root))
        .route("/anything/", any(fixtures::http_anything_root))
        .route("/anything/{*path}", any(fixtures::http_anything_path))
        .route("/http/status/{code}", any(fixtures::http_status))
        .route("/http/delay/{seconds}", any(fixtures::http_delay))
        .route("/http/redirect/{count}", any(fixtures::http_redirect))
        .route("/http/bytes/{count}", get(fixtures::http_bytes))
        .route(
            "/http/stream-bytes/{count}",
            get(fixtures::http_stream_bytes),
        )
        .route("/http/gzip", any(fixtures::http_gzip))
        .route("/http/deflate", any(fixtures::http_deflate))
        .route(
            "/http/basic-auth/{user}/{password}",
            any(fixtures::http_basic_auth),
        )
        .route("/http/anything", any(fixtures::http_anything_root))
        .route("/http/anything/{*path}", any(fixtures::http_anything_path))
        .route("/http/headers", any(fixtures::http_headers))
        .route("/http/user-agent", any(fixtures::http_user_agent))
        .route("/http/ip", any(fixtures::http_ip))
        .route("/ws/echo", get(websocket::ws_echo))
        .route("/ws/room/{name}", get(websocket::ws_room))
        .route("/ws/ticker", get(websocket::ws_ticker))
        .route("/ws/close", get(websocket::ws_close))
        .route("/sse/events", get(sse::sse_events))
        .route("/sse/ticker", get(sse::sse_ticker))
        .route("/api/v1/events/stream", get(sse::events_stream))
        .layer(axum::extract::DefaultBodyLimit::max(body_limit))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::request_id_middleware,
        ))
        .with_state(state)
}

mod control;
mod fixtures;
pub(crate) mod middleware;
pub(crate) mod response;
mod sse;
mod websocket;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addresses::BoundAddresses;
    use crate::graphql;
    use crate::state::AppState;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use biubin_core::{Config, EventStore, Readiness};
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::io::Read;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use tokio::sync::Semaphore;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let config = Config::default();
        let readiness = Readiness::with_required(["http".to_owned(), "grpc_h2c".to_owned()]);
        readiness.mark("http", true);
        readiness.mark("grpc_h2c", true);
        let events = EventStore::new(200);
        let config = Arc::new(config);
        AppState {
            graphql: graphql::build_schema(config.clone(), events.clone()),
            bound: BoundAddresses {
                http: "127.0.0.1:8080".parse().unwrap(),
                grpc_h2c: "127.0.0.1:9000".parse().unwrap(),
                grpc_tls: None,
                tcp: "127.0.0.1:7000".parse().unwrap(),
                udp: "127.0.0.1:7001".parse().unwrap(),
                thrift: "127.0.0.1:9090".parse().unwrap(),
                mqtt: None,
            },
            events,
            request_seq: Arc::new(AtomicU64::new(1)),
            connection_slots: Arc::new(Semaphore::new(config.max_connections)),
            rooms: Arc::new(Mutex::new(HashMap::new())),
            config,
            readiness,
        }
    }

    #[tokio::test]
    async fn anything_echoes_request_and_redacts_sensitive_headers() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/http/anything/demo?q=1")
                    .header("x-request-id", "test-request")
                    .header("authorization", "Bearer secret")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-request-id"], "test-request");
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["method"], "POST");
        assert_eq!(value["path"], "demo");
        assert_eq!(value["body"], "hello");
        assert_eq!(value["headers"]["authorization"][0], "[REDACTED]");
    }

    #[tokio::test]
    async fn request_echo_aliases_echo_their_explicit_methods() {
        for (method, path, body) in [
            ("GET", "/get", "get body"),
            ("POST", "/post", "post body"),
            ("PUT", "/put", "put body"),
            ("PATCH", "/patch", "patch body"),
            ("DELETE", "/delete", "delete body"),
        ] {
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{method} {path}");
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["method"], method);
            assert_eq!(value["path"], "");
            assert_eq!(value["body"], body);
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/get")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .is_empty()
        );

        for (method, path, body, expected_path) in [
            ("GET", "/anything", "root body", ""),
            ("DELETE", "/anything/", "slash body", ""),
            ("PATCH", "/anything/foo/bar", "nested body", "foo/bar"),
        ] {
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{method} {path}");
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["method"], method);
            assert_eq!(value["path"], expected_path);
            assert_eq!(value["body"], body);
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/get")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[header::ALLOW], "GET,HEAD");
    }

    #[tokio::test]
    async fn capabilities_describe_the_request_echo_root_routes() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let endpoints = value["endpoints"].as_array().unwrap();
        assert!(
            endpoints
                .iter()
                .any(|endpoint| { endpoint["method"] == "*" && endpoint["path"] == "/anything" })
        );
        assert!(
            endpoints
                .iter()
                .any(|endpoint| { endpoint["method"] == "*" && endpoint["path"] == "/anything/" })
        );
        assert!(endpoints.iter().any(|endpoint| {
            endpoint["method"] == "*" && endpoint["path"] == "/anything/{*path}"
        }));
    }

    #[tokio::test]
    async fn status_and_compression_endpoints_are_deterministic() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/http/status/418")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/http/gzip")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let mut decoder = GzDecoder::new(bytes.as_ref());
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).unwrap();
        assert_eq!(decoded, r#"{"message":"biubin compressed response"}"#);
    }

    #[tokio::test]
    async fn sse_endpoint_contains_event_fields() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/sse/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("event: message"));
        assert!(body.contains("id: 1"));
        assert!(body.contains("retry: 1000"));
    }

    #[tokio::test]
    async fn middleware_adds_request_ids_to_rejections() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/not-found")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            response.headers()["x-request-id"]
                .as_bytes()
                .starts_with(b"req_")
        );

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/http/anything/limited")
                    .body(Body::from(vec![b'x'; 2 * 1024 * 1024 + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(response.headers().contains_key("x-request-id"));
    }
}
