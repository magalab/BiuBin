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
        .route("/openapi", get(openapi::ui))
        .route("/openapi.json", get(openapi::document))
        .route(
            "/graphql",
            get(graphql::graphql_handler).post(graphql::graphql_handler),
        )
        .route("/graphql/ws", get(graphql::graphql_ws_handler))
        // HTTPBin-compatible fixture routes use the root namespace.
        // Axum's `get(...)` router also accepts HEAD and removes the response body.
        .route("/get", get(fixtures::http_anything_root))
        .route("/post", post(fixtures::http_anything_root))
        .route("/put", put(fixtures::http_anything_root))
        .route("/patch", patch(fixtures::http_anything_root))
        .route("/delete", delete(fixtures::http_anything_root))
        .route("/anything", any(fixtures::http_anything_root))
        .route("/anything/", any(fixtures::http_anything_root))
        .route("/anything/{*path}", any(fixtures::http_anything_path))
        .route("/status/{code}", any(fixtures::http_status))
        .route("/delay/{seconds}", any(fixtures::http_delay))
        .route("/redirect/{count}", any(fixtures::http_redirect))
        .route("/bytes/{count}", get(fixtures::http_bytes))
        .route("/stream-bytes/{count}", get(fixtures::http_stream_bytes))
        .route("/gzip", any(fixtures::http_gzip))
        .route("/deflate", any(fixtures::http_deflate))
        .route(
            "/basic-auth/{user}/{password}",
            any(fixtures::http_basic_auth),
        )
        .route("/headers", any(fixtures::http_headers))
        .route("/user-agent", any(fixtures::http_user_agent))
        .route("/ip", any(fixtures::http_ip))
        .route("/image", get(fixtures::http_image_default))
        .route("/image/{format}", get(fixtures::http_image_format))
        .route("/video", get(fixtures::http_video_default))
        .route("/video/{format}", get(fixtures::http_video_format))
        .route("/audio", get(fixtures::http_audio_default))
        .route("/audio/{format}", get(fixtures::http_audio_format))
        .route("/response-headers", get(fixtures::http_response_headers))
        .route("/cookies", get(fixtures::http_cookies))
        .route(
            "/cookies/set/{name}/{value}",
            get(fixtures::http_cookies_set),
        )
        .route("/cookies/delete/{name}", get(fixtures::http_cookies_delete))
        .route("/cache", get(fixtures::http_cache))
        .route("/cache/{seconds}", get(fixtures::http_cache_with_max_age))
        .route("/etag/{value}", get(fixtures::http_etag))
        .route("/redirect-to", get(fixtures::http_redirect_to))
        .route("/json", get(fixtures::http_json))
        .route("/html", get(fixtures::http_html))
        .route("/xml", get(fixtures::http_xml))
        .route("/encoding/utf8", get(fixtures::http_utf8))
        .route("/range/{count}", get(fixtures::http_range))
        .route("/drip", get(fixtures::http_drip))
        .route("/unstable", get(fixtures::http_unstable))
        .route("/bearer", get(fixtures::http_bearer))
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
mod openapi;
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
    use std::collections::{HashMap, HashSet};
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
                    .uri("/anything/demo?q=1")
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

        for (method, path, expected_allow) in [
            ("POST", "/get", "GET,HEAD"),
            ("GET", "/post", "POST"),
            ("GET", "/put", "PUT"),
            ("GET", "/patch", "PATCH"),
            ("GET", "/delete", "DELETE"),
        ] {
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path}"
            );
            assert_eq!(
                response.headers()[header::ALLOW],
                expected_allow,
                "{method} {path}"
            );
        }
    }

    #[tokio::test]
    async fn capabilities_describe_all_request_echo_routes() {
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
        assert_eq!(value["schema_version"], 1);
        let endpoints = value["endpoints"].as_array().unwrap();
        assert!(endpoints.iter().all(|endpoint| {
            endpoint["methods"].is_array() && endpoint.get("method").is_none()
        }));

        let has_endpoint = |path: &str, expected_methods: &[&str]| {
            endpoints.iter().any(|endpoint| {
                if endpoint["path"] != path {
                    return false;
                }
                let methods: HashSet<&str> = endpoint["methods"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(Value::as_str)
                    .collect();
                methods == expected_methods.iter().copied().collect()
            })
        };
        let fixture_probe_path = |path: &str| {
            if path == "/anything/*" {
                return "/anything/probe".to_owned();
            }
            path.replace("{code}", "200")
                .replace("{seconds}", "0")
                .replace("{count}", "1")
                .replace("{user}", "user")
                .replace("{password}", "password")
                .replace("{name}", "name")
                .replace("{value}", "value")
                .replace("{path}", "probe")
        };
        for endpoint in endpoints.iter() {
            if endpoint["protocol"] != "http" {
                continue;
            }
            let path = fixture_probe_path(endpoint["path"].as_str().unwrap());
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .uri(path.clone())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_ne!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
        for (path, methods) in [
            ("/get", &["GET", "HEAD"][..]),
            ("/post", &["POST"][..]),
            ("/put", &["PUT"][..]),
            ("/patch", &["PATCH"][..]),
            ("/delete", &["DELETE"][..]),
            ("/anything", &["*"][..]),
            ("/anything/", &["*"][..]),
            ("/anything/*", &["*"][..]),
            ("/status/{code}", &["*"][..]),
            ("/delay/{seconds}", &["*"][..]),
            ("/redirect/{count}", &["*"][..]),
            ("/bytes/{count}", &["GET", "HEAD"][..]),
            ("/stream-bytes/{count}", &["GET", "HEAD"][..]),
            ("/gzip", &["*"][..]),
            ("/deflate", &["*"][..]),
            ("/basic-auth/{user}/{password}", &["*"][..]),
            ("/headers", &["*"][..]),
            ("/ip", &["*"][..]),
            ("/user-agent", &["*"][..]),
            ("/image", &["GET", "HEAD"][..]),
            ("/image/png", &["GET", "HEAD"][..]),
            ("/image/jpeg", &["GET", "HEAD"][..]),
            ("/image/svg", &["GET", "HEAD"][..]),
            ("/image/webp", &["GET", "HEAD"][..]),
            ("/video", &["GET", "HEAD"][..]),
            ("/video/mp4", &["GET", "HEAD"][..]),
            ("/video/webm", &["GET", "HEAD"][..]),
            ("/audio", &["GET", "HEAD"][..]),
            ("/audio/wav", &["GET", "HEAD"][..]),
            ("/audio/mp3", &["GET", "HEAD"][..]),
            ("/response-headers", &["GET", "HEAD"][..]),
            ("/cookies", &["GET", "HEAD"][..]),
            ("/cookies/set/{name}/{value}", &["GET", "HEAD"][..]),
            ("/cookies/delete/{name}", &["GET", "HEAD"][..]),
            ("/cache", &["GET", "HEAD"][..]),
            ("/cache/{seconds}", &["GET", "HEAD"][..]),
            ("/etag/{value}", &["GET", "HEAD"][..]),
            ("/redirect-to", &["GET", "HEAD"][..]),
            ("/json", &["GET", "HEAD"][..]),
            ("/html", &["GET", "HEAD"][..]),
            ("/xml", &["GET", "HEAD"][..]),
            ("/encoding/utf8", &["GET", "HEAD"][..]),
            ("/range/{count}", &["GET", "HEAD"][..]),
            ("/drip", &["GET", "HEAD"][..]),
            ("/unstable", &["GET", "HEAD"][..]),
            ("/bearer", &["GET", "HEAD"][..]),
        ] {
            assert!(
                has_endpoint(path, methods),
                "missing {:?} {} endpoint",
                methods,
                path
            );
        }
        assert!(endpoints.iter().all(|endpoint| {
            endpoint["path"]
                .as_str()
                .is_none_or(|path| !path.starts_with("/http/"))
        }));
    }

    #[tokio::test]
    async fn old_http_namespace_is_not_registered() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/http/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_and_compression_endpoints_are_deterministic() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/status/418")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);

        let response = router(test_state())
            .oneshot(Request::builder().uri("/gzip").body(Body::empty()).unwrap())
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
    async fn httpbin_root_fixtures_cover_media_and_openapi() {
        for (path, content_type, magic) in [
            ("/image", "image/png", b"\x89PNG".as_slice()),
            ("/image/png", "image/png", b"\x89PNG".as_slice()),
            ("/image/jpeg", "image/jpeg", b"\xff\xd8\xff".as_slice()),
            ("/image/svg", "image/svg+xml", b"<svg".as_slice()),
            ("/image/webp", "image/webp", b"RIFF".as_slice()),
            ("/video", "video/mp4", b"\x00\x00\x00\x20ftyp".as_slice()),
            (
                "/video/mp4",
                "video/mp4",
                b"\x00\x00\x00\x20ftyp".as_slice(),
            ),
            ("/video/webm", "video/webm", b"\x1a\x45\xdf\xa3".as_slice()),
            ("/audio", "audio/wav", b"RIFF".as_slice()),
            ("/audio/wav", "audio/wav", b"RIFF".as_slice()),
            ("/audio/mp3", "audio/mpeg", b"ID3".as_slice()),
        ] {
            let response = router(test_state())
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            assert!(body.starts_with(magic), "{path} returned unexpected bytes");
            let minimum_size = match content_type {
                "image/svg+xml" => 500,
                content_type if content_type.starts_with("image/") => 5_000,
                content_type if content_type.starts_with("video/") => 10_000,
                content_type if content_type.starts_with("audio/") => 5_000,
                _ => 0,
            };
            assert!(
                body.len() >= minimum_size,
                "{path} fixture is unexpectedly small: {} bytes",
                body.len()
            );
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/image/png")
                    .header(header::RANGE, "bytes=0-9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert!(
            response.headers()[header::CONTENT_RANGE]
                .to_str()
                .unwrap()
                .starts_with("bytes 0-9/")
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .len(),
            10
        );

        for path in ["/image/png", "/video/mp4", "/audio/wav"] {
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .method("HEAD")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "HEAD {path}");
            assert!(
                response.headers()[header::CONTENT_LENGTH]
                    .to_str()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
                    > 0
            );
            assert!(
                to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        let document: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(document["openapi"], "3.0.3");
        let tags = document["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 8);
        assert_eq!(tags[0]["name"], "Request fixtures");
        assert!(tags.iter().all(|tag| tag["description"].is_string()));
        let operation_ids: Vec<&str> = document["paths"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|path| path.as_object().unwrap().values())
            .filter_map(|operation| operation["operationId"].as_str())
            .collect();
        let unique_operation_ids: HashSet<_> = operation_ids.iter().copied().collect();
        assert_eq!(operation_ids.len(), unique_operation_ids.len());
        assert!(document["paths"]["/status/{code}"].is_object());
        assert!(document["paths"]["/image/png"].is_object());
        assert!(document["paths"]["/video"].is_object());
        assert!(document["paths"]["/audio"].is_object());
        assert!(document["components"]["securitySchemes"]["bearerAuth"].is_object());
        let drip_parameters = document["paths"]["/drip"]["get"]["parameters"]
            .as_array()
            .unwrap();
        let duration_schema = drip_parameters
            .iter()
            .find(|parameter| parameter["name"] == "duration")
            .unwrap();
        assert_eq!(duration_schema["schema"]["type"], "number");
        assert_eq!(duration_schema["schema"]["minimum"], 0);
        assert!(document["paths"]["/openapi"].is_null());

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/openapi")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("/assets/") || body.contains("/openapi.json"));
    }

    #[tokio::test]
    async fn response_headers_cookies_and_conditional_requests_work() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/response-headers?X-Test=one&X-Test=two&Cache-Control=no-cache")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-cache");
        let values: Vec<_> = response
            .headers()
            .get_all("x-test")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(values, vec!["one", "two"]);

        for name in [
            "Content-Type",
            "CONTENT-LENGTH",
            "Set-Cookie",
            "Host",
            "X-Request-Id",
        ] {
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/response-headers?{name}=test"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{name}");
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cookies/set/demo/value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()[header::LOCATION], "/cookies");
        let cookie = response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cookies")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["cookies"]["demo"], "value");

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers()[header::ETAG].clone();
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .header(header::IF_NONE_MATCH, etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert!(response.headers().contains_key(header::LAST_MODIFIED));

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .header(header::IF_NONE_MATCH, "\"other\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .header(header::IF_NONE_MATCH, "\"other\"")
                    .header(header::IF_MODIFIED_SINCE, "Thu, 22 Oct 2015 07:28:00 GMT")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .header(header::IF_MODIFIED_SINCE, "Wed, 21 Oct 2015 07:28:00 GMT")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/cache")
                    .header(header::IF_MODIFIED_SINCE, "Tue, 20 Oct 2015 07:28:00 GMT")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/etag/demo")
                    .header(header::IF_MATCH, "\"other\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/etag/demo")
                    .header(header::IF_MATCH, "W/\"demo\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/etag/demo")
                    .header(header::IF_NONE_MATCH, "W/\"demo\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn structured_echo_redirect_range_and_failure_fixtures_work() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post?tag=one&tag=two")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"hello":"world"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["args"]["tag"], serde_json::json!(["one", "two"]));
        assert_eq!(value["json"]["hello"], "world");
        assert_eq!(value["data"], r#"{"hello":"world"}"#);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("a=1&a=2&b=ok"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["form"]["a"], serde_json::json!(["1", "2"]));
        assert_eq!(value["form"]["b"], serde_json::json!(["ok"]));

        let multipart_body = concat!(
            "--biubin-boundary\r\n",
            "Content-Disposition: form-data; name=\"field\"\r\n",
            "\r\n",
            "value\r\n",
            "--biubin-boundary\r\n",
            "Content-Disposition: form-data; name=\"upload\"; filename=\"demo.txt\"\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "hello\r\n",
            "--biubin-boundary--\r\n"
        );
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post")
                    .header(
                        header::CONTENT_TYPE,
                        "multipart/form-data; boundary=biubin-boundary",
                    )
                    .body(Body::from(multipart_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["form"]["field"], serde_json::json!(["value"]));
        assert_eq!(value["files"]["upload"], serde_json::json!(["hello"]));

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/redirect-to?url=%2Ftarget&status_code=307")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers()[header::LOCATION], "/target");

        for uri in [
            "/redirect-to?url=https%3A%2F%2Fexample.com%2Ftarget",
            "/redirect-to?url=%2F%2Fevil.example%2Ftarget",
            "/redirect-to?url=%2Ftarget%0D%0AX-Test%3A%20injected",
            "/redirect-to?url=%2Ftarget&status_code=400",
        ] {
            let response = router(test_state())
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }

        let long_url = format!("/redirect-to?url=%2F{}", "a".repeat(2048));
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri(long_url)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let mut state = test_state();
        Arc::make_mut(&mut state.config).http_allow_external_redirects = true;
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/redirect-to?url=https%3A%2F%2Fexample.com%2Ftarget")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers()[header::LOCATION],
            "https://example.com/target"
        );
        for url in [
            "http%3A%2F%2F127.0.0.1%2Fadmin",
            "http%3A%2F%2F%5B%3A%3A1%5D%2Fadmin",
            "http%3A%2F%2F%5B%3A%3Affff%3A127.0.0.1%5D%3A6379",
            "http%3A%2F%2F%5B%3A%3Affff%3A169.254.169.254%5D%2Flatest",
            "gopher%3A%2F%2Fexample.com%2F1",
        ] {
            let response = router(state.clone())
                .oneshot(
                    Request::builder()
                        .uri(format!("/redirect-to?url={url}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{url}");
        }

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/range/32")
                    .header(header::RANGE, "bytes=2-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-5/32");
        assert_eq!(
            to_bytes(response.into_body(), 1024 * 1024).await.unwrap(),
            &b"\x02\x03\x04\x05"[..]
        );

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/range/32")
                    .header(header::RANGE, "bytes=99-100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/drip?numbytes=5&chunk_size=2&code=201")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            to_bytes(response.into_body(), 1024 * 1024).await.unwrap(),
            &[0_u8, 1, 2, 3, 4][..]
        );

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/unstable?failure_rate=1&seed=42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/unstable?failure_rate=0&seed=42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/bearer")
                    .header(header::AUTHORIZATION, "Bearer biubin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/bearer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
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
                    .uri("/anything/limited")
                    .body(Body::from(vec![b'x'; 2 * 1024 * 1024 + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(response.headers().contains_key("x-request-id"));
    }
}
