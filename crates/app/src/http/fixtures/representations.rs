use crate::http::{
    middleware::request_id,
    response::{json_response, text_response},
};
use crate::state::AppState;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use serde_json::json;
use std::io::Write;

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
