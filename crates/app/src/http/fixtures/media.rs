use crate::http::{middleware::request_id, response::json_response};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use serde_json::json;

use super::support::ranged_response;

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
