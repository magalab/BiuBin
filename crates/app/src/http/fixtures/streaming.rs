use crate::http::{middleware::request_id, response::json_response};
use crate::state::{AppState, STREAM_BYTES_CHUNK_SIZE};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;

use super::support::{
    deterministic_bytes, deterministic_bytes_from, deterministic_unit, ranged_response,
};

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
