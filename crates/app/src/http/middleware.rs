use crate::http::response::json_response;
use crate::state::{AppState, MAX_HEADER_VALUE_LEN, MAX_REQUEST_ID_LEN};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::json;
use std::sync::atomic::Ordering;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) async fn request_id_middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let oversized = request
        .headers()
        .iter()
        .any(|(_, value)| value.as_bytes().len() > MAX_HEADER_VALUE_LEN);
    let request_id = request_id(request.headers(), &state);
    request.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).expect("request id is valid"),
    );

    if oversized {
        return json_response(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            request_id,
            json!({"error": "request header value exceeds the configured limit"}),
        );
    }

    let long_lived = request.uri().path().starts_with("/ws/")
        || request.uri().path().starts_with("/sse/")
        || request.uri().path() == "/api/v1/events/stream"
        || request.uri().path() == "/graphql/ws";
    let permit = if long_lived {
        None
    } else {
        let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
            return json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                request_id,
                json!({"error": "connection limit reached"}),
            );
        };
        Some(permit)
    };
    let mut response = next.run(request).await;
    if !response.headers().contains_key("x-request-id") {
        response.headers_mut().insert(
            "x-request-id",
            HeaderValue::from_str(&request_id).expect("request id is valid"),
        );
    }
    drop(permit);
    response
}

pub(crate) fn request_id(headers: &HeaderMap, state: &AppState) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= MAX_REQUEST_ID_LEN)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("req_{}", state.request_seq.fetch_add(1, Ordering::Relaxed)))
}

pub(crate) fn acquire_connection_slot(
    state: &AppState,
    request_id: &str,
) -> Result<OwnedSemaphorePermit, ()> {
    state
        .connection_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            state.events.push(
                "connection",
                "connection_rejected",
                format!("request_id={request_id}"),
            );
        })
}
