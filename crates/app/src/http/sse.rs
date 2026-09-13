use crate::http::{
    middleware::{acquire_connection_slot, request_id},
    response::json_response,
};
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::convert::Infallible;
use tokio::sync::{OwnedSemaphorePermit, broadcast};

#[derive(serde::Deserialize)]
pub(crate) struct SseTickerQuery {
    interval_ms: Option<u64>,
    count: Option<u64>,
}

pub(crate) async fn sse_events(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let last_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let stream = fixed_sse_stream(last_id, 5, 100);
    sse_response(request_id, stream, permit)
}

pub(crate) async fn sse_ticker(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SseTickerQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let interval_ms = query.interval_ms.unwrap_or(1000).clamp(10, 60_000);
    let count = query.count.unwrap_or(5).min(1000);
    let stream = ticker_sse_stream(interval_ms, count);
    sse_response(request_id, stream, permit)
}

pub(crate) async fn events_stream(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut receiver = state.events.subscribe();
    let initial = state.events.list(200, None);
    let stream = async_stream::stream! {
        let mut cursor = cursor;
        let initial = initial
            .into_iter()
            .rev()
            .filter(|event| event.seq > cursor)
            .collect::<Vec<_>>();
        for event in initial {
            cursor = event.seq;
            let result = SseEvent::default()
                .event("event")
                .id(event.seq.to_string())
                .json_data(event);
            if let Ok(event) = result {
                yield Ok::<SseEvent, Infallible>(event);
            }
        }
        loop {
            match receiver.recv().await {
                Ok(event) if event.seq > cursor => {
                    cursor = event.seq;
                    let result = SseEvent::default()
                        .event("event")
                        .id(event.seq.to_string())
                        .json_data(event);
                    if let Ok(event) = result {
                        yield Ok::<SseEvent, Infallible>(event);
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let notice = SseEvent::default()
                        .event("lagged")
                        .id(cursor.to_string())
                        .json_data(json!({
                            "dropped": skipped,
                            "message": "event stream lagged; replaying retained events"
                        }));
                    if let Ok(notice) = notice {
                        yield Ok::<SseEvent, Infallible>(notice);
                    }
                    let replay = state
                        .events
                        .list(200, None)
                        .into_iter()
                        .rev()
                        .filter(|event| event.seq > cursor)
                        .collect::<Vec<_>>();
                    for event in replay {
                        cursor = event.seq;
                        let result = SseEvent::default()
                            .event("event")
                            .id(event.seq.to_string())
                            .json_data(event);
                        if let Ok(event) = result {
                            yield Ok::<SseEvent, Infallible>(event);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    sse_response(request_id, stream, permit)
}

fn fixed_sse_stream(
    last_id: u64,
    count: u64,
    interval_ms: u64,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    async_stream::stream! {
        for sequence in 1..=count {
            if sequence <= last_id { continue; }
            if sequence > last_id + 1 {
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            }
            let event = SseEvent::default()
                .event("message")
                .id(sequence.to_string())
                .retry(std::time::Duration::from_millis(1000))
                .json_data(json!({"sequence": sequence, "message": "biubin event"}))
                .expect("static SSE event is serializable");
            yield Ok(event);
        }
    }
}

fn ticker_sse_stream(
    interval_ms: u64,
    count: u64,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    async_stream::stream! {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        interval.tick().await;
        for sequence in 1..=count {
            interval.tick().await;
            let event = SseEvent::default()
                .event("tick")
                .id(sequence.to_string())
                .retry(std::time::Duration::from_millis(1000))
                .json_data(json!({"sequence": sequence}))
                .expect("static SSE event is serializable");
            yield Ok(event);
        }
    }
}

fn sse_response<S>(request_id: String, stream: S, permit: OwnedSemaphorePermit) -> Response
where
    S: Stream<Item = Result<SseEvent, Infallible>> + Send + 'static,
{
    let stream = async_stream::stream! {
        let _permit = permit;
        futures_util::pin_mut!(stream);
        while let Some(event) = stream.next().await {
            yield event;
        }
    };
    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(10))
                .text("keep-alive"),
        )
        .into_response();
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}
