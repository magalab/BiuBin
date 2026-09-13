use crate::http::{
    middleware::{acquire_connection_slot, request_id},
    response::{json_response, response_with_request_id},
};
use crate::state::{AppState, MAX_WS_MESSAGE_SIZE, MAX_WS_ROOM_NAME_LEN, MAX_WS_ROOMS, WsPayload};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, broadcast};

pub(crate) async fn ws_echo(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let task_request_id = request_id.clone();
    let response = upgrade
        .on_upgrade(move |socket| ws_echo_task(socket, state, task_request_id, permit))
        .into_response();
    response_with_request_id(response, request_id)
}

async fn ws_echo_task(
    mut socket: WebSocket,
    state: AppState,
    request_id: String,
    _permit: OwnedSemaphorePermit,
) {
    while let Some(result) = socket.next().await {
        let Ok(message) = result else { break };
        match message {
            Message::Text(text) => {
                if text.len() > MAX_WS_MESSAGE_SIZE {
                    let _ = socket
                        .send(Message::Close(Some(CloseFrame {
                            code: 1009,
                            reason: "message too large".into(),
                        })))
                        .await;
                    break;
                }
                state
                    .events
                    .push("websocket", "message_received", "text echo");
                if socket.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
            Message::Binary(bytes) => {
                if bytes.len() > MAX_WS_MESSAGE_SIZE {
                    let _ = socket
                        .send(Message::Close(Some(CloseFrame {
                            code: 1009,
                            reason: "message too large".into(),
                        })))
                        .await;
                    break;
                }
                state
                    .events
                    .push("websocket", "message_received", "binary echo");
                if socket.send(Message::Binary(bytes)).await.is_err() {
                    break;
                }
            }
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }
    state.events.push(
        "websocket",
        "connection_closed",
        format!("echo {request_id}"),
    );
}

pub(crate) async fn ws_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let request_id = request_id(&headers, &state);
    if name.is_empty() || name.len() > MAX_WS_ROOM_NAME_LEN {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "room name must contain 1-128 bytes"}),
        );
    }
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let sender = {
        let mut rooms = state
            .rooms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !rooms.contains_key(&name) && rooms.len() >= MAX_WS_ROOMS {
            None
        } else {
            Some(
                rooms
                    .entry(name.clone())
                    .or_insert_with(|| broadcast::channel(64).0)
                    .clone(),
            )
        }
    };
    let Some(sender) = sender else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "room limit reached"}),
        );
    };
    let receiver = sender.subscribe();
    let task_request_id = request_id.clone();
    let response = upgrade
        .on_upgrade(move |socket| {
            ws_room_task(
                socket,
                state,
                name,
                sender,
                receiver,
                task_request_id,
                permit,
            )
        })
        .into_response();
    response_with_request_id(response, request_id)
}

async fn ws_room_task(
    mut socket: WebSocket,
    state: AppState,
    name: String,
    sender: broadcast::Sender<WsPayload>,
    mut receiver: broadcast::Receiver<WsPayload>,
    request_id: String,
    _permit: OwnedSemaphorePermit,
) {
    loop {
        tokio::select! {
            incoming = socket.next() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) if text.len() <= MAX_WS_MESSAGE_SIZE => {
                        let _ = sender.send(WsPayload { binary: false, data: text.to_string().into_bytes() });
                    }
                    Message::Binary(bytes) if bytes.len() <= MAX_WS_MESSAGE_SIZE => {
                        let _ = sender.send(WsPayload { binary: true, data: bytes.to_vec() });
                    }
                    Message::Text(_) | Message::Binary(_) => {
                        let _ = socket.send(Message::Close(Some(CloseFrame { code: 1009, reason: "message too large".into() }))).await;
                        break;
                    }
                    Message::Ping(bytes) => {
                        if socket.send(Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                }
            }
            incoming = receiver.recv() => {
                let Ok(payload) = incoming else {
                    let _ = socket.send(Message::Close(Some(CloseFrame { code: 1013, reason: "room overloaded".into() }))).await;
                    break;
                };
                let message = if payload.binary {
                    Message::Binary(payload.data.into())
                } else {
                    Message::Text(String::from_utf8_lossy(&payload.data).to_string().into())
                };
                if socket.send(message).await.is_err() { break; }
                state.events.push("websocket", "room_message", format!("room={name}"));
            }
        }
    }
    state.events.push(
        "websocket",
        "connection_closed",
        format!("room={name} {request_id}"),
    );
    drop(receiver);
    if sender.receiver_count() == 0 {
        state
            .rooms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&name);
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct TickerQuery {
    interval_ms: Option<u64>,
    count: Option<u64>,
}

pub(crate) async fn ws_ticker(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TickerQuery>,
    upgrade: WebSocketUpgrade,
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
    let task_request_id = request_id.clone();
    let response = upgrade
        .on_upgrade(move |socket| {
            ws_ticker_task(socket, state, task_request_id, interval_ms, count, permit)
        })
        .into_response();
    response_with_request_id(response, request_id)
}

async fn ws_ticker_task(
    mut socket: WebSocket,
    state: AppState,
    request_id: String,
    interval_ms: u64,
    count: u64,
    _permit: OwnedSemaphorePermit,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    interval.tick().await;
    for index in 1..=count {
        interval.tick().await;
        let payload = json!({"sequence": index, "request_id": request_id});
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
        state
            .events
            .push("websocket", "ticker", format!("sequence={index}"));
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct CloseQuery {
    code: Option<u16>,
    after: Option<u64>,
}

pub(crate) async fn ws_close(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CloseQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let code = query.code.unwrap_or(1000);
    let code = if (1000..=1015).contains(&code) {
        code
    } else {
        1000
    };
    let after_ms = query.after.unwrap_or(0).min(60_000);
    let task_request_id = request_id.clone();
    let response = upgrade
        .on_upgrade(move |mut socket| async move {
            let _permit = permit;
            tokio::time::sleep(std::time::Duration::from_millis(after_ms)).await;
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code,
                    reason: format!("biubin close {task_request_id}").into(),
                })))
                .await;
            state
                .events
                .push("websocket", "connection_closed", "predictable close");
        })
        .into_response();
    response_with_request_id(response, request_id)
}
