use crate::http::{middleware::request_id, response::json_response};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use base64::Engine;
use serde::Deserialize;
use serde_json::json;

pub(crate) async fn http_basic_auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((user, password)): Path<(String, String)>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
        .is_some_and(|value| value == format!("{user}:{password}"));
    if !valid {
        let mut response = json_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            json!({"authenticated": false}),
        );
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=biubin"),
        );
        return response;
    }
    json_response(
        StatusCode::OK,
        request_id,
        json!({"authenticated": true, "user": user}),
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct BearerQuery {
    token: Option<String>,
}

pub(crate) async fn http_bearer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BearerQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let expected = query.token.unwrap_or_else(|| "biubin".to_owned());
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if supplied != Some(expected.as_str()) {
        let mut response = json_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            json!({"authenticated": false}),
        );
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    json_response(
        StatusCode::OK,
        request_id,
        json!({"authenticated": true, "token": expected}),
    )
}
