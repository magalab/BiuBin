use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

pub(crate) fn json_response(status: StatusCode, request_id: String, value: Value) -> Response {
    let response = (status, Json(value)).into_response();
    response_with_request_id(response, request_id)
}

pub(crate) fn response_with_request_id(mut response: Response, request_id: String) -> Response {
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) fn text_response(
    status: StatusCode,
    request_id: String,
    body: &str,
    content_type: &'static str,
) -> Response {
    let mut response = (status, body.to_owned()).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

pub(crate) fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "cookie" | "set-cookie"
    )
}
