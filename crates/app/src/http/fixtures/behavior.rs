use crate::http::{
    middleware::request_id,
    response::{json_response, response_with_request_id},
};
use crate::state::AppState;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use std::net::IpAddr;
use url::Url;

use super::support::{
    cookie_values, forbidden_response_header, query_pairs, query_values, strong_etag_matches,
    valid_cookie_piece, weak_etag_matches,
};

const MAX_REDIRECT_URL_LEN: usize = 2048;
const CACHE_ETAG: &str = "\"biubin-cache-v1\"";
const CACHE_LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

pub(crate) async fn http_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(code) = code.parse::<u16>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid status code"}),
        );
    };
    let Ok(status) = StatusCode::from_u16(code) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid status code"}),
        );
    };
    state
        .events
        .push("http", "response", format!("status {code}"));
    json_response(status, request_id, json!({"status": code}))
}

pub(crate) async fn http_delay(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(seconds): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(seconds) = seconds.parse::<u64>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid delay"}),
        );
    };
    let seconds = seconds.min(30);
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    state
        .events
        .push("http", "response", format!("delay {seconds}s"));
    json_response(
        StatusCode::OK,
        request_id,
        json!({"delay_seconds": seconds}),
    )
}

pub(crate) async fn http_redirect(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(count): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(count) = count.parse::<u16>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid redirect count"}),
        );
    };
    let count = count.min(20);
    if count == 0 {
        return json_response(StatusCode::OK, request_id, json!({"redirects": 0}));
    }
    let location = format!("/redirect/{}", count - 1);
    let mut response = StatusCode::FOUND.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}
pub(crate) async fn http_response_headers(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: OriginalUri,
) -> Response {
    let current_request_id = request_id(&headers, &state);
    let mut response = json_response(
        StatusCode::OK,
        current_request_id.clone(),
        json!({"headers": query_values(&uri.0)}),
    );
    for (name, value) in query_pairs(&uri.0) {
        if forbidden_response_header(&name) {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": format!("header {name:?} cannot be set")}),
            );
        }
        let Ok(name) = header::HeaderName::from_bytes(name.as_bytes()) else {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": "invalid response header name"}),
            );
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return json_response(
                StatusCode::BAD_REQUEST,
                current_request_id.clone(),
                json!({"error": "invalid response header value"}),
            );
        };
        response.headers_mut().append(name, value);
    }
    response
}

pub(crate) async fn http_cookies(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    json_response(
        StatusCode::OK,
        request_id,
        json!({"cookies": cookie_values(&headers)}),
    )
}

pub(crate) async fn http_cookies_set(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, value)): Path<(String, String)>,
) -> Response {
    let request_id = request_id(&headers, &state);
    if !valid_cookie_piece(&name) || !valid_cookie_piece(&value) {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cookie name or value"}),
        );
    }
    cookie_redirect(request_id, &name, &format!("{name}={value}; Path=/"))
}

pub(crate) async fn http_cookies_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    if !valid_cookie_piece(&name) {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cookie name"}),
        );
    }
    cookie_redirect(request_id, &name, &format!("{name}=; Max-Age=0; Path=/"))
}

fn cookie_redirect(request_id: String, _name: &str, set_cookie: &str) -> Response {
    let mut response = StatusCode::FOUND.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("/cookies"));
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(set_cookie).expect("validated cookie is a valid header"),
    );
    response_with_request_id(response, request_id)
}

pub(crate) async fn http_cache(State(state): State<AppState>, headers: HeaderMap) -> Response {
    cache_response(state, headers, None)
}

pub(crate) async fn http_cache_with_max_age(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(seconds): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(seconds) = seconds.parse::<u64>() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid cache max-age"}),
        );
    };
    cache_response(state, headers, Some(seconds.min(86_400)))
}

fn cache_response(state: AppState, headers: HeaderMap, max_age: Option<u64>) -> Response {
    let request_id = request_id(&headers, &state);
    let not_modified = if let Some(if_none_match) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        // RFC 9110 gives If-None-Match precedence over If-Modified-Since.
        weak_etag_matches(if_none_match, CACHE_ETAG)
    } else {
        headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .zip(httpdate::parse_http_date(CACHE_LAST_MODIFIED).ok())
            .is_some_and(|(if_modified_since, last_modified)| last_modified <= if_modified_since)
    };
    let mut response = if not_modified {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        json_response(StatusCode::OK, request_id.clone(), json!({"cache": true}))
    };
    response
        .headers_mut()
        .insert(header::ETAG, HeaderValue::from_static(CACHE_ETAG));
    response.headers_mut().insert(
        header::LAST_MODIFIED,
        HeaderValue::from_static(CACHE_LAST_MODIFIED),
    );
    if let Some(max_age) = max_age {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_str(&format!("public, max-age={max_age}"))
                .expect("numeric cache control is valid"),
        );
    }
    if !response.headers().contains_key("x-request-id") {
        response
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    }
    response
}

pub(crate) async fn http_etag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(value): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let etag = format!("\"{}\"", value.replace('"', ""));
    if let Some(if_match) = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        && !strong_etag_matches(if_match, &etag)
    {
        let mut response = StatusCode::PRECONDITION_FAILED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
        );
        return response_with_request_id(response, request_id);
    }
    if let Some(if_none_match) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        && weak_etag_matches(if_none_match, &etag)
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
        );
        return response_with_request_id(response, request_id);
    }
    let mut response = json_response(StatusCode::OK, request_id, json!({"etag": value}));
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("sanitized etag is valid"),
    );
    response
}

#[derive(Debug, Deserialize)]
pub(crate) struct RedirectToQuery {
    url: Option<String>,
    status_code: Option<u16>,
}

pub(crate) async fn http_redirect_to(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RedirectToQuery>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Some(url) = query.url.filter(|url| !url.is_empty()) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "url query parameter is required"}),
        );
    };
    let status = match query.status_code.unwrap_or(302) {
        301 => StatusCode::MOVED_PERMANENTLY,
        302 => StatusCode::FOUND,
        303 => StatusCode::SEE_OTHER,
        307 => StatusCode::TEMPORARY_REDIRECT,
        308 => StatusCode::PERMANENT_REDIRECT,
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                request_id,
                json!({"error": "status_code must be 301, 302, 303, 307 or 308"}),
            );
        }
    };
    if let Some(error) = validate_redirect_target(
        &url,
        state.config.http_allow_external_redirects,
        &state.config.http_external_redirect_hosts,
    ) {
        return json_response(StatusCode::BAD_REQUEST, request_id, json!({"error": error}));
    }
    redirect_response(status, request_id, &url)
}

fn validate_redirect_target(
    url: &str,
    allow_external: bool,
    allowed_hosts: &[String],
) -> Option<&'static str> {
    if url.len() > MAX_REDIRECT_URL_LEN {
        return Some("redirect URL is too long");
    }
    if url.contains('\r') || url.contains('\n') || url.contains('\\') {
        return Some("url contains invalid characters");
    }

    if let Ok(parsed) = Url::parse(url) {
        if !allow_external {
            return Some("absolute external redirects are disabled");
        }
        if !matches!(parsed.scheme(), "http" | "https") {
            return Some("redirect URL scheme must be http or https");
        }
        let Some(host) = parsed.host_str() else {
            return Some("redirect URL must include a host");
        };
        if host.eq_ignore_ascii_case("localhost")
            || host.to_ascii_lowercase().ends_with(".localhost")
            || host.parse::<IpAddr>().is_ok_and(is_restricted_ip)
        {
            return Some("redirect URL points to a local address");
        }
        if host.parse::<IpAddr>().is_err()
            && !allowed_hosts
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(host))
        {
            return Some("redirect URL host is not allowlisted");
        }
        return None;
    }

    let base = Url::parse("http://biubin.invalid").expect("static redirect base is valid");
    let Ok(resolved) = base.join(url) else {
        return Some("invalid redirect URL");
    };
    if resolved.host_str() != base.host_str() {
        return Some("external redirects require an absolute http(s) URL");
    }
    None
}

fn is_restricted_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_loopback()
                || address.is_private()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_multicast()
        }
        IpAddr::V6(address) => {
            if address.is_loopback()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                || address.is_unspecified()
                || address.is_multicast()
            {
                return true;
            }
            address
                .to_ipv4_mapped()
                .or_else(|| address.to_ipv4())
                .is_some_and(|address| is_restricted_ip(IpAddr::V4(address)))
        }
    }
}

/*
    The target has been validated above, but keep HeaderValue validation as the
    final guard before putting user input into Location.
*/
fn redirect_response(status: StatusCode, request_id: String, url: &str) -> Response {
    let Ok(location) = HeaderValue::from_str(url) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            request_id,
            json!({"error": "invalid redirect URL"}),
        );
    };
    let mut response = status.into_response();
    response.headers_mut().insert(header::LOCATION, location);
    response_with_request_id(response, request_id)
}
