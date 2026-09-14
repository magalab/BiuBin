use crate::generated::OPENAPI_HTML;
use crate::http::{
    middleware::request_id,
    response::{json_response, text_response},
};
use crate::state::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use serde_json::{Map, Value, json};

const ANY: &[&str] = &["*"];
const GET: &[&str] = &["GET", "HEAD"];
const POST: &[&str] = &["POST"];
const PUT: &[&str] = &["PUT"];
const PATCH: &[&str] = &["PATCH"];
const DELETE: &[&str] = &["DELETE"];

const TAG_REQUEST_FIXTURES: &str = "Request fixtures";
const TAG_MEDIA: &str = "Media";
const TAG_STATE_CACHING: &str = "State and caching";
const TAG_AUTHENTICATION: &str = "Authentication";
const TAG_HTTP_BEHAVIOR: &str = "HTTP behavior";
const TAG_REPRESENTATIONS: &str = "Representations";
const TAG_STREAMING_BYTES: &str = "Streaming and bytes";
const TAG_FAILURE_SIMULATION: &str = "Failure simulation";

const HTTP_TAGS: &[(&str, &str)] = &[
    (
        TAG_REQUEST_FIXTURES,
        "Request echo and basic HTTP request fixtures.",
    ),
    (TAG_MEDIA, "Image, video, and audio response fixtures."),
    (
        TAG_STATE_CACHING,
        "Cookies, cache validators, and ETag behavior.",
    ),
    (
        TAG_AUTHENTICATION,
        "Basic and bearer authentication fixtures.",
    ),
    (
        TAG_HTTP_BEHAVIOR,
        "Redirects and response header manipulation.",
    ),
    (
        TAG_REPRESENTATIONS,
        "Common JSON, HTML, XML, and UTF-8 representations.",
    ),
    (
        TAG_STREAMING_BYTES,
        "Byte ranges, streaming responses, and throttled output.",
    ),
    (
        TAG_FAILURE_SIMULATION,
        "Deterministic failure and instability simulation.",
    ),
];

#[derive(Clone, Copy)]
struct QueryParameter {
    name: &'static str,
    description: &'static str,
}

#[derive(Clone, Copy)]
struct HttpEndpoint {
    capability_path: &'static str,
    openapi_path: &'static str,
    methods: &'static [&'static str],
    summary: &'static str,
    response_content_type: &'static str,
    query_parameters: &'static [QueryParameter],
}

const NO_QUERY: &[QueryParameter] = &[];

const HTTP_ENDPOINTS: &[HttpEndpoint] = &[
    HttpEndpoint {
        capability_path: "/get",
        openapi_path: "/get",
        methods: GET,
        summary: "Echo a GET request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/post",
        openapi_path: "/post",
        methods: POST,
        summary: "Echo a POST request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/put",
        openapi_path: "/put",
        methods: PUT,
        summary: "Echo a PUT request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/patch",
        openapi_path: "/patch",
        methods: PATCH,
        summary: "Echo a PATCH request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/delete",
        openapi_path: "/delete",
        methods: DELETE,
        summary: "Echo a DELETE request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/anything",
        openapi_path: "/anything",
        methods: ANY,
        summary: "Echo any HTTP request",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/anything/",
        openapi_path: "/anything/",
        methods: ANY,
        summary: "Echo any HTTP request at the root trailing-slash path",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/anything/*",
        openapi_path: "/anything/{path}",
        methods: ANY,
        summary: "Echo any HTTP request at a nested path",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/status/{code}",
        openapi_path: "/status/{code}",
        methods: ANY,
        summary: "Return the requested HTTP status code",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/delay/{seconds}",
        openapi_path: "/delay/{seconds}",
        methods: ANY,
        summary: "Delay the response",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/redirect/{count}",
        openapi_path: "/redirect/{count}",
        methods: ANY,
        summary: "Return a fixed number of relative redirects",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/bytes/{count}",
        openapi_path: "/bytes/{count}",
        methods: GET,
        summary: "Return deterministic binary bytes",
        response_content_type: "application/octet-stream",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/stream-bytes/{count}",
        openapi_path: "/stream-bytes/{count}",
        methods: GET,
        summary: "Stream deterministic binary bytes",
        response_content_type: "application/octet-stream",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/gzip",
        openapi_path: "/gzip",
        methods: ANY,
        summary: "Return a gzip-encoded response",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/deflate",
        openapi_path: "/deflate",
        methods: ANY,
        summary: "Return a deflate-encoded response",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/basic-auth/{user}/{password}",
        openapi_path: "/basic-auth/{user}/{password}",
        methods: ANY,
        summary: "Challenge and validate HTTP Basic Auth",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/headers",
        openapi_path: "/headers",
        methods: ANY,
        summary: "Return request headers",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/ip",
        openapi_path: "/ip",
        methods: ANY,
        summary: "Return the connected client IP",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/user-agent",
        openapi_path: "/user-agent",
        methods: ANY,
        summary: "Return the User-Agent header",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/image",
        openapi_path: "/image",
        methods: GET,
        summary: "Return the default PNG fixture",
        response_content_type: "image/png",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/image/png",
        openapi_path: "/image/png",
        methods: GET,
        summary: "Return a PNG fixture",
        response_content_type: "image/png",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/image/jpeg",
        openapi_path: "/image/jpeg",
        methods: GET,
        summary: "Return a JPEG fixture",
        response_content_type: "image/jpeg",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/image/svg",
        openapi_path: "/image/svg",
        methods: GET,
        summary: "Return an SVG fixture",
        response_content_type: "image/svg+xml",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/image/webp",
        openapi_path: "/image/webp",
        methods: GET,
        summary: "Return a WebP fixture",
        response_content_type: "image/webp",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/video",
        openapi_path: "/video",
        methods: GET,
        summary: "Return the default MP4 fixture",
        response_content_type: "video/mp4",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/video/mp4",
        openapi_path: "/video/mp4",
        methods: GET,
        summary: "Return an MP4 fixture",
        response_content_type: "video/mp4",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/video/webm",
        openapi_path: "/video/webm",
        methods: GET,
        summary: "Return a WebM fixture",
        response_content_type: "video/webm",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/audio",
        openapi_path: "/audio",
        methods: GET,
        summary: "Return the default WAV fixture",
        response_content_type: "audio/wav",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/audio/wav",
        openapi_path: "/audio/wav",
        methods: GET,
        summary: "Return a WAV audio fixture",
        response_content_type: "audio/wav",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/audio/mp3",
        openapi_path: "/audio/mp3",
        methods: GET,
        summary: "Return an MP3 audio fixture",
        response_content_type: "audio/mpeg",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/response-headers",
        openapi_path: "/response-headers",
        methods: GET,
        summary: "Set safe response headers from query parameters",
        response_content_type: "application/json",
        query_parameters: &[QueryParameter {
            name: "X-Test",
            description: "Any additional query key becomes a response header",
        }],
    },
    HttpEndpoint {
        capability_path: "/cookies",
        openapi_path: "/cookies",
        methods: GET,
        summary: "Return cookies sent by the client",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/cookies/set/{name}/{value}",
        openapi_path: "/cookies/set/{name}/{value}",
        methods: GET,
        summary: "Set a cookie and redirect to /cookies",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/cookies/delete/{name}",
        openapi_path: "/cookies/delete/{name}",
        methods: GET,
        summary: "Delete a cookie and redirect to /cookies",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/cache",
        openapi_path: "/cache",
        methods: GET,
        summary: "Return a cacheable response",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/cache/{seconds}",
        openapi_path: "/cache/{seconds}",
        methods: GET,
        summary: "Return a response with Cache-Control max-age",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/etag/{value}",
        openapi_path: "/etag/{value}",
        methods: GET,
        summary: "Return an ETag-aware response",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/redirect-to",
        openapi_path: "/redirect-to",
        methods: GET,
        summary: "Redirect to a requested URL",
        response_content_type: "application/json",
        query_parameters: &[
            QueryParameter {
                name: "url",
                description: "Relative target URL; absolute HTTP(S) targets require BIUBIN_HTTP_ALLOW_EXTERNAL_REDIRECTS=true",
            },
            QueryParameter {
                name: "status_code",
                description: "One of 301, 302, 303, 307 or 308",
            },
        ],
    },
    HttpEndpoint {
        capability_path: "/json",
        openapi_path: "/json",
        methods: GET,
        summary: "Return a JSON fixture",
        response_content_type: "application/json",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/html",
        openapi_path: "/html",
        methods: GET,
        summary: "Return an HTML fixture",
        response_content_type: "text/html",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/xml",
        openapi_path: "/xml",
        methods: GET,
        summary: "Return an XML fixture",
        response_content_type: "application/xml",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/encoding/utf8",
        openapi_path: "/encoding/utf8",
        methods: GET,
        summary: "Return UTF-8 text",
        response_content_type: "text/plain; charset=utf-8",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/range/{count}",
        openapi_path: "/range/{count}",
        methods: GET,
        summary: "Return a Range-aware deterministic byte resource",
        response_content_type: "application/octet-stream",
        query_parameters: NO_QUERY,
    },
    HttpEndpoint {
        capability_path: "/drip",
        openapi_path: "/drip",
        methods: GET,
        summary: "Stream bytes over a configurable duration",
        response_content_type: "application/octet-stream",
        query_parameters: &[
            QueryParameter {
                name: "numbytes",
                description: "Number of bytes",
            },
            QueryParameter {
                name: "duration",
                description: "Streaming duration in seconds",
            },
            QueryParameter {
                name: "delay",
                description: "Initial delay in seconds",
            },
            QueryParameter {
                name: "chunk_size",
                description: "Bytes per emitted chunk when duration is positive; zero-duration responses are coalesced",
            },
            QueryParameter {
                name: "code",
                description: "Final HTTP status code",
            },
        ],
    },
    HttpEndpoint {
        capability_path: "/unstable",
        openapi_path: "/unstable",
        methods: GET,
        summary: "Return a deterministic success or failure",
        response_content_type: "application/json",
        query_parameters: &[
            QueryParameter {
                name: "failure_rate",
                description: "Failure probability from 0 to 1",
            },
            QueryParameter {
                name: "seed",
                description: "Deterministic decision seed",
            },
        ],
    },
    HttpEndpoint {
        capability_path: "/bearer",
        openapi_path: "/bearer",
        methods: GET,
        summary: "Validate a Bearer token",
        response_content_type: "application/json",
        query_parameters: &[QueryParameter {
            name: "token",
            description: "Expected token; defaults to biubin",
        }],
    },
];

pub(crate) fn http_capabilities() -> Vec<Value> {
    HTTP_ENDPOINTS
        .iter()
        .map(|endpoint| {
            let mut value = json!({
                "protocol": "http",
                "methods": endpoint.methods,
                "path": endpoint.capability_path,
            });
            if endpoint.methods == ANY {
                value["x-biubin-methods"] = json!(["*"]);
            }
            value
        })
        .collect()
}

pub(crate) fn openapi_document() -> Value {
    let mut paths = Map::new();
    for endpoint in HTTP_ENDPOINTS {
        let path_item = paths
            .entry(endpoint.openapi_path.to_owned())
            .or_insert_with(|| json!({}));
        let path_item = path_item.as_object_mut().expect("path item is an object");
        if endpoint.methods == ANY {
            path_item.insert("x-biubin-methods".to_owned(), json!(["*"]));
        }
        for method in openapi_methods(endpoint.methods) {
            path_item.insert(method.to_ascii_lowercase(), operation(endpoint, method));
        }
    }
    let tags = HTTP_TAGS
        .iter()
        .map(|(name, description)| json!({"name": name, "description": description}))
        .collect::<Vec<_>>();
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "BiuBin HTTP fixtures",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Deterministic HTTP request and response fixtures for local development and integration tests."
        },
        "servers": [{"url": "/"}],
        "tags": tags,
        "components": {
            "securitySchemes": {
                "basicAuth": {"type": "http", "scheme": "basic"},
                "bearerAuth": {"type": "http", "scheme": "bearer"}
            }
        },
        "paths": paths,
    })
}

fn openapi_methods<'a>(methods: &'a [&'a str]) -> Vec<&'a str> {
    if methods == ANY {
        vec!["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
    } else {
        methods.to_vec()
    }
}

fn operation(endpoint: &HttpEndpoint, method: &str) -> Value {
    let mut parameters = Vec::new();
    for name in path_parameters(endpoint.openapi_path) {
        parameters.push(json!({
            "name": name,
            "in": "path",
            "required": true,
            "schema": {"type": parameter_type(name)},
        }));
    }
    for parameter in endpoint.query_parameters {
        parameters.push(json!({
            "name": parameter.name,
            "in": "query",
            "required": false,
            "description": parameter.description,
            "schema": query_parameter_schema(parameter.name),
        }));
    }
    let content_type = media_type(endpoint.response_content_type);
    let responses = response_definitions(endpoint.openapi_path, content_type);
    let mut operation = json!({
        "tags": [tag_for_path(endpoint.openapi_path)],
        "summary": endpoint.summary,
        "operationId": operation_id(endpoint.openapi_path, method),
        "responses": responses,
    });
    if !parameters.is_empty() {
        operation["parameters"] = Value::Array(parameters);
    }
    if let Some(description) = endpoint_description(endpoint.openapi_path) {
        operation["description"] = json!(description);
    }
    if matches!(method, "POST" | "PUT" | "PATCH") && supports_structured_body(endpoint.openapi_path)
    {
        operation["requestBody"] = json!({
            "required": false,
            "content": {
                "application/json": {
                    "schema": {"type": "object", "additionalProperties": true},
                    "example": {"hello": "world"}
                },
                "application/x-www-form-urlencoded": {
                    "schema": {"type": "object", "additionalProperties": {"type": "string"}}
                },
                "multipart/form-data": {
                    "schema": {"type": "object", "additionalProperties": {"type": "string"}}
                }
            }
        });
    }
    if endpoint.openapi_path.starts_with("/basic-auth/") {
        operation["security"] = json!([{"basicAuth": []}]);
    } else if endpoint.openapi_path == "/bearer" {
        operation["security"] = json!([{"bearerAuth": []}]);
    }
    operation
}

fn response_definitions(path: &str, content_type: &str) -> Map<String, Value> {
    let mut responses = Map::new();
    responses.insert(
        "200".to_owned(),
        documented_response("Successful fixture response", Some(content_type)),
    );

    match path {
        "/status/{code}" => insert_response(
            &mut responses,
            "400",
            "The status code is missing or invalid.",
            None,
        ),
        "/delay/{seconds}" => insert_response(&mut responses, "400", "The delay is invalid.", None),
        "/redirect/{count}" => {
            insert_response(
                &mut responses,
                "302",
                "A relative redirect to the next count.",
                None,
            );
            insert_response(
                &mut responses,
                "400",
                "The redirect count is invalid.",
                None,
            );
        }
        "/redirect-to" => {
            for status in ["301", "302", "303", "307", "308"] {
                insert_response(
                    &mut responses,
                    status,
                    "Redirect to the validated target URL.",
                    None,
                );
            }
            insert_response(
                &mut responses,
                "400",
                "The target URL or status code is invalid.",
                None,
            );
        }
        "/bytes/{count}" => {
            insert_response(
                &mut responses,
                "206",
                "A partial byte range response.",
                Some(content_type),
            );
            insert_response(&mut responses, "400", "The byte count is invalid.", None);
            insert_response(
                &mut responses,
                "413",
                "The byte count exceeds the configured response limit.",
                None,
            );
            insert_response(
                &mut responses,
                "416",
                "The requested byte range is not satisfiable.",
                None,
            );
        }
        "/stream-bytes/{count}" => {
            insert_response(&mut responses, "400", "The byte count is invalid.", None);
            insert_response(
                &mut responses,
                "413",
                "The byte count exceeds the configured response limit.",
                None,
            );
        }
        "/range/{count}" => {
            insert_response(
                &mut responses,
                "206",
                "A partial byte range response.",
                Some(content_type),
            );
            insert_response(&mut responses, "400", "The byte count is invalid.", None);
            insert_response(
                &mut responses,
                "413",
                "The byte count exceeds the configured response limit.",
                None,
            );
            insert_response(
                &mut responses,
                "416",
                "The requested byte range is not satisfiable.",
                None,
            );
        }
        path if path.starts_with("/image/")
            || path.starts_with("/video/")
            || path.starts_with("/audio/")
            || matches!(path, "/image" | "/video" | "/audio") =>
        {
            insert_response(
                &mut responses,
                "206",
                "A partial media byte range response.",
                Some(content_type),
            );
            insert_response(
                &mut responses,
                "404",
                "The requested media format is not supported.",
                None,
            );
            insert_response(
                &mut responses,
                "416",
                "The requested byte range is not satisfiable.",
                None,
            );
        }
        "/basic-auth/{user}/{password}" => {
            insert_response(
                &mut responses,
                "401",
                "The credentials are missing or invalid.",
                None,
            );
        }
        "/response-headers" => insert_response(
            &mut responses,
            "400",
            "A requested response header is not allowed.",
            None,
        ),
        "/cookies/set/{name}/{value}" | "/cookies/delete/{name}" => {
            insert_response(
                &mut responses,
                "302",
                "Redirect to the cookie inspection endpoint.",
                None,
            );
            insert_response(
                &mut responses,
                "400",
                "The cookie name or value is invalid.",
                None,
            );
        }
        "/cache" => insert_response(
            &mut responses,
            "304",
            "The cached representation is still fresh.",
            None,
        ),
        "/cache/{seconds}" => {
            insert_response(
                &mut responses,
                "304",
                "The cached representation is still fresh.",
                None,
            );
            insert_response(&mut responses, "400", "The cache max-age is invalid.", None);
        }
        "/etag/{value}" => {
            insert_response(
                &mut responses,
                "304",
                "The representation matches If-None-Match.",
                None,
            );
            insert_response(
                &mut responses,
                "412",
                "The representation does not satisfy If-Match.",
                None,
            );
        }
        "/drip" => insert_response(
            &mut responses,
            "400",
            "One or more drip parameters are invalid.",
            None,
        ),
        "/unstable" => {
            insert_response(&mut responses, "400", "The failure rate is invalid.", None);
            insert_response(
                &mut responses,
                "503",
                "The deterministic failure decision failed the request.",
                None,
            );
        }
        "/bearer" => insert_response(
            &mut responses,
            "401",
            "The Bearer token is missing or invalid.",
            None,
        ),
        _ => {}
    }

    responses.insert(
        "default".to_owned(),
        documented_response("Other fixture error response", None),
    );
    responses
}

fn insert_response(
    responses: &mut Map<String, Value>,
    status: &str,
    description: &str,
    content_type: Option<&str>,
) {
    responses.insert(
        status.to_owned(),
        documented_response(description, content_type),
    );
}

fn documented_response(description: &str, content_type: Option<&str>) -> Value {
    let mut response = json!({"description": description});
    if let Some(content_type) = content_type {
        let mut content = Map::new();
        content.insert(content_type.to_owned(), json!({}));
        response["content"] = Value::Object(content);
    }
    response
}

fn media_type(content_type: &str) -> &str {
    content_type
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or(content_type)
}

fn query_parameter_schema(name: &str) -> Value {
    match name {
        "status_code" | "code" => json!({
            "type": "integer",
            "minimum": 100,
            "maximum": 999
        }),
        "numbytes" => json!({"type": "integer", "minimum": 0}),
        "duration" | "delay" => json!({"type": "number", "minimum": 0}),
        "chunk_size" => json!({"type": "integer", "minimum": 1}),
        "failure_rate" => json!({
            "type": "number",
            "minimum": 0,
            "maximum": 1
        }),
        _ => json!({"type": "string"}),
    }
}

fn endpoint_description(path: &str) -> Option<&'static str> {
    match path {
        "/bytes/{count}" | "/range/{count}" | "/stream-bytes/{count}" => {
            Some("The byte count is limited by the BIUBIN_MAX_BYTES_RESPONSE configuration value.")
        }
        "/drip" => Some(
            "numbytes is limited by BIUBIN_MAX_BYTES_RESPONSE; duration and delay are clamped to 30 seconds.",
        ),
        "/redirect-to" => Some(
            "Relative targets are accepted by default. Absolute HTTP(S) targets require BIUBIN_HTTP_ALLOW_EXTERNAL_REDIRECTS=true and are subject to local-address and length checks.",
        ),
        _ => None,
    }
}

fn supports_structured_body(path: &str) -> bool {
    matches!(
        path,
        "/post" | "/put" | "/patch" | "/anything" | "/anything/" | "/anything/{path}"
    )
}

fn tag_for_path(path: &str) -> &'static str {
    if path.starts_with("/image") || path.starts_with("/video") || path.starts_with("/audio") {
        TAG_MEDIA
    } else if path.starts_with("/cookies")
        || path.starts_with("/cache")
        || path.starts_with("/etag")
    {
        TAG_STATE_CACHING
    } else if path.starts_with("/basic-auth") || path == "/bearer" {
        TAG_AUTHENTICATION
    } else if path == "/response-headers" || path == "/redirect-to" {
        TAG_HTTP_BEHAVIOR
    } else if path == "/json" || path == "/html" || path == "/xml" || path == "/encoding/utf8" {
        TAG_REPRESENTATIONS
    } else if path == "/drip" || path == "/range/{count}" || path == "/stream-bytes/{count}" {
        TAG_STREAMING_BYTES
    } else if path == "/unstable" {
        TAG_FAILURE_SIMULATION
    } else {
        TAG_REQUEST_FIXTURES
    }
}

fn path_parameters(path: &str) -> Vec<&str> {
    path.split('{')
        .skip(1)
        .filter_map(|part| part.split('}').next())
        .collect()
}

fn parameter_type(name: &str) -> &'static str {
    match name {
        "code" | "count" | "seconds" => "integer",
        _ => "string",
    }
}

fn operation_id(path: &str, method: &str) -> String {
    let trailing_slash = path.len() > 1 && path.ends_with('/');
    let path = path
        .trim_matches('/')
        .replace(['{', '}'], "")
        .replace(['/', '-'], "_");
    let operation_id = format!(
        "{}_{}",
        method.to_ascii_lowercase(),
        if path.is_empty() { "root" } else { &path }
    );
    if trailing_slash {
        format!("{operation_id}_trailing_slash")
    } else {
        operation_id
    }
}

pub(crate) async fn document(State(state): State<AppState>, headers: HeaderMap) -> Response {
    json_response(
        axum::http::StatusCode::OK,
        request_id(&headers, &state),
        openapi_document(),
    )
}

pub(crate) async fn ui(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let page = if OPENAPI_HTML.contains("/assets/") {
        OPENAPI_HTML
    } else {
        OPENAPI_FALLBACK_HTML
    };
    text_response(
        axum::http::StatusCode::OK,
        request_id(&headers, &state),
        page,
        "text/html; charset=utf-8",
    )
}

// Used only when the optional web build has not been run. Normal releases serve
// the Scalar-powered web bundle through OPENAPI_HTML above.
const OPENAPI_FALLBACK_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BiuBin HTTP fixtures</title>
  <style>
    :root { color-scheme: dark; font: 15px/1.5 system-ui, sans-serif; background: #101418; color: #e9eef5; }
    body { margin: 0; padding: 32px; background: radial-gradient(circle at 80% -10%, #26474c 0, transparent 36rem), #101418; }
    main { max-width: 1100px; margin: auto; }
    h1 { letter-spacing: -.04em; }
    p { color: #a9b5bf; }
    a { color: #7ee0c4; }
    .operation { border: 1px solid #2b343c; border-radius: 12px; padding: 16px; margin: 12px 0; background: #171d22; }
    .operation h2 { font: 600 16px ui-monospace, monospace; margin: 0 0 12px; }
    .method { color: #7ee0c4; display: inline-block; min-width: 72px; }
    form { display: grid; gap: 8px; }
    input, textarea, button { font: inherit; border-radius: 7px; border: 1px solid #3a454e; padding: 8px 10px; background: #101418; color: #e9eef5; }
    textarea { min-height: 58px; resize: vertical; }
    button { background: #7ee0c4; color: #10221e; border: 0; font-weight: 700; cursor: pointer; }
    pre { white-space: pre-wrap; overflow: auto; background: #0d1013; border-radius: 7px; padding: 10px; color: #b9d4c8; }
    .muted { color: #84929d; font-size: 13px; }
    .binary { display: block; max-width: 100%; max-height: 260px; margin-top: 8px; }
  </style>
</head>
<body>
<main>
  <h1>BiuBin HTTP fixtures</h1>
  <p>Interactive, offline API documentation. The schema is available at <a href="/openapi.json">/openapi.json</a>.</p>
  <div id="app"><p>Loading OpenAPI document…</p></div>
</main>
<script>
const app = document.querySelector('#app');
const pathDefaults = { code: '200', seconds: '1', count: '16', user: 'alice', password: 'secret', name: 'demo', value: 'value' };
const examplePath = (path) => path.replace(/\{([^}]+)\}/g, (_, name) => pathDefaults[name] || name);
const escapeText = (value) => String(value).replace(/[&<>"']/g, (char) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const renderResult = async (response, target) => {
  const type = response.headers.get('content-type') || '';
  const headers = [...response.headers].map(([key, value]) => `${key}: ${value}`).join('\n');
  target.innerHTML = `<strong>${response.status} ${response.statusText}</strong><pre>${escapeText(headers)}</pre>`;
  if (type.startsWith('image/') || type.startsWith('video/')) {
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const media = type.startsWith('image/') ? `<img class="binary" src="${url}" alt="fixture">` : `<video class="binary" controls src="${url}"></video>`;
    target.insertAdjacentHTML('beforeend', media);
    return;
  }
  target.insertAdjacentText('beforeend', `\n${await response.text()}`);
};
const render = (spec) => {
  app.innerHTML = '';
  for (const [path, item] of Object.entries(spec.paths)) {
    if (path.startsWith('/openapi')) continue;
    for (const [method, operation] of Object.entries(item)) {
      if (method.startsWith('x-')) continue;
      const section = document.createElement('section');
      section.className = 'operation';
      const pathValue = examplePath(path);
      section.innerHTML = `<h2><span class="method">${method.toUpperCase()}</span>${escapeText(path)}</h2><p class="muted">${escapeText(operation.summary || '')}</p><form><input class="path" value="${escapeText(pathValue)}" aria-label="path"><input class="query" placeholder="query string, e.g. ?foo=bar" aria-label="query"><textarea class="body" placeholder="request body (optional)"></textarea><button>send request</button><div class="result"></div></form>`;
      const form = section.querySelector('form');
      const body = section.querySelector('.body');
      if (['get', 'head', 'delete'].includes(method)) body.style.display = 'none';
      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const result = section.querySelector('.result');
        result.textContent = 'Requesting…';
        try {
          const requestBody = body.value;
          const response = await fetch(form.querySelector('.path').value + form.querySelector('.query').value, {
            method: method.toUpperCase(),
            headers: requestBody ? {'content-type': 'application/json'} : undefined,
            body: requestBody || undefined
          });
          await renderResult(response, result);
        } catch (error) {
          result.textContent = error instanceof Error ? error.message : String(error);
        }
      });
      app.appendChild(section);
    }
  }
};
fetch('/openapi.json').then((response) => response.json()).then(render).catch((error) => { app.textContent = error.message; });
</script>
</body>
</html>"##;
