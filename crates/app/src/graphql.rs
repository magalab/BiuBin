use async_graphql::{
    Context, Error, ErrorExtensions, InputObject, Json, Object, Result, Schema, SimpleObject,
    Subscription,
};
use biubin_core::{Config, Event, EventStore};
use futures_util::Stream;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

pub type BiubinSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(config: Arc<Config>, events: EventStore) -> BiubinSchema {
    let mut schema = Schema::build(QueryRoot, MutationRoot, SubscriptionRoot);
    if !config.graphql_introspection_enabled {
        schema = schema.disable_introspection();
    }
    schema.data(config).data(events).finish()
}

pub fn annotate_errors(response: &mut async_graphql::Response, syntax_valid: bool) {
    for error in &mut response.errors {
        if error
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code"))
            .is_none()
        {
            let code = if syntax_valid {
                "VALIDATION_ERROR"
            } else {
                "SYNTAX_ERROR"
            };
            error
                .extensions
                .get_or_insert_with(Default::default)
                .set("code", code);
        }
    }
}

#[derive(Default)]
pub struct QueryRoot;

#[derive(Default)]
pub struct MutationRoot;

#[derive(Default)]
pub struct SubscriptionRoot;

#[derive(Clone, Default, InputObject)]
pub struct EchoInput {
    pub message: Option<String>,
    pub repeat: Option<i32>,
}

#[derive(Clone, SimpleObject)]
pub struct EchoResult {
    pub message: String,
    pub repeated: Vec<String>,
}

#[derive(Clone, SimpleObject)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
    pub bind_host: String,
    pub advertise_host: String,
    pub http_port: i32,
    pub grpc_h2c_port: i32,
    pub grpc_tls_port: i32,
    pub tcp_port: i32,
    pub udp_port: i32,
    pub mqtt_enabled: bool,
    pub mqtt_tls_enabled: bool,
}

#[derive(Clone, SimpleObject)]
pub struct GraphEvent {
    pub schema_version: i32,
    pub id: String,
    pub seq: i64,
    pub at: String,
    pub protocol: String,
    pub kind: String,
    pub connection_id: Option<String>,
    pub direction: Option<String>,
    pub summary: String,
    pub attributes: Json<Value>,
}

#[derive(Clone, Default, InputObject)]
pub struct PublishEventInput {
    pub protocol: String,
    pub summary: String,
    pub require_auth: Option<bool>,
    pub token: Option<String>,
}

#[derive(Clone, SimpleObject)]
pub struct GraphTick {
    pub sequence: i64,
    pub message: String,
}

#[Object]
impl QueryRoot {
    async fn echo(&self, input: Option<EchoInput>) -> Result<EchoResult> {
        let input = input.unwrap_or_default();
        let message = input.message.unwrap_or_else(|| "biubin echo".to_owned());
        if message == "__resolver_error" {
            return Err(stable_error("RESOLVER_ERROR", "requested resolver error"));
        }
        if message == "__auth_error" {
            return Err(stable_error(
                "UNAUTHENTICATED",
                "authentication required for this resolver example",
            ));
        }
        let repeat = input.repeat.unwrap_or(1).clamp(1, 100) as usize;
        Ok(EchoResult {
            message: message.clone(),
            repeated: (1..=repeat)
                .map(|sequence| format!("{message} #{sequence}"))
                .collect(),
        })
    }

    async fn server_info(&self, ctx: &Context<'_>) -> Result<ServerInfo> {
        let config = ctx.data::<Arc<Config>>()?;
        Ok(ServerInfo {
            name: "biubin".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            bind_host: config.bind_host.clone(),
            advertise_host: config.advertise_host.clone(),
            http_port: i32::from(config.ports.http),
            grpc_h2c_port: i32::from(config.ports.grpc_h2c),
            grpc_tls_port: i32::from(config.ports.grpc_tls),
            tcp_port: i32::from(config.ports.tcp),
            udp_port: i32::from(config.ports.udp),
            mqtt_enabled: config.mqtt_enabled,
            mqtt_tls_enabled: config.mqtt_tls_enabled,
        })
    }

    async fn recent_events(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        protocol: Option<String>,
    ) -> Result<Vec<GraphEvent>> {
        let events = ctx.data::<EventStore>()?;
        let limit = limit.unwrap_or(20).clamp(1, 200) as usize;
        Ok(events
            .list(limit, protocol.as_deref())
            .iter()
            .map(GraphEvent::from_event)
            .collect())
    }
}

#[Object]
impl MutationRoot {
    async fn publish_test_event(
        &self,
        ctx: &Context<'_>,
        input: PublishEventInput,
    ) -> Result<GraphEvent> {
        if input.protocol.trim().is_empty() || input.protocol.len() > 32 {
            return Err(stable_error(
                "INVALID_INPUT",
                "protocol must contain 1-32 characters",
            ));
        }
        if input.summary.len() > 256 {
            return Err(stable_error(
                "INVALID_INPUT",
                "summary must contain at most 256 characters",
            ));
        }
        if input.require_auth.unwrap_or(false) && input.token.as_deref() != Some("biubin-dev") {
            return Err(stable_error(
                "UNAUTHENTICATED",
                "publishTestEvent requires token biubin-dev in this development fixture",
            ));
        }
        let events = ctx.data::<EventStore>()?;
        let event = events.push(&input.protocol, "test_event", input.summary);
        Ok(GraphEvent::from_event(&event))
    }
}

#[Subscription]
impl SubscriptionRoot {
    async fn ticker(
        &self,
        interval_ms: Option<u64>,
        count: Option<u64>,
    ) -> impl Stream<Item = GraphTick> {
        let interval_ms = interval_ms.unwrap_or(1000).clamp(10, 60_000);
        let count = count.unwrap_or(5).min(1000);
        async_stream::stream! {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            interval.tick().await;
            for sequence in 1..=count {
                interval.tick().await;
                yield GraphTick {
                    sequence: sequence as i64,
                    message: "biubin graphql tick".to_owned(),
                };
            }
        }
    }

    async fn events(&self, ctx: &Context<'_>) -> impl Stream<Item = GraphEvent> {
        let events = ctx
            .data::<EventStore>()
            .expect("GraphQL schema always contains EventStore")
            .clone();
        let mut receiver = events.subscribe();
        let initial = events.list(200, None);
        async_stream::stream! {
            let mut cursor = 0;
            for event in initial.into_iter().rev() {
                cursor = event.seq;
                yield GraphEvent::from_event(&event);
            }
            loop {
                match receiver.recv().await {
                    Ok(event) if event.seq > cursor => {
                        cursor = event.seq;
                        yield GraphEvent::from_event(&event);
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let replay = events
                            .list(200, None)
                            .into_iter()
                            .rev()
                            .filter(|event| event.seq > cursor)
                            .collect::<Vec<_>>();
                        for event in replay {
                            cursor = event.seq;
                            yield GraphEvent::from_event(&event);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

impl GraphEvent {
    fn from_event(event: &Event) -> Self {
        Self {
            schema_version: i32::from(event.schema_version),
            id: event.id.clone(),
            seq: event.seq as i64,
            at: event.at.clone(),
            protocol: event.protocol.clone(),
            kind: event.kind.clone(),
            connection_id: event.connection_id.clone(),
            direction: event.direction.clone(),
            summary: event.summary.clone(),
            attributes: Json(json!(event.attributes)),
        }
    }
}

fn stable_error(code: &str, message: &str) -> Error {
    Error::new(message).extend_with(|_, extensions| {
        extensions.set("code", code);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn introspection_can_be_disabled_by_configuration() {
        let config = Arc::new(Config {
            graphql_introspection_enabled: false,
            ..Config::default()
        });
        let schema = build_schema(config, EventStore::new(16));
        let response = schema.execute("{ __schema { queryType { name } } }").await;
        assert_eq!(
            response.data.into_json().unwrap(),
            json!({"__schema": null})
        );
    }

    #[tokio::test]
    async fn error_examples_have_stable_categories() {
        let schema = build_schema(Arc::new(Config::default()), EventStore::new(16));
        let cases = [
            ("{ echo(\"", "SYNTAX_ERROR"),
            ("{ missingField }", "VALIDATION_ERROR"),
            (
                "{ echo(input: { message: \"__resolver_error\" }) { message } }",
                "RESOLVER_ERROR",
            ),
            (
                "mutation { publishTestEvent(input: { protocol: \"http\", summary: \"x\", requireAuth: true, token: \"bad\" }) { id } }",
                "UNAUTHENTICATED",
            ),
        ];
        for (query, expected_code) in cases {
            let syntax_valid = async_graphql::Request::new(query).parsed_query().is_ok();
            let mut response = schema.execute(query).await;
            annotate_errors(&mut response, syntax_valid);
            assert_eq!(
                response.errors[0]
                    .extensions
                    .as_ref()
                    .and_then(|extensions| extensions.get("code")),
                Some(&async_graphql::Value::from(expected_code)),
                "unexpected GraphQL error for {query}"
            );
        }
    }
}
