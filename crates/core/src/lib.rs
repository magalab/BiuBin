use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::broadcast;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ports {
    pub http: u16,
    pub grpc_h2c: u16,
    pub grpc_tls: u16,
    pub tcp: u16,
    pub udp: u16,
    pub mqtt_tcp: u16,
    pub mqtt_auth_tcp: u16,
    pub mqtt_v5: u16,
    pub mqtt_tls: u16,
    #[serde(skip_serializing)]
    pub mqtt_tls_backend: u16,
    pub mqtt_ws: u16,
    pub thrift: u16,
}

impl Default for Ports {
    fn default() -> Self {
        Self {
            http: 8080,
            grpc_h2c: 9000,
            grpc_tls: 9001,
            tcp: 7000,
            udp: 7001,
            mqtt_tcp: 1883,
            mqtt_auth_tcp: 1884,
            mqtt_v5: 1885,
            mqtt_tls: 8883,
            mqtt_tls_backend: 1886,
            mqtt_ws: 8083,
            thrift: 9090,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_host: String,
    pub advertise_host: String,
    pub ports: Ports,
    pub mqtt_enabled: bool,
    pub mqtt_tls_enabled: bool,
    pub mqtt_tls_mode: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub grpc_tls_enabled: bool,
    pub grpc_tls_mode: String,
    pub graphql_introspection_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_client_ca_path: String,
    pub http_body_limit: usize,
    pub max_bytes_response: usize,
    pub event_capacity: usize,
    pub tcp_delay_ms: u64,
    pub tcp_close_after: usize,
    pub tcp_read_limit: usize,
    pub udp_delay_ms: u64,
    pub udp_drop_percent: u8,
    pub max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_owned(),
            advertise_host: "127.0.0.1".to_owned(),
            ports: Ports::default(),
            mqtt_enabled: false,
            mqtt_tls_enabled: false,
            mqtt_tls_mode: "tls".to_owned(),
            mqtt_username: "biubin".to_owned(),
            mqtt_password: "biubin-dev".to_owned(),
            grpc_tls_enabled: false,
            grpc_tls_mode: "tls".to_owned(),
            graphql_introspection_enabled: true,
            tls_cert_path: "certs/dev/server.pem".to_owned(),
            tls_key_path: "certs/dev/server-key.pem".to_owned(),
            tls_client_ca_path: "certs/dev/ca.pem".to_owned(),
            http_body_limit: 2 * 1024 * 1024,
            max_bytes_response: 10 * 1024 * 1024,
            event_capacity: 200,
            tcp_delay_ms: 0,
            tcp_close_after: 0,
            tcp_read_limit: 4 * 1024 * 1024,
            udp_delay_ms: 0,
            udp_drop_percent: 0,
            max_connections: 1000,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        config.bind_host = env_or("BIUBIN_BIND_HOST", config.bind_host);
        config.advertise_host = env_or("BIUBIN_ADVERTISE_HOST", config.advertise_host);
        config.mqtt_enabled = parse_bool_env("BIUBIN_MQTT_ENABLED", config.mqtt_enabled)?;
        config.mqtt_tls_enabled =
            parse_bool_env("BIUBIN_MQTT_TLS_ENABLED", config.mqtt_tls_enabled)?;
        config.mqtt_tls_mode = env_or("BIUBIN_MQTT_TLS_MODE", config.mqtt_tls_mode);
        config.mqtt_username = env_or("BIUBIN_MQTT_USERNAME", config.mqtt_username);
        config.mqtt_password = env_or("BIUBIN_MQTT_PASSWORD", config.mqtt_password);
        config.grpc_tls_enabled =
            parse_bool_env("BIUBIN_GRPC_TLS_ENABLED", config.grpc_tls_enabled)?;
        config.grpc_tls_mode = env_or("BIUBIN_GRPC_TLS_MODE", config.grpc_tls_mode);
        config.graphql_introspection_enabled = parse_bool_env(
            "BIUBIN_GRAPHQL_INTROSPECTION_ENABLED",
            config.graphql_introspection_enabled,
        )?;
        config.tls_cert_path = env_or("BIUBIN_TLS_CERT_PATH", config.tls_cert_path);
        config.tls_key_path = env_or("BIUBIN_TLS_KEY_PATH", config.tls_key_path);
        config.tls_client_ca_path = env_or("BIUBIN_TLS_CLIENT_CA_PATH", config.tls_client_ca_path);
        config.ports.http = parse_env("BIUBIN_HTTP_PORT", config.ports.http)?;
        config.ports.grpc_h2c = parse_env("BIUBIN_GRPC_H2C_PORT", config.ports.grpc_h2c)?;
        config.ports.grpc_tls = parse_env("BIUBIN_GRPC_TLS_PORT", config.ports.grpc_tls)?;
        config.ports.tcp = parse_env("BIUBIN_TCP_PORT", config.ports.tcp)?;
        config.ports.udp = parse_env("BIUBIN_UDP_PORT", config.ports.udp)?;
        config.ports.mqtt_tcp = parse_env("BIUBIN_MQTT_TCP_PORT", config.ports.mqtt_tcp)?;
        config.ports.mqtt_auth_tcp =
            parse_env("BIUBIN_MQTT_AUTH_TCP_PORT", config.ports.mqtt_auth_tcp)?;
        config.ports.mqtt_v5 = parse_env("BIUBIN_MQTT_V5_PORT", config.ports.mqtt_v5)?;
        config.ports.mqtt_tls = parse_env("BIUBIN_MQTT_TLS_PORT", config.ports.mqtt_tls)?;
        config.ports.mqtt_tls_backend = parse_env(
            "BIUBIN_MQTT_TLS_BACKEND_PORT",
            config.ports.mqtt_tls_backend,
        )?;
        config.ports.mqtt_ws = parse_env("BIUBIN_MQTT_WS_PORT", config.ports.mqtt_ws)?;
        config.ports.thrift = parse_env("BIUBIN_THRIFT_PORT", config.ports.thrift)?;
        config.http_body_limit = parse_env("BIUBIN_HTTP_BODY_LIMIT", config.http_body_limit)?;
        config.max_bytes_response =
            parse_env("BIUBIN_MAX_BYTES_RESPONSE", config.max_bytes_response)?;
        config.event_capacity = parse_env("BIUBIN_EVENT_CAPACITY", config.event_capacity)?;
        config.tcp_delay_ms = parse_env("BIUBIN_TCP_DELAY_MS", config.tcp_delay_ms)?;
        config.tcp_close_after = parse_env("BIUBIN_TCP_CLOSE_AFTER", config.tcp_close_after)?;
        config.tcp_read_limit = parse_env("BIUBIN_TCP_READ_LIMIT", config.tcp_read_limit)?;
        config.udp_delay_ms = parse_env("BIUBIN_UDP_DELAY_MS", config.udp_delay_ms)?;
        config.udp_drop_percent = parse_env("BIUBIN_UDP_DROP_PERCENT", config.udp_drop_percent)?;
        config.max_connections = parse_env("BIUBIN_MAX_CONNECTIONS", config.max_connections)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.bind_host.trim().is_empty() || self.advertise_host.trim().is_empty() {
            return Err("bind_host and advertise_host must not be empty".to_owned());
        }
        if !matches!(self.grpc_tls_mode.as_str(), "tls" | "mtls") {
            return Err("grpc_tls_mode must be either tls or mtls".to_owned());
        }
        if !matches!(self.mqtt_tls_mode.as_str(), "tls" | "mtls") {
            return Err("mqtt_tls_mode must be either tls or mtls".to_owned());
        }
        if self.mqtt_tls_enabled && !self.mqtt_enabled {
            return Err("mqtt_tls_enabled requires mqtt_enabled=true".to_owned());
        }
        if self.http_body_limit == 0
            || self.max_bytes_response == 0
            || self.event_capacity == 0
            || self.tcp_read_limit == 0
            || self.max_connections == 0
        {
            return Err("limits and event_capacity must be greater than zero".to_owned());
        }
        if self.udp_drop_percent > 100 {
            return Err("udp_drop_percent must be between 0 and 100".to_owned());
        }
        Ok(())
    }
}

fn env_or(name: &str, default: String) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool, String> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!("{name}={value:?} must be a boolean")),
        },
        Err(_) => Ok(default),
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T, String>
where
    T: FromStr + Display,
    T::Err: Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("{name}={value:?} is invalid: {error}")),
        Err(_) => Ok(default),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub schema_version: u8,
    pub id: String,
    pub seq: u64,
    pub at: String,
    pub protocol: String,
    pub kind: String,
    pub connection_id: Option<String>,
    pub direction: Option<String>,
    pub summary: String,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct EventStore {
    inner: Arc<Mutex<EventStoreInner>>,
    updates: broadcast::Sender<Event>,
}

struct EventStoreInner {
    capacity: usize,
    next_seq: u64,
    dropped: u64,
    events: VecDeque<Event>,
}

impl EventStore {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (updates, _) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(EventStoreInner {
                capacity,
                next_seq: 1,
                dropped: 0,
                events: VecDeque::new(),
            })),
            updates,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.updates.subscribe()
    }

    pub fn push(&self, protocol: &str, kind: &str, summary: impl Into<String>) -> Event {
        self.push_with_attributes(protocol, kind, summary, BTreeMap::new())
    }

    pub fn push_with_attributes(
        &self,
        protocol: &str,
        kind: &str,
        summary: impl Into<String>,
        attributes: BTreeMap<String, Value>,
    ) -> Event {
        let event = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let seq = inner.next_seq;
            inner.next_seq = inner.next_seq.saturating_add(1);
            let event = Event {
                schema_version: 1,
                id: format!("evt_{seq}"),
                seq,
                at: now_rfc3339_like(),
                protocol: protocol.to_owned(),
                kind: kind.to_owned(),
                connection_id: None,
                direction: None,
                summary: summary.into(),
                attributes,
            };
            inner.events.push_back(event.clone());
            while inner.events.len() > inner.capacity {
                inner.events.pop_front();
                inner.dropped = inner.dropped.saturating_add(1);
            }
            event
        };
        let _ = self.updates.send(event.clone());
        event
    }

    pub fn list(&self, limit: usize, protocol: Option<&str>) -> Vec<Event> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .events
            .iter()
            .rev()
            .filter(|event| protocol.is_none_or(|wanted| event.protocol == wanted))
            .take(limit.min(inner.capacity))
            .cloned()
            .collect()
    }

    pub fn dropped(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dropped
    }
}

#[derive(Clone, Default)]
pub struct Readiness {
    required: Arc<BTreeSet<String>>,
    states: Arc<Mutex<BTreeMap<String, bool>>>,
}

impl Readiness {
    pub fn with_required<I>(required: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let required: BTreeSet<String> = required.into_iter().collect();
        let states = required.iter().map(|name| (name.clone(), false)).collect();
        Self {
            required: Arc::new(required),
            states: Arc::new(Mutex::new(states)),
        }
    }

    pub fn mark(&self, name: impl Into<String>, ready: bool) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        states.insert(name.into(), ready);
    }

    pub fn is_ready(&self) -> bool {
        let states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.required
            .iter()
            .all(|name| states.get(name).copied().unwrap_or(false))
    }

    pub fn snapshot(&self) -> BTreeMap<String, bool> {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

fn now_rfc3339_like() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_store_is_bounded_and_returns_newest_first() {
        let store = EventStore::new(2);
        store.push("http", "one", "one");
        store.push("http", "two", "two");
        store.push("http", "three", "three");
        let events = store.list(10, None);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].summary, "three");
        assert_eq!(events[1].summary, "two");
        assert_eq!(store.dropped(), 1);
    }

    #[tokio::test]
    async fn event_store_notifies_subscribers_without_blocking_publishers() {
        let store = EventStore::new(2);
        let mut receiver = store.subscribe();
        let event = store.push("http", "request", "request");
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.id, event.id);
        assert_eq!(received.summary, "request");
    }

    #[test]
    fn readiness_requires_all_required_listeners() {
        let readiness = Readiness::with_required(["http".to_owned(), "grpc".to_owned()]);
        assert!(!readiness.is_ready());
        readiness.mark("http", true);
        assert!(!readiness.is_ready());
        readiness.mark("grpc", true);
        assert!(readiness.is_ready());
    }

    #[test]
    fn mqtt_tls_requires_the_mqtt_listener() {
        let config = Config {
            mqtt_tls_enabled: true,
            ..Config::default()
        };
        let error = config.validate().unwrap_err();
        assert!(error.contains("mqtt_enabled=true"));
    }
}
