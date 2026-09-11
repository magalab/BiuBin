use async_graphql::http::ALL_WEBSOCKET_PROTOCOLS;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::body::{Body, Bytes};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{
    ConnectInfo, DefaultBodyLimit, Extension, OriginalUri, Path, Query, Request, State,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use base64::Engine;
use biubin_core::{Config, EventStore, Readiness};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server as GrpcServer;
use tonic::transport::{Certificate as GrpcCertificate, Identity, ServerTlsConfig};
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status as GrpcStatus, Streaming};
use tracing::info;

mod graphql;
mod mqtt;
mod thrift;

const INDEX_HTML: &str = include_str!(concat!(env!("OUT_DIR"), "/index.html"));
const MAX_REQUEST_ID_LEN: usize = 128;
const MAX_HEADER_VALUE_LEN: usize = 4096;
const MAX_WS_MESSAGE_SIZE: usize = 64 * 1024;
const MAX_WS_ROOM_NAME_LEN: usize = 128;
const MAX_WS_ROOMS: usize = 100;
const MAX_SOCKET_FRAME_SIZE: usize = 1024 * 1024;
const MAX_SOCKET_READ_SIZE: usize = 4 * 1024 * 1024;
const SOCKET_IDLE_TIMEOUT_SECS: u64 = 60;
const MAX_UDP_PACKET_SIZE: usize = 65_535;
const STREAM_BYTES_CHUNK_SIZE: usize = 16 * 1024;
const GRPC_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/biubin_descriptor.bin"));

mod embedded_web {
    include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));
}

pub mod proto {
    tonic::include_proto!("biubin.v1");
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    bound: BoundAddresses,
    events: EventStore,
    graphql: graphql::BiubinSchema,
    readiness: Readiness,
    request_seq: Arc<AtomicU64>,
    connection_slots: Arc<Semaphore>,
    rooms: Arc<Mutex<HashMap<String, broadcast::Sender<WsPayload>>>>,
}

#[derive(Clone, Debug)]
struct BoundAddresses {
    http: SocketAddr,
    grpc_h2c: SocketAddr,
    grpc_tls: Option<SocketAddr>,
    tcp: SocketAddr,
    udp: SocketAddr,
    thrift: SocketAddr,
    mqtt: Option<MqttAddresses>,
}

#[derive(Clone, Debug)]
struct MqttAddresses {
    tcp: u16,
    auth_tcp: u16,
    v5: u16,
    tls: Option<u16>,
    ws: u16,
}

impl BoundAddresses {
    fn bound_json(&self) -> Value {
        json!({
            "http": self.http.to_string(),
            "grpc_h2c": self.grpc_h2c.to_string(),
            "grpc_tls": self.grpc_tls.map(|address| address.to_string()),
            "tcp": self.tcp.to_string(),
            "udp": self.udp.to_string(),
            "thrift": self.thrift.to_string(),
            "mqtt": self.mqtt.as_ref().map(|mqtt| json!({
                "tcp": SocketAddr::new(self.http.ip(), mqtt.tcp).to_string(),
                "auth_tcp": SocketAddr::new(self.http.ip(), mqtt.auth_tcp).to_string(),
                "v5": SocketAddr::new(self.http.ip(), mqtt.v5).to_string(),
                "tls": mqtt.tls.map(|port| SocketAddr::new(self.http.ip(), port).to_string()),
                "ws": SocketAddr::new(self.http.ip(), mqtt.ws).to_string(),
            })),
        })
    }

    fn advertised_json(&self, host: &str) -> Value {
        let endpoint = |address: SocketAddr| host_port(host, address.port());
        json!({
            "http": endpoint(self.http),
            "grpc_h2c": endpoint(self.grpc_h2c),
            "grpc_tls": self.grpc_tls.map(endpoint),
            "tcp": endpoint(self.tcp),
            "udp": endpoint(self.udp),
            "thrift": endpoint(self.thrift),
            "mqtt": self.mqtt.as_ref().map(|mqtt| json!({
                "tcp": host_port(host, mqtt.tcp),
                "auth_tcp": host_port(host, mqtt.auth_tcp),
                "v5": host_port(host, mqtt.v5),
                "tls": mqtt.tls.map(|port| host_port(host, port)),
                "ws": host_port(host, mqtt.ws),
            })),
        })
    }
}

fn host_port(host: &str, port: u16) -> String {
    if host
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_ipv6())
    {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[derive(Clone, Debug)]
struct WsPayload {
    binary: bool,
    data: Vec<u8>,
}

#[derive(Serialize)]
struct EventsResponse {
    events: Vec<biubin_core::Event>,
    dropped: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck();
    }
    tracing_subscriber::fmt().with_env_filter("info").init();

    let mut config = Config::from_env().map_err(|error| format!("configuration error: {error}"))?;
    mqtt::normalize_ephemeral_ports(&mut config)
        .map_err(|error| format!("MQTT configuration error: {error}"))?;
    if config.grpc_tls_enabled || config.mqtt_tls_enabled {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }
    if config.grpc_tls_enabled {
        validate_grpc_tls_config(&config)?;
    }
    if config
        .bind_host
        .parse::<IpAddr>()
        .is_ok_and(|address| !address.is_loopback())
    {
        tracing::warn!(
            bind_host = %config.bind_host,
            "biubin is listening beyond loopback; protect the development endpoints and default credentials"
        );
    }
    let listener_addr = host_port(&config.bind_host, config.ports.http);
    let listener = TcpListener::bind(&listener_addr).await?;
    let actual_addr = listener.local_addr()?;
    let grpc_listener_addr = host_port(&config.bind_host, config.ports.grpc_h2c);
    let grpc_listener = TcpListener::bind(&grpc_listener_addr).await?;
    let actual_grpc_addr = grpc_listener.local_addr()?;
    let grpc_tls_listener = if config.grpc_tls_enabled {
        let address = host_port(&config.bind_host, config.ports.grpc_tls);
        Some(TcpListener::bind(address).await?)
    } else {
        None
    };
    let actual_grpc_tls_addr = grpc_tls_listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()?;
    let tcp_listener_addr = host_port(&config.bind_host, config.ports.tcp);
    let tcp_listener = TcpListener::bind(&tcp_listener_addr).await?;
    let actual_tcp_addr = tcp_listener.local_addr()?;
    let udp_listener_addr = host_port(&config.bind_host, config.ports.udp);
    let udp_listener = UdpSocket::bind(&udp_listener_addr).await?;
    let actual_udp_addr = udp_listener.local_addr()?;
    let thrift_listener_addr = host_port(&config.bind_host, config.ports.thrift);
    let thrift_listener = thrift::bind(thrift_listener_addr.parse()?)
        .map_err(std::io::Error::other)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
    let actual_thrift_addr = thrift_listener.address();
    config.ports.http = actual_addr.port();
    config.ports.grpc_h2c = actual_grpc_addr.port();
    if let Some(address) = actual_grpc_tls_addr {
        config.ports.grpc_tls = address.port();
    }
    config.ports.tcp = actual_tcp_addr.port();
    config.ports.udp = actual_udp_addr.port();
    config.ports.thrift = actual_thrift_addr.port();
    let bound = BoundAddresses {
        http: actual_addr,
        grpc_h2c: actual_grpc_addr,
        grpc_tls: actual_grpc_tls_addr,
        tcp: actual_tcp_addr,
        udp: actual_udp_addr,
        thrift: actual_thrift_addr,
        mqtt: config.mqtt_enabled.then(|| MqttAddresses {
            tcp: config.ports.mqtt_tcp,
            auth_tcp: config.ports.mqtt_auth_tcp,
            v5: config.ports.mqtt_v5,
            tls: config.mqtt_tls_enabled.then_some(config.ports.mqtt_tls),
            ws: config.ports.mqtt_ws,
        }),
    };

    let mut required_listeners = vec![
        "http".to_owned(),
        "grpc_h2c".to_owned(),
        "tcp".to_owned(),
        "udp".to_owned(),
        "thrift".to_owned(),
    ];
    if config.mqtt_enabled {
        required_listeners.push("mqtt".to_owned());
    }
    if config.grpc_tls_enabled {
        required_listeners.push("grpc_tls".to_owned());
    }
    let readiness = Readiness::with_required(required_listeners);
    readiness.mark("http", true);
    readiness.mark("grpc_h2c", true);
    readiness.mark("tcp", true);
    readiness.mark("udp", true);
    if config.grpc_tls_enabled {
        readiness.mark("grpc_tls", true);
    }
    let config = Arc::new(config);
    let events = EventStore::new(config.event_capacity);
    let state = AppState {
        graphql: graphql::build_schema(config.clone(), events.clone()),
        bound,
        events,
        request_seq: Arc::new(AtomicU64::new(1)),
        connection_slots: Arc::new(Semaphore::new(config.max_connections)),
        rooms: Arc::new(Mutex::new(HashMap::new())),
        config,
        readiness,
    };
    if state.config.mqtt_enabled {
        mqtt::spawn(&state.config, &state.events)
            .map_err(std::io::Error::other)
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
        state.readiness.mark("mqtt", true);
    }
    let mut thrift_handle = thrift::spawn(
        thrift_listener,
        state.events.clone(),
        state.connection_slots.clone(),
    )
    .map_err(std::io::Error::other)
    .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
    state.readiness.mark("thrift", true);

    info!(
        service = "biubin",
        bind_address = %actual_addr,
        advertise_address = %host_port(&state.config.advertise_host, actual_addr.port()),
        grpc_bind_address = %actual_grpc_addr,
        grpc_tls_bind_address = ?actual_grpc_tls_addr,
        tcp_bind_address = %actual_tcp_addr,
        udp_bind_address = %actual_udp_addr,
        thrift_bind_address = %actual_thrift_addr,
        "server starting"
    );

    let app = router(state.clone());
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal_tx = shutdown_tx.clone();
    let signal_shutdown_requested = shutdown_requested.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_shutdown_requested.store(true, Ordering::Release);
        let _ = signal_tx.send(());
    });

    let mut tasks = JoinSet::new();
    let mut grpc_tls_shutdown = shutdown_tx.subscribe();
    if state.config.grpc_tls_enabled {
        let state_for_tls = state.clone();
        tasks.spawn(async move {
            (
                "grpc_tls",
                run_grpc_tls(grpc_tls_listener, state_for_tls, async move {
                    let _ = grpc_tls_shutdown.recv().await;
                })
                .await,
            )
        });
    } else {
        drop(grpc_tls_shutdown);
    }
    let tcp_state = state.clone();
    let mut tcp_shutdown = shutdown_tx.subscribe();
    tasks.spawn(async move {
        (
            "tcp",
            run_tcp(
                tcp_listener,
                tcp_state.events.clone(),
                tcp_state.config.clone(),
                tcp_state.connection_slots.clone(),
                async move {
                    let _ = tcp_shutdown.recv().await;
                },
            )
            .await,
        )
    });
    let udp_state = state.clone();
    let mut udp_shutdown = shutdown_tx.subscribe();
    tasks.spawn(async move {
        (
            "udp",
            run_udp(
                udp_listener,
                udp_state.events.clone(),
                udp_state.config.clone(),
                async move {
                    let _ = udp_shutdown.recv().await;
                },
            )
            .await,
        )
    });

    let grpc_state = state.clone();
    let mut grpc_shutdown = shutdown_tx.subscribe();
    tasks.spawn(async move {
        (
            "grpc_h2c",
            run_grpc(
                grpc_listener,
                grpc_state.events.clone(),
                grpc_state.connection_slots.clone(),
                async move {
                    let _ = grpc_shutdown.recv().await;
                },
            )
            .await,
        )
    });

    let mut http_shutdown = shutdown_tx.subscribe();
    tasks.spawn(async move {
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = http_shutdown.recv().await;
        })
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) });
        ("http", result)
    });

    let thrift_stop = thrift_handle.stop_token();
    let mut thrift_shutdown = shutdown_tx.subscribe();
    let thrift_signal = tokio::spawn(async move {
        let _ = thrift_shutdown.recv().await;
        thrift_stop.store(true, Ordering::Release);
    });

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((name, Ok(()))) => {
                if shutdown_requested.load(Ordering::Acquire) {
                    tracing::debug!(listener = name, "listener stopped during shutdown");
                } else {
                    state.readiness.mark(name, false);
                    tracing::error!(listener = name, "listener stopped without shutdown request");
                }
            }
            Ok((name, Err(error))) => {
                tracing::error!(listener = name, %error, "listener stopped unexpectedly");
                state.readiness.mark(name, false);
            }
            Err(error) => {
                tracing::error!(%error, "listener task panicked");
            }
        }
    }
    let _ = thrift_signal.await;
    thrift_handle.join();
    Ok(())
}

fn healthcheck() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{Read, Write};
    use std::time::Duration;

    let port = std::env::var("BIUBIN_HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_owned())
        .parse::<u16>()?;
    if port == 0 {
        return Err("BIUBIN_HTTP_PORT=0 cannot be used by the fixed-port healthcheck".into());
    }
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = [0_u8; 256];
    let length = stream.read(&mut response)?;
    let response = std::str::from_utf8(&response[..length])?;
    if response.starts_with("HTTP/1.1 200 ") || response.starts_with("HTTP/1.0 200 ") {
        Ok(())
    } else {
        Err(format!(
            "biubin is not ready: {}",
            response.lines().next().unwrap_or("empty response")
        )
        .into())
    }
}

fn validate_grpc_tls_config(
    config: &Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cert = std::fs::read(&config.tls_cert_path).map_err(|error| {
        std::io::Error::other(format!(
            "read gRPC TLS certificate {}: {error}",
            config.tls_cert_path
        ))
    })?;
    let key = std::fs::read(&config.tls_key_path).map_err(|error| {
        std::io::Error::other(format!(
            "read gRPC TLS key {}: {error}",
            config.tls_key_path
        ))
    })?;
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
    if config.grpc_tls_mode == "mtls" {
        let client_ca = std::fs::read(&config.tls_client_ca_path).map_err(|error| {
            std::io::Error::other(format!(
                "read gRPC mTLS client CA {}: {error}",
                config.tls_client_ca_path
            ))
        })?;
        tls = tls.client_ca_root(GrpcCertificate::from_pem(client_ca));
    }
    GrpcServer::builder().tls_config(tls)?;
    Ok(())
}

async fn run_grpc<F>(
    listener: TcpListener,
    events: EventStore,
    connection_slots: Arc<Semaphore>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(GRPC_DESCRIPTOR_SET)
        .build_v1()?;
    GrpcServer::builder()
        .add_service(proto::biubin_server::BiubinServer::new(GrpcService {
            events,
            connection_slots,
        }))
        .add_service(reflection)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

async fn run_grpc_tls<F>(
    listener: Option<TcpListener>,
    state: AppState,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let Some(listener) = listener else {
        std::mem::drop(shutdown);
        return Ok(());
    };
    let cert = std::fs::read(&state.config.tls_cert_path)?;
    let key = std::fs::read(&state.config.tls_key_path)?;
    let identity = Identity::from_pem(cert, key);
    let mut tls = ServerTlsConfig::new().identity(identity);
    if state.config.grpc_tls_mode == "mtls" {
        let client_ca = std::fs::read(&state.config.tls_client_ca_path)?;
        tls = tls.client_ca_root(GrpcCertificate::from_pem(client_ca));
    }
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(GRPC_DESCRIPTOR_SET)
        .build_v1()?;
    GrpcServer::builder()
        .tls_config(tls)?
        .add_service(proto::biubin_server::BiubinServer::new(GrpcService {
            events: state.events,
            connection_slots: state.connection_slots,
        }))
        .add_service(reflection)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

async fn run_tcp<F>(
    listener: TcpListener,
    events: EventStore,
    config: Arc<Config>,
    connection_slots: Arc<Semaphore>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::pin!(shutdown);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                break;
            }
            accepted = listener.accept() => {
                let (stream, address) = accepted?;
                let permit = match connection_slots.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        events.push("tcp", "connection_rejected", format!("peer={address}"));
                        drop(stream);
                        continue;
                    }
                };
                let events = events.clone();
                let config = config.clone();
                connections.spawn(async move {
                    handle_tcp_connection(stream, address, events, config, permit).await;
                });
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result.expect("join_next branch must yield a task") {
                    tracing::warn!(%error, "tcp connection task stopped unexpectedly");
                }
            }
        }
    }
    Ok(())
}

async fn handle_tcp_connection(
    mut stream: TcpStream,
    address: SocketAddr,
    events: EventStore,
    config: Arc<Config>,
    _permit: OwnedSemaphorePermit,
) {
    let mut buffer = Vec::with_capacity(4096);
    let mut scratch = [0_u8; 8192];
    let mut total_read = 0_usize;
    let mut frame_count = 0_usize;
    loop {
        let read = match tokio::time::timeout(
            std::time::Duration::from_secs(SOCKET_IDLE_TIMEOUT_SECS),
            stream.read(&mut scratch),
        )
        .await
        {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => {
                tracing::debug!(%address, %error, "tcp read failed");
                break;
            }
            Err(_) => {
                tracing::debug!(%address, "tcp idle timeout");
                break;
            }
        };
        if read == 0 {
            break;
        }
        total_read = total_read.saturating_add(read);
        if total_read > config.tcp_read_limit.min(MAX_SOCKET_READ_SIZE) {
            let _ = stream.shutdown().await;
            break;
        }
        buffer.extend_from_slice(&scratch[..read]);

        loop {
            let line_end = buffer.iter().position(|byte| *byte == b'\n');
            let frame = if buffer.len() >= 4 {
                let length =
                    u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
                if length > MAX_SOCKET_FRAME_SIZE {
                    if let Some(end) = line_end {
                        Some((end + 1, "line"))
                    } else {
                        let _ = stream.shutdown().await;
                        return;
                    }
                } else if buffer.len() >= 4 + length {
                    Some((4 + length, "length_prefixed"))
                } else {
                    None
                }
            } else {
                line_end.map(|end| (end + 1, "line"))
            };
            let Some((frame_size, framing)) = frame else {
                break;
            };
            if frame_size > MAX_SOCKET_FRAME_SIZE {
                let _ = stream.shutdown().await;
                return;
            }
            let frame: Vec<u8> = buffer.drain(..frame_size).collect();
            if config.tcp_delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(config.tcp_delay_ms)).await;
            }
            if stream.write_all(&frame).await.is_err() {
                return;
            }
            frame_count = frame_count.saturating_add(1);
            events.push("tcp", "frame_echoed", format!("{framing} peer={address}"));
            if config.tcp_close_after > 0 && frame_count >= config.tcp_close_after {
                let _ = stream.shutdown().await;
                return;
            }
        }
    }
    let _ = stream.shutdown().await;
    events.push("tcp", "connection_closed", format!("peer={address}"));
}

async fn run_udp<F>(
    socket: UdpSocket,
    events: EventStore,
    config: Arc<Config>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::pin!(shutdown);
    let mut buffer = vec![0_u8; MAX_UDP_PACKET_SIZE];
    let mut packet_sequence = 0_u64;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            received = socket.recv_from(&mut buffer) => {
                let (length, address) = received?;
                packet_sequence = packet_sequence.saturating_add(1);
                if config.udp_drop_percent > 0
                    && (packet_sequence.saturating_sub(1) % 100)
                        < u64::from(config.udp_drop_percent)
                {
                    events.push("udp", "datagram_dropped", format!("peer={address} bytes={length}"));
                    continue;
                }
                if config.udp_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(config.udp_delay_ms)).await;
                }
                socket.send_to(&buffer[..length], address).await?;
                events.push("udp", "datagram_echoed", format!("peer={address} bytes={length}"));
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct GrpcService {
    events: EventStore,
    connection_slots: Arc<Semaphore>,
}

impl GrpcService {
    fn acquire_connection_slot(&self) -> Result<OwnedSemaphorePermit, GrpcStatus> {
        self.connection_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| GrpcStatus::resource_exhausted("connection limit reached"))
    }
}

type GrpcResponseStream =
    Pin<Box<dyn Stream<Item = Result<proto::EchoResponse, GrpcStatus>> + Send>>;

#[tonic::async_trait]
impl proto::biubin_server::Biubin for GrpcService {
    async fn index(
        &self,
        _request: GrpcRequest<proto::IndexRequest>,
    ) -> Result<GrpcResponse<proto::IndexResponse>, GrpcStatus> {
        let _permit = self.acquire_connection_slot()?;
        Ok(GrpcResponse::new(proto::IndexResponse {
            service: "biubin".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            methods: vec![
                "Index".to_owned(),
                "EchoUnary".to_owned(),
                "EchoServerStream".to_owned(),
                "EchoClientStream".to_owned(),
                "EchoBidiStream".to_owned(),
                "Error".to_owned(),
            ],
            modes: vec!["h2c".to_owned(), "tls".to_owned(), "mtls".to_owned()],
        }))
    }

    async fn echo_unary(
        &self,
        request: GrpcRequest<proto::EchoRequest>,
    ) -> Result<GrpcResponse<proto::EchoResponse>, GrpcStatus> {
        let _permit = self.acquire_connection_slot()?;
        let metadata_keys = metadata_keys(request.metadata());
        let request = request.into_inner();
        self.events.push("grpc", "request_received", "EchoUnary");
        Ok(GrpcResponse::new(proto::EchoResponse {
            message: request.message,
            payload: request.payload,
            sequence: 1,
            metadata_keys,
        }))
    }

    type EchoServerStreamStream = GrpcResponseStream;

    async fn echo_server_stream(
        &self,
        request: GrpcRequest<proto::EchoRequest>,
    ) -> Result<GrpcResponse<Self::EchoServerStreamStream>, GrpcStatus> {
        let permit = self.acquire_connection_slot()?;
        let metadata_keys = metadata_keys(request.metadata());
        let request = request.into_inner();
        let count = u64::from(request.count.clamp(1, 1000));
        let interval_ms = request.interval_ms.min(60_000);
        let message = request.message;
        let payload = request.payload;
        let events = self.events.clone();
        let stream = async_stream::try_stream! {
            let _permit = permit;
            for sequence in 1..=count {
                if interval_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
                }
                events.push("grpc", "stream_message", format!("EchoServerStream sequence={sequence}"));
                yield proto::EchoResponse {
                    message: message.clone(),
                    payload: payload.clone(),
                    sequence,
                    metadata_keys: metadata_keys.clone(),
                };
            }
        };
        Ok(GrpcResponse::new(Box::pin(stream)))
    }

    async fn echo_client_stream(
        &self,
        request: GrpcRequest<Streaming<proto::EchoRequest>>,
    ) -> Result<GrpcResponse<proto::StreamSummary>, GrpcStatus> {
        let _permit = self.acquire_connection_slot()?;
        let mut stream = request.into_inner();
        let mut count = 0;
        let mut last_message = String::new();
        let mut last_payload = Vec::new();
        while let Some(item) = stream.message().await? {
            count += 1;
            if count > 1000 {
                return Err(GrpcStatus::resource_exhausted(
                    "stream message limit exceeded",
                ));
            }
            last_message = item.message;
            last_payload = item.payload;
        }
        self.events.push(
            "grpc",
            "request_received",
            format!("EchoClientStream count={count}"),
        );
        Ok(GrpcResponse::new(proto::StreamSummary {
            count,
            last_message,
            last_payload,
        }))
    }

    type EchoBidiStreamStream = GrpcResponseStream;

    async fn echo_bidi_stream(
        &self,
        request: GrpcRequest<Streaming<proto::EchoRequest>>,
    ) -> Result<GrpcResponse<Self::EchoBidiStreamStream>, GrpcStatus> {
        let permit = self.acquire_connection_slot()?;
        let mut inbound = request.into_inner();
        let events = self.events.clone();
        let stream = async_stream::try_stream! {
            let _permit = permit;
            let mut sequence = 0;
            while let Some(item) = inbound.message().await? {
                sequence += 1;
                if sequence > 1000 {
                    Err(GrpcStatus::resource_exhausted("stream message limit exceeded"))?;
                }
                let interval_ms = item.interval_ms.min(60_000);
                if interval_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
                }
                events.push("grpc", "stream_message", format!("EchoBidiStream sequence={sequence}"));
                yield proto::EchoResponse {
                    message: item.message,
                    payload: item.payload,
                    sequence,
                    metadata_keys: Vec::new(),
                };
            }
        };
        Ok(GrpcResponse::new(Box::pin(stream)))
    }

    async fn error(
        &self,
        request: GrpcRequest<proto::ErrorRequest>,
    ) -> Result<GrpcResponse<proto::Empty>, GrpcStatus> {
        let _permit = self.acquire_connection_slot()?;
        let request = request.into_inner();
        let code = grpc_code(request.code);
        if code == tonic::Code::Ok {
            return Ok(GrpcResponse::new(proto::Empty {}));
        }
        let mut status = GrpcStatus::new(
            code,
            if request.message.is_empty() {
                "biubin requested error"
            } else {
                &request.message
            },
        );
        if request.include_trailer {
            status.metadata_mut().insert(
                "biubin-trailer",
                "present".parse().expect("valid metadata value"),
            );
        }
        Err(status)
    }
}

fn metadata_keys(metadata: &tonic::metadata::MetadataMap) -> Vec<String> {
    metadata
        .keys()
        .map(|key| match key {
            tonic::metadata::KeyRef::Ascii(key) => key.as_str().to_owned(),
            tonic::metadata::KeyRef::Binary(key) => key.as_str().to_owned(),
        })
        .collect()
}

fn grpc_code(code: u32) -> tonic::Code {
    match code {
        0 => tonic::Code::Ok,
        1 => tonic::Code::Cancelled,
        2 => tonic::Code::Unknown,
        3 => tonic::Code::InvalidArgument,
        4 => tonic::Code::DeadlineExceeded,
        5 => tonic::Code::NotFound,
        6 => tonic::Code::AlreadyExists,
        7 => tonic::Code::PermissionDenied,
        8 => tonic::Code::ResourceExhausted,
        9 => tonic::Code::FailedPrecondition,
        10 => tonic::Code::Aborted,
        11 => tonic::Code::OutOfRange,
        12 => tonic::Code::Unimplemented,
        13 => tonic::Code::Internal,
        14 => tonic::Code::Unavailable,
        15 => tonic::Code::DataLoss,
        16 => tonic::Code::Unauthenticated,
        _ => tonic::Code::InvalidArgument,
    }
}

fn router(state: AppState) -> Router {
    let body_limit = state.config.http_body_limit;
    Router::new()
        .route("/", get(index))
        .route("/assets/{*path}", get(web_asset))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/info", get(info_api))
        .route("/api/v1/capabilities", get(capabilities_api))
        .route("/api/v1/events", get(events_api))
        .route("/graphql", get(graphql_handler).post(graphql_handler))
        .route("/graphql/ws", get(graphql_ws_handler))
        .route("/http/status/{code}", any(http_status))
        .route("/http/delay/{seconds}", any(http_delay))
        .route("/http/redirect/{count}", any(http_redirect))
        .route("/http/bytes/{count}", get(http_bytes))
        .route("/http/stream-bytes/{count}", get(http_stream_bytes))
        .route("/http/gzip", any(http_gzip))
        .route("/http/deflate", any(http_deflate))
        .route("/http/basic-auth/{user}/{password}", any(http_basic_auth))
        .route("/http/anything", any(http_anything_root))
        .route("/http/anything/{*path}", any(http_anything_path))
        .route("/http/headers", any(http_headers))
        .route("/http/user-agent", any(http_user_agent))
        .route("/http/ip", any(http_ip))
        .route("/ws/echo", get(ws_echo))
        .route("/ws/room/{name}", get(ws_room))
        .route("/ws/ticker", get(ws_ticker))
        .route("/ws/close", get(ws_close))
        .route("/sse/events", get(sse_events))
        .route("/sse/ticker", get(sse_ticker))
        .route("/api/v1/events/stream", get(events_stream))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_id_middleware,
        ))
        .with_state(state)
}

async fn request_id_middleware(
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

async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response {
    let request_id = request_id(&headers, &state);
    state.events.push("graphql", "request_received", "GraphQL");
    let mut request = request.into_inner();
    let syntax_valid = request.parsed_query().is_ok();
    let mut graphql_response = state.graphql.execute(request).await;
    graphql::annotate_errors(&mut graphql_response, syntax_valid);
    let response: GraphQLResponse = graphql_response.into();
    let mut response = response.into_response();
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn graphql_ws_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    upgrade: axum::extract::WebSocketUpgrade,
) -> Response {
    let request_id = request_id(&headers, &state);
    let Ok(permit) = acquire_connection_slot(&state, &request_id) else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            json!({"error": "connection limit reached"}),
        );
    };
    let schema = state.graphql.clone();
    let mut response = upgrade
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            GraphQLWebSocket::new(socket, schema, protocol)
                .serve()
                .await;
        })
        .into_response();
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    text_response(
        StatusCode::OK,
        request_id(&headers, &state),
        INDEX_HTML,
        "text/html; charset=utf-8",
    )
}

async fn web_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let asset_name = format!("assets/{path}");
    let Some((_, bytes)) = embedded_web::WEB_ASSETS
        .iter()
        .find(|(name, _)| *name == asset_name)
    else {
        return text_response(
            StatusCode::NOT_FOUND,
            request_id,
            "asset not found",
            "text/plain; charset=utf-8",
        );
    };
    let content_type = match path.rsplit('.').next().unwrap_or_default() {
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    };
    let mut response = (
        [(header::CONTENT_TYPE, content_type)],
        Body::from(bytes.to_vec()),
    )
        .into_response();
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn healthz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        json!({"status": "ok", "service": "biubin"}),
    )
}

async fn readyz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let ready = state.readiness.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(
        status,
        request_id(&headers, &state),
        json!({"ready": ready, "listeners": state.readiness.snapshot()}),
    )
}

async fn info_api(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    json_response(
        StatusCode::OK,
        request_id,
        json!({
            "name": "biubin",
            "version": env!("CARGO_PKG_VERSION"),
            "bind_host": state.config.bind_host,
            "advertise_host": state.config.advertise_host,
            "ports": state.config.ports,
            "bound_addresses": state.bound.bound_json(),
            "advertised_addresses": state.bound.advertised_json(&state.config.advertise_host),
            "mqtt_enabled": state.config.mqtt_enabled,
            "mqtt_tls_enabled": state.config.mqtt_tls_enabled,
            "mqtt_tls_mode": state.config.mqtt_tls_mode,
            "graphql_introspection_enabled": state.config.graphql_introspection_enabled,
            "mqtt_topic_acl": false,
            "mqtt_session_persistence": "process",
            "mqtt_graceful_drain": false,
            "grpc_tls_enabled": state.config.grpc_tls_enabled,
            "grpc_tls_mode": state.config.grpc_tls_mode,
            "limits": {
                "http_body_bytes": state.config.http_body_limit,
                "max_response_bytes": state.config.max_bytes_response,
                "max_connections": state.config.max_connections,
                "event_capacity": state.config.event_capacity,
            },
            "ready": state.readiness.is_ready(),
        }),
    )
}

async fn capabilities_api(State(state): State<AppState>, headers: HeaderMap) -> Response {
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        json!({
            "schema_version": 1,
            "service": "biubin",
            "addresses": {
                "bound": state.bound.bound_json(),
                "advertised": state.bound.advertised_json(&state.config.advertise_host),
            },
            "features": {
                "graphql_introspection": state.config.graphql_introspection_enabled,
                "mqtt_topic_acl": false,
                "mqtt_session_persistence": "process",
                "mqtt_graceful_drain": false,
            },
            "endpoints": [
                {"protocol": "http", "method": "*", "path": "/http/anything/{*path}"},
                {"protocol": "http", "method": "*", "path": "/http/status/{code}"},
                {"protocol": "http", "method": "*", "path": "/http/delay/{seconds}"},
                {"protocol": "http", "method": "*", "path": "/http/redirect/{count}"},
                {"protocol": "http", "method": "GET", "path": "/http/bytes/{count}"},
                {"protocol": "http", "method": "GET", "path": "/http/stream-bytes/{count}"},
                {"protocol": "http", "method": "*", "path": "/http/gzip"},
                {"protocol": "http", "method": "*", "path": "/http/deflate"},
                {"protocol": "http", "method": "*", "path": "/http/basic-auth/{user}/{password}"},
                {"protocol": "http", "method": "*", "path": "/http/headers"},
                {"protocol": "http", "method": "*", "path": "/http/ip"},
                {"protocol": "http", "method": "*", "path": "/http/user-agent"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/echo"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/room/{name}"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/ticker"},
                {"protocol": "websocket", "method": "GET", "path": "/ws/close"},
                {"protocol": "sse", "method": "GET", "path": "/sse/events"},
                {"protocol": "sse", "method": "GET", "path": "/sse/ticker"},
                {"protocol": "sse", "method": "GET", "path": "/api/v1/events/stream"},
                {"protocol": "graphql", "method": "GET/POST", "path": "/graphql"},
                {"protocol": "graphql", "method": "WS", "path": "/graphql/ws", "subprotocols": ["graphql-transport-ws", "graphql-ws"], "introspection": state.config.graphql_introspection_enabled},
                {"protocol": "tcp", "method": "line", "path": "tcp://{host}:{tcp_port}"},
                {"protocol": "tcp", "method": "length-prefixed", "path": "tcp://{host}:{tcp_port}"},
                {"protocol": "udp", "method": "echo", "path": "udp://{host}:{udp_port}"},
                {"protocol": "thrift", "method": "binary", "path": "thrift://{host}:{thrift_port}"},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_tcp_port}", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_auth_tcp_port}", "auth": "username/password", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtt://{host}:{mqtt_v5_port}", "version": 5, "auth": "username/password", "enabled": state.config.mqtt_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "mqtts://{host}:{mqtt_tls_port}", "tls": state.config.mqtt_tls_mode, "enabled": state.config.mqtt_enabled && state.config.mqtt_tls_enabled},
                {"protocol": "mqtt", "method": "publish/subscribe", "path": "ws://{host}:{mqtt_ws_port}", "transport": "websocket", "enabled": state.config.mqtt_enabled},
            ],
            "listeners": state.readiness.snapshot(),
        }),
    )
}

#[derive(serde::Deserialize)]
struct EventsQuery {
    limit: Option<usize>,
    protocol: Option<String>,
}

async fn events_api(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let events = state.events.list(limit, query.protocol.as_deref());
    json_response(
        StatusCode::OK,
        request_id(&headers, &state),
        serde_json::to_value(EventsResponse {
            events,
            dropped: state.events.dropped(),
        })
        .expect("event serialization cannot fail"),
    )
}

async fn http_status(
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

async fn http_delay(
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

async fn http_redirect(
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
    let location = format!("/http/redirect/{}", count - 1);
    let mut response = StatusCode::FOUND.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn http_bytes(
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
    let body: Vec<u8> = (0..count).map(|index| (index % 251) as u8).collect();
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn http_stream_bytes(
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

async fn http_gzip(State(state): State<AppState>, headers: HeaderMap) -> Response {
    compressed_http_response(state, headers, "gzip")
}

async fn http_deflate(State(state): State<AppState>, headers: HeaderMap) -> Response {
    compressed_http_response(state, headers, "deflate")
}

fn compressed_http_response(state: AppState, headers: HeaderMap, encoding: &str) -> Response {
    let request_id = request_id(&headers, &state);
    let source = br#"{"message":"biubin compressed response"}"#;
    let compressed = if encoding == "gzip" {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(source).and_then(|()| encoder.finish())
    } else {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(source).and_then(|()| encoder.finish())
    };
    let Ok(compressed) = compressed else {
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request_id,
            json!({"error": "compression failed"}),
        );
    };
    let mut response = compressed.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        header::CONTENT_ENCODING,
        HeaderValue::from_str(encoding).expect("static encoding is valid"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

async fn http_basic_auth(
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

async fn http_anything_root(
    State(state): State<AppState>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    client: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    http_anything_impl(
        state,
        method,
        uri,
        headers,
        client.map(|Extension(ConnectInfo(address))| address),
        body,
        String::new(),
    )
    .await
}

async fn http_anything_path(
    State(state): State<AppState>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    Path(path): Path<String>,
    client: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    http_anything_impl(
        state,
        method,
        uri,
        headers,
        client.map(|Extension(ConnectInfo(address))| address),
        body,
        path,
    )
    .await
}

async fn http_anything_impl(
    state: AppState,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    client: Option<SocketAddr>,
    body: Bytes,
    path: String,
) -> Response {
    let request_id = request_id(&headers, &state);
    let mut header_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in &headers {
        header_values.entry(name.to_string()).or_default().push(
            if is_sensitive_header(name.as_str()) {
                "[REDACTED]".to_owned()
            } else {
                header_preview(value)
            },
        );
    }
    let body_preview = String::from_utf8_lossy(&body[..body.len().min(4096)]).into_owned();
    state.events.push(
        "http",
        "request_received",
        format!("{} {}", method, uri.path()),
    );
    json_response(
        StatusCode::OK,
        request_id.clone(),
        json!({
            "request_id": request_id,
            "method": method.as_str(),
            "path": path,
            "uri": uri.0.to_string(),
            "query": uri.0.query().unwrap_or_default(),
            "headers": header_values,
            "body": body_preview,
            "body_length": body.len(),
            "client": client.map(|address| json!({"ip": address.ip().to_string(), "port": address.port()})),
        }),
    )
}

async fn http_headers(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let values = redacted_headers(&headers);
    json_response(StatusCode::OK, request_id, json!({"headers": values}))
}

async fn http_user_agent(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers, &state);
    let value = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok());
    json_response(StatusCode::OK, request_id, json!({"user_agent": value}))
}

async fn http_ip(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
) -> Response {
    let request_id = request_id(&headers, &state);
    let origin = address.ip().to_string();
    json_response(StatusCode::OK, request_id, json!({"origin": origin}))
}

fn redacted_headers(headers: &HeaderMap) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::new();
    for (name, value) in headers {
        values
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(if is_sensitive_header(name.as_str()) {
                "[REDACTED]".to_owned()
            } else {
                header_preview(value)
            });
    }
    values
}

fn header_preview(value: &HeaderValue) -> String {
    let value = value.to_str().unwrap_or("[INVALID_UTF8]");
    if value.len() <= MAX_HEADER_VALUE_LEN {
        value.to_owned()
    } else {
        let preview: String = value.chars().take(MAX_HEADER_VALUE_LEN).collect();
        format!("{preview}…")
    }
}

async fn ws_echo(
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

async fn ws_room(
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
struct TickerQuery {
    interval_ms: Option<u64>,
    count: Option<u64>,
}

async fn ws_ticker(
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
struct CloseQuery {
    code: Option<u16>,
    after: Option<u64>,
}

async fn ws_close(
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

#[derive(serde::Deserialize)]
struct SseTickerQuery {
    interval_ms: Option<u64>,
    count: Option<u64>,
}

async fn sse_events(State(state): State<AppState>, headers: HeaderMap) -> Response {
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

async fn sse_ticker(
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

async fn events_stream(State(state): State<AppState>, headers: HeaderMap) -> Response {
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

fn request_id(headers: &HeaderMap, state: &AppState) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= MAX_REQUEST_ID_LEN)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("req_{}", state.request_seq.fetch_add(1, Ordering::Relaxed)))
}

fn acquire_connection_slot(state: &AppState, request_id: &str) -> Result<OwnedSemaphorePermit, ()> {
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

fn json_response(status: StatusCode, request_id: String, value: Value) -> Response {
    let response = (status, Json(value)).into_response();
    response_with_request_id(response, request_id)
}

fn response_with_request_id(mut response: Response, request_id: String) -> Response {
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    response
}

fn text_response(
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

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "cookie" | "set-cookie"
    )
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let config = Config::default();
        let readiness = Readiness::with_required(["http".to_owned(), "grpc_h2c".to_owned()]);
        readiness.mark("http", true);
        readiness.mark("grpc_h2c", true);
        let events = EventStore::new(200);
        let config = Arc::new(config);
        AppState {
            graphql: graphql::build_schema(config.clone(), events.clone()),
            bound: BoundAddresses {
                http: "127.0.0.1:8080".parse().unwrap(),
                grpc_h2c: "127.0.0.1:9000".parse().unwrap(),
                grpc_tls: None,
                tcp: "127.0.0.1:7000".parse().unwrap(),
                udp: "127.0.0.1:7001".parse().unwrap(),
                thrift: "127.0.0.1:9090".parse().unwrap(),
                mqtt: None,
            },
            events,
            request_seq: Arc::new(AtomicU64::new(1)),
            connection_slots: Arc::new(Semaphore::new(config.max_connections)),
            rooms: Arc::new(Mutex::new(HashMap::new())),
            config,
            readiness,
        }
    }

    #[tokio::test]
    async fn anything_echoes_request_and_redacts_sensitive_headers() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/http/anything/demo?q=1")
                    .header("x-request-id", "test-request")
                    .header("authorization", "Bearer secret")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-request-id"], "test-request");
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["method"], "POST");
        assert_eq!(value["path"], "demo");
        assert_eq!(value["body"], "hello");
        assert_eq!(value["headers"]["authorization"][0], "[REDACTED]");
    }

    #[tokio::test]
    async fn status_and_compression_endpoints_are_deterministic() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/http/status/418")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/http/gzip")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let mut decoder = GzDecoder::new(bytes.as_ref());
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).unwrap();
        assert_eq!(decoded, r#"{"message":"biubin compressed response"}"#);
    }

    #[tokio::test]
    async fn sse_endpoint_contains_event_fields() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/sse/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("event: message"));
        assert!(body.contains("id: 1"));
        assert!(body.contains("retry: 1000"));
    }

    #[tokio::test]
    async fn middleware_adds_request_ids_to_rejections() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .uri("/not-found")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            response.headers()["x-request-id"]
                .as_bytes()
                .starts_with(b"req_")
        );

        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/http/anything/limited")
                    .body(Body::from(vec![b'x'; 2 * 1024 * 1024 + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(response.headers().contains_key("x-request-id"));
    }
}
