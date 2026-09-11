use biubin_core::{Config, EventStore};
use rumqttd::{
    Broker, Config as BrokerConfig, ConnectionSettings, Meter, Notification, RouterConfig,
    ServerSettings,
};
use serde_json::json;
use std::collections::HashMap;
use std::io::BufReader;
use std::net::{
    SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream, ToSocketAddrs,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};

const MAX_MQTT_PAYLOAD: usize = 1024 * 1024;

pub(crate) fn normalize_ephemeral_ports(config: &mut Config) -> Result<(), String> {
    if !config.mqtt_enabled {
        return Ok(());
    }
    let bind_host = config.bind_host.clone();
    allocate_port(&bind_host, &mut config.ports.mqtt_tcp)?;
    allocate_port(&bind_host, &mut config.ports.mqtt_auth_tcp)?;
    allocate_port(&bind_host, &mut config.ports.mqtt_v5)?;
    allocate_port(&bind_host, &mut config.ports.mqtt_ws)?;
    if config.mqtt_tls_enabled {
        allocate_port(&bind_host, &mut config.ports.mqtt_tls)?;
        allocate_port("127.0.0.1", &mut config.ports.mqtt_tls_backend)?;
    }
    Ok(())
}

fn allocate_port(host: &str, port: &mut u16) -> Result<(), String> {
    if *port != 0 {
        return Ok(());
    }
    let listener = StdTcpListener::bind(socket_addr(host, 0)?)
        .map_err(|error| format!("allocate MQTT port on {host}: {error}"))?;
    *port = listener
        .local_addr()
        .map_err(|error| format!("read allocated MQTT port: {error}"))?
        .port();
    Ok(())
}

/// Starts the embedded MQTT broker. TLS is terminated by an in-process proxy
/// so normal TLS and mTLS can share the same public port without a sidecar.
pub fn spawn(config: &Config, events: &EventStore) -> Result<(), String> {
    let (broker_config, tls_proxy) = build_config(config)?;
    let tls_acceptor = tls_proxy
        .as_ref()
        .map(|proxy| build_tls_config(&proxy.config).map(TlsAcceptor::from))
        .transpose()?;
    let mut broker = Broker::new(broker_config);
    let (mut link_tx, mut link_rx) = broker
        .link("biubin-events")
        .map_err(|error| format!("create MQTT event link: {error}"))?;
    link_tx
        .subscribe("#")
        .map_err(|error| format!("subscribe MQTT event link: {error}"))?;
    let meters = broker
        .meters()
        .map_err(|error| format!("create MQTT meter link: {error}"))?;

    let events_for_thread = events.clone();
    std::thread::Builder::new()
        .name("biubin-mqtt-broker".to_owned())
        .spawn(move || {
            if let Err(error) = broker.start() {
                tracing::error!(%error, "embedded MQTT broker stopped");
            }
        })
        .map_err(|error| format!("spawn MQTT broker: {error}"))?;

    std::thread::Builder::new()
        .name("biubin-mqtt-events".to_owned())
        .spawn(move || {
            while let Ok(notification) = link_rx.recv() {
                let Some(Notification::Forward(forward)) = notification else {
                    continue;
                };
                let topic = String::from_utf8_lossy(&forward.publish.topic);
                events_for_thread.push_with_attributes(
                    "mqtt",
                    "message_received",
                    format!("PUBLISH {topic}"),
                    HashMap::from([
                        ("topic".to_owned(), json!(topic.to_string())),
                        (
                            "payload_bytes".to_owned(),
                            json!(forward.publish.payload.len()),
                        ),
                        ("retain".to_owned(), json!(forward.publish.retain)),
                    ])
                    .into_iter()
                    .collect(),
                );
            }
        })
        .map_err(|error| format!("spawn MQTT event collector: {error}"))?;

    let events_for_meters = events.clone();
    std::thread::Builder::new()
        .name("biubin-mqtt-meters".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "build MQTT meter runtime");
                    return;
                }
            };
            runtime.block_on(async move {
                while let Ok(meters) = meters.next().await {
                    for meter in meters {
                        match meter {
                            Meter::Router(_, meter) => {
                                events_for_meters.push(
                                    "mqtt",
                                    "connection_summary",
                                    format!(
                                        "connections={} subscriptions={} publishes={}",
                                        meter.total_connections,
                                        meter.total_subscriptions,
                                        meter.total_publishes
                                    ),
                                );
                            }
                            Meter::Subscription(filter, meter) => {
                                events_for_meters.push(
                                    "mqtt",
                                    "subscription_summary",
                                    format!("filter={filter} messages={}", meter.count),
                                );
                            }
                        }
                    }
                }
            });
        })
        .map_err(|error| format!("spawn MQTT meter collector: {error}"))?;

    if let (Some(proxy), Some(acceptor)) = (tls_proxy, tls_acceptor) {
        spawn_tls_proxy(proxy.listener, proxy.backend_addr, acceptor, events.clone())?;
    }

    for port in [
        config.ports.mqtt_tcp,
        config.ports.mqtt_auth_tcp,
        config.ports.mqtt_v5,
        config.ports.mqtt_ws,
    ] {
        wait_for_listener(socket_addr(&config.bind_host, port)?)?;
    }
    if config.mqtt_tls_enabled {
        wait_for_listener(socket_addr("127.0.0.1", config.ports.mqtt_tls_backend)?)?;
        wait_for_listener(socket_addr(&config.bind_host, config.ports.mqtt_tls)?)?;
    }

    Ok(())
}

fn wait_for_listener(address: SocketAddr) -> Result<(), String> {
    for _ in 0..500 {
        if StdTcpStream::connect_timeout(&address, Duration::from_millis(50)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err(format!("MQTT listener did not become ready at {address}"))
}

struct TlsProxyConfig {
    cert_path: String,
    key_path: String,
    client_ca_path: String,
    mode: String,
}

struct TlsProxy {
    listener: StdTcpListener,
    backend_addr: SocketAddr,
    config: TlsProxyConfig,
}

fn build_config(config: &Config) -> Result<(BrokerConfig, Option<TlsProxy>), String> {
    let anonymous = connection_settings(None);
    let authenticated = connection_settings(Some(HashMap::from([(
        config.mqtt_username.clone(),
        config.mqtt_password.clone(),
    )])));

    let mut v4 = HashMap::new();
    v4.insert(
        "mqtt-anonymous".to_owned(),
        server_settings(
            "mqtt-anonymous",
            socket_addr(&config.bind_host, config.ports.mqtt_tcp)?,
            None,
            anonymous.clone(),
        ),
    );
    v4.insert(
        "mqtt-auth".to_owned(),
        server_settings(
            "mqtt-auth",
            socket_addr(&config.bind_host, config.ports.mqtt_auth_tcp)?,
            None,
            authenticated.clone(),
        ),
    );

    let mut v5 = HashMap::new();
    v5.insert(
        "mqtt-v5".to_owned(),
        server_settings(
            "mqtt-v5",
            socket_addr(&config.bind_host, config.ports.mqtt_v5)?,
            None,
            authenticated,
        ),
    );

    let mut ws = HashMap::new();
    ws.insert(
        "mqtt-websocket".to_owned(),
        server_settings(
            "mqtt-websocket",
            socket_addr(&config.bind_host, config.ports.mqtt_ws)?,
            None,
            anonymous,
        ),
    );

    let tls_proxy = if config.mqtt_tls_enabled {
        for path in [&config.tls_cert_path, &config.tls_key_path] {
            if !Path::new(path).exists() {
                return Err(format!("MQTT TLS file does not exist: {path}"));
            }
        }
        if config.mqtt_tls_mode == "mtls" && !Path::new(&config.tls_client_ca_path).exists() {
            return Err(format!(
                "MQTT mTLS client CA file does not exist: {}",
                config.tls_client_ca_path
            ));
        }

        let backend_addr = socket_addr("127.0.0.1", config.ports.mqtt_tls_backend)?;
        v4.insert(
            "mqtt-tls-backend".to_owned(),
            server_settings(
                "mqtt-tls-backend",
                backend_addr,
                None,
                connection_settings(Some(HashMap::from([(
                    config.mqtt_username.clone(),
                    config.mqtt_password.clone(),
                )]))),
            ),
        );
        let public_addr = socket_addr(&config.bind_host, config.ports.mqtt_tls)?;
        let listener = StdTcpListener::bind(public_addr)
            .map_err(|error| format!("bind MQTT TLS listener {public_addr}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("configure MQTT TLS listener {public_addr}: {error}"))?;
        Some(TlsProxy {
            listener,
            backend_addr,
            config: TlsProxyConfig {
                cert_path: config.tls_cert_path.clone(),
                key_path: config.tls_key_path.clone(),
                client_ca_path: config.tls_client_ca_path.clone(),
                mode: config.mqtt_tls_mode.clone(),
            },
        })
    } else {
        None
    };

    for (name, address) in [
        (
            "MQTT anonymous",
            socket_addr(&config.bind_host, config.ports.mqtt_tcp)?,
        ),
        (
            "MQTT authenticated",
            socket_addr(&config.bind_host, config.ports.mqtt_auth_tcp)?,
        ),
        (
            "MQTT v5",
            socket_addr(&config.bind_host, config.ports.mqtt_v5)?,
        ),
        (
            "MQTT WebSocket",
            socket_addr(&config.bind_host, config.ports.mqtt_ws)?,
        ),
    ] {
        let listener = StdTcpListener::bind(address)
            .map_err(|error| format!("preflight {name} listener {address}: {error}"))?;
        drop(listener);
    }
    if config.mqtt_tls_enabled {
        let backend = socket_addr("127.0.0.1", config.ports.mqtt_tls_backend)?;
        let listener = StdTcpListener::bind(backend)
            .map_err(|error| format!("preflight MQTT TLS backend {backend}: {error}"))?;
        drop(listener);
    }

    Ok((
        BrokerConfig {
            id: 0,
            router: RouterConfig {
                max_connections: config.max_connections,
                max_outgoing_packet_count: 200,
                max_segment_size: 16 * 1024 * 1024,
                max_segment_count: 4,
                custom_segment: None,
                initialized_filters: None,
                ..Default::default()
            },
            v4: Some(v4),
            v5: Some(v5),
            ws: Some(ws),
            cluster: None,
            console: None,
            bridge: None,
            prometheus: None,
            metrics: None,
        },
        tls_proxy,
    ))
}

fn server_settings(
    name: &str,
    listen: SocketAddr,
    tls: Option<rumqttd::TlsConfig>,
    connections: ConnectionSettings,
) -> ServerSettings {
    ServerSettings {
        name: name.to_owned(),
        listen,
        tls,
        next_connection_delay_ms: 1,
        connections,
    }
}

fn connection_settings(auth: Option<HashMap<String, String>>) -> ConnectionSettings {
    ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: MAX_MQTT_PAYLOAD,
        max_inflight_count: 100,
        auth,
        external_auth: None,
        dynamic_filters: true,
    }
}

fn socket_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let address = if host
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_ipv6())
    {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    address
        .to_socket_addrs()
        .map_err(|error| format!("resolve MQTT listener {host}:{port}: {error}"))?
        .next()
        .ok_or_else(|| format!("no address for MQTT listener {host}:{port}"))
}

fn spawn_tls_proxy(
    listener: StdTcpListener,
    backend_addr: SocketAddr,
    acceptor: TlsAcceptor,
    events: EventStore,
) -> Result<(), String> {
    std::thread::Builder::new()
        .name("biubin-mqtt-tls".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "build MQTT TLS runtime");
                    return;
                }
            };
            runtime.block_on(async move {
                if let Err(error) = run_tls_proxy(listener, backend_addr, acceptor, events).await {
                    tracing::error!(%error, "MQTT TLS proxy stopped");
                }
            });
        })
        .map_err(|error| format!("spawn MQTT TLS proxy: {error}"))?;
    Ok(())
}

async fn run_tls_proxy(
    listener: StdTcpListener,
    backend_addr: SocketAddr,
    acceptor: TlsAcceptor,
    events: EventStore,
) -> Result<(), String> {
    let listener = TcpListener::from_std(listener)
        .map_err(|error| format!("create MQTT TLS async listener: {error}"))?;
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .map_err(|error| format!("accept MQTT TLS connection: {error}"))?;
        let acceptor = acceptor.clone();
        let events = events.clone();
        tokio::spawn(async move {
            if let Err(error) = proxy_tls_connection(stream, peer, backend_addr, acceptor).await {
                tracing::debug!(%error, ?peer, "MQTT TLS connection closed");
            }
            events.push("mqtt", "tls_connection_closed", format!("TLS {peer}"));
        });
    }
}

async fn proxy_tls_connection(
    stream: TcpStream,
    peer: SocketAddr,
    backend_addr: SocketAddr,
    acceptor: TlsAcceptor,
) -> Result<(), String> {
    let mut tls_stream = acceptor
        .accept(stream)
        .await
        .map_err(|error| format!("MQTT TLS handshake from {peer}: {error}"))?;
    let mut backend = TcpStream::connect(backend_addr)
        .await
        .map_err(|error| format!("connect MQTT TLS backend {backend_addr}: {error}"))?;
    copy_bidirectional(&mut tls_stream, &mut backend)
        .await
        .map_err(|error| format!("proxy MQTT TLS connection from {peer}: {error}"))?;
    Ok(())
}

fn build_tls_config(config: &TlsProxyConfig) -> Result<Arc<ServerConfig>, String> {
    // The provider may already have been selected by another TLS adapter in
    // this process. In that case the existing process-wide provider is valid.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let certificates = load_certificates(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;
    let builder = ServerConfig::builder();
    let server = if config.mode == "mtls" {
        let roots = load_root_store(&config.client_ca_path)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| format!("build MQTT mTLS client verifier: {error}"))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, key)
            .map_err(|error| format!("build MQTT mTLS server config: {error}"))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|error| format!("build MQTT TLS server config: {error}"))?
    };
    Ok(Arc::new(server))
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("open TLS certificate {path}: {error}"))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read TLS certificate {path}: {error}"))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let file =
        std::fs::File::open(path).map_err(|error| format!("open TLS key {path}: {error}"))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| format!("read TLS key {path}: {error}"))?
        .ok_or_else(|| format!("TLS key {path} does not contain a private key"))
}

fn load_root_store(path: &str) -> Result<RootCertStore, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("open TLS CA {path}: {error}"))?;
    let mut roots = RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut BufReader::new(file)) {
        let certificate = certificate.map_err(|error| format!("read TLS CA {path}: {error}"))?;
        roots
            .add(certificate)
            .map_err(|error| format!("add TLS CA {path}: {error}"))?;
    }
    if roots.is_empty() {
        return Err(format!("TLS CA {path} does not contain certificates"));
    }
    Ok(roots)
}
