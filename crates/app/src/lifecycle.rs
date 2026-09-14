use crate::addresses::{BoundAddresses, MqttAddresses, host_port};
use crate::{graphql, grpc, http, mqtt, state::AppState, tcp, thrift, udp};
use biubin_core::{Config, EventStore, Readiness};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinSet;
use tracing::info;

type ListenerTaskResult = (
    &'static str,
    Result<(), Box<dyn std::error::Error + Send + Sync>>,
);

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let mut config = Config::from_env().map_err(|error| format!("configuration error: {error}"))?;
    mqtt::normalize_ephemeral_ports(&mut config)
        .map_err(|error| format!("MQTT configuration error: {error}"))?;
    if config.grpc_tls_enabled || config.mqtt_tls_enabled {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }
    if config.grpc_tls_enabled {
        grpc::validate_grpc_tls_config(&config)?;
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

    let app = http::router(state.clone());
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
                grpc::run_grpc_tls(grpc_tls_listener, state_for_tls, async move {
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
            tcp::run(
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
            udp::run(
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
            grpc::run(
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

    supervise_listeners(tasks, shutdown_requested, state.readiness.clone()).await;
    let _ = thrift_signal.await;
    thrift_handle.join();
    Ok(())
}

async fn supervise_listeners(
    mut tasks: JoinSet<ListenerTaskResult>,
    shutdown_requested: Arc<std::sync::atomic::AtomicBool>,
    readiness: Readiness,
) {
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((name, Ok(()))) => {
                if shutdown_requested.load(Ordering::Acquire) {
                    tracing::debug!(listener = name, "listener stopped during shutdown");
                } else {
                    readiness.mark(name, false);
                    tracing::error!(listener = name, "listener stopped without shutdown request");
                }
            }
            Ok((name, Err(error))) => {
                tracing::error!(listener = name, %error, "listener stopped unexpectedly");
                readiness.mark(name, false);
            }
            Err(error) => {
                tracing::error!(%error, "listener task panicked");
            }
        }
    }
}

pub(crate) fn healthcheck() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

pub(crate) async fn shutdown_signal() {
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

    #[tokio::test]
    async fn unexpected_listener_exit_marks_readiness_unready() {
        let readiness = Readiness::with_required(["http".to_owned()]);
        readiness.mark("http", true);
        let shutdown_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = JoinSet::new();
        tasks.spawn(async { ("http", Ok(())) });

        supervise_listeners(tasks, shutdown_requested, readiness.clone()).await;

        assert!(!readiness.is_ready());
    }

    #[tokio::test]
    async fn listener_error_marks_readiness_unready() {
        let readiness = Readiness::with_required(["grpc".to_owned()]);
        readiness.mark("grpc", true);
        let shutdown_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            (
                "grpc",
                Err::<(), Box<dyn std::error::Error + Send + Sync>>("listener failed".into()),
            )
        });

        supervise_listeners(tasks, shutdown_requested, readiness.clone()).await;

        assert!(!readiness.is_ready());
    }

    #[tokio::test]
    async fn graceful_listener_exit_keeps_readiness_during_shutdown() {
        let readiness = Readiness::with_required(["http".to_owned()]);
        readiness.mark("http", true);
        let shutdown_requested = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut tasks = JoinSet::new();
        tasks.spawn(async { ("http", Ok(())) });

        supervise_listeners(tasks, shutdown_requested, readiness.clone()).await;

        assert!(readiness.is_ready());
    }
}
