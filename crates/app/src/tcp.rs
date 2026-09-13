use crate::state::{MAX_SOCKET_FRAME_SIZE, MAX_SOCKET_READ_SIZE, SOCKET_IDLE_TIMEOUT_SECS};
use biubin_core::{Config, EventStore};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

pub(crate) async fn run<F>(
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
