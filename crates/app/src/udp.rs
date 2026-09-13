use crate::state::MAX_UDP_PACKET_SIZE;
use biubin_core::{Config, EventStore};
use std::sync::Arc;
use tokio::net::UdpSocket;

pub(crate) async fn run<F>(
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
