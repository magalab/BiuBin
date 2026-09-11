use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".to_owned());
    let port = args
        .next()
        .unwrap_or_else(|| "1883".to_owned())
        .parse::<u16>()?;
    let third = args.next();
    let (username, password, transport) = match third.as_deref() {
        Some("tcp" | "tls" | "ws") => (None, None, third.unwrap_or_else(|| "tcp".to_owned())),
        _ => (
            third,
            args.next(),
            args.next().unwrap_or_else(|| "tcp".to_owned()),
        ),
    };
    let ca_path = args.next();
    let client_cert_path = args.next();
    let client_key_path = args.next();

    let mut options = if transport == "ws" {
        MqttOptions::new("biubin-smoke", format!("ws://{host}:{port}/mqtt"), port)
    } else {
        MqttOptions::new("biubin-smoke", host, port)
    };
    options.set_keep_alive(Duration::from_secs(5));
    if let (Some(username), Some(password)) = (username, password) {
        options.set_credentials(username, password);
    }
    match transport.as_str() {
        "tcp" => {}
        "tls" => {
            let ca = std::fs::read(ca_path.ok_or("tls transport requires a CA path")?)?;
            let client_auth = match (client_cert_path, client_key_path) {
                (Some(cert), Some(key)) => Some((std::fs::read(cert)?, std::fs::read(key)?)),
                (None, None) => None,
                _ => return Err("client certificate and key must be provided together".into()),
            };
            options.set_transport(rumqttc::Transport::tls(ca, client_auth, None));
        }
        "ws" => {
            options.set_transport(rumqttc::Transport::ws());
        }
        _ => return Err(format!("unsupported transport: {transport}").into()),
    }
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    client.subscribe("biubin/smoke/#", QoS::AtLeastOnce).await?;
    client
        .publish(
            "biubin/smoke/hello",
            QoS::AtLeastOnce,
            false,
            b"hello from biubin",
        )
        .await?;

    loop {
        if let Event::Incoming(Packet::Publish(message)) = eventloop.poll().await? {
            println!(
                "topic={} qos={:?} payload={}",
                message.topic,
                message.qos,
                String::from_utf8_lossy(&message.payload)
            );
            break;
        }
    }
    Ok(())
}
