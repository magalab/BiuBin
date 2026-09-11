use rumqttc::v5::mqttbytes::{
    QoS,
    v5::{ConnectProperties, Packet},
};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(8);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".to_owned());
    let port = args
        .next()
        .unwrap_or_else(|| "1885".to_owned())
        .parse::<u16>()?;
    let username = args.next().unwrap_or_else(|| "biubin".to_owned());
    let password = args.next().unwrap_or_else(|| "biubin-dev".to_owned());

    let topic = "biubin/v5/smoke";
    let mut subscriber_options = MqttOptions::new("biubin-v5-subscriber", &host, port);
    let mut properties = ConnectProperties::new();
    properties.session_expiry_interval = Some(30);
    subscriber_options
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_start(false)
        .set_credentials(&username, &password)
        .set_connect_properties(properties);
    let (subscriber, mut subscriber_loop) = AsyncClient::new(subscriber_options, 32);
    subscriber.subscribe(topic, QoS::AtLeastOnce).await?;
    wait_for_packet(&mut subscriber_loop, |packet| {
        matches!(packet, Packet::SubAck(_))
    })
    .await?;

    let mut publisher_options = MqttOptions::new("biubin-v5-publisher", &host, port);
    publisher_options
        .set_keep_alive(Duration::from_secs(5))
        .set_credentials(&username, &password);
    let (publisher, mut publisher_loop) = AsyncClient::new(publisher_options, 32);
    publisher
        .publish(topic, QoS::AtLeastOnce, false, "mqtt v5 payload")
        .await?;
    let publisher_task = tokio::spawn(async move {
        wait_for_packet(&mut publisher_loop, |packet| {
            matches!(packet, Packet::PubAck(_))
        })
        .await
    });

    let message = wait_for_packet(&mut subscriber_loop, |packet| {
        matches!(packet, Packet::Publish(publish) if publish.topic.as_ref() == topic.as_bytes())
    })
    .await?;
    publisher_task.await??;
    if let Packet::Publish(publish) = message {
        assert_eq!(publish.qos, QoS::AtLeastOnce);
        assert_eq!(publish.payload.as_ref(), b"mqtt v5 payload");
    }
    println!("mqtt v5 authenticated/session smoke passed");
    Ok(())
}

async fn wait_for_packet(
    eventloop: &mut EventLoop,
    predicate: impl Fn(&Packet) -> bool,
) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
    let packet = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Event::Incoming(packet) = eventloop.poll().await?
                && predicate(&packet)
            {
                return Ok::<Packet, rumqttc::v5::ConnectionError>(packet);
            }
        }
    })
    .await??;
    Ok(packet)
}
