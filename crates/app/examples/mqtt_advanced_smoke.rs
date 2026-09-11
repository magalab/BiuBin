use rumqttc::{AsyncClient, Event, EventLoop, LastWill, MqttOptions, Packet, QoS};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(8);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".to_owned());
    let port = args
        .next()
        .unwrap_or_else(|| "1883".to_owned())
        .parse::<u16>()?;

    qos2_round_trip(&host, port).await?;
    retain_round_trip(&host, port).await?;
    will_round_trip(&host, port).await?;
    persistent_session_round_trip(&host, port).await?;
    println!("mqtt qos2/retain/will/session smoke passed");
    Ok(())
}

async fn qos2_round_trip(
    host: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let topic = "biubin/advanced/qos2";
    let (subscriber, mut subscriber_loop) = client("advanced-qos2-sub", host, port, true, None);
    subscriber.subscribe(topic, QoS::ExactlyOnce).await?;
    wait_for_packet(&mut subscriber_loop, |packet| {
        matches!(packet, Packet::SubAck(_))
    })
    .await?;

    let (publisher, mut publisher_loop) = client("advanced-qos2-pub", host, port, true, None);
    publisher
        .publish(topic, QoS::ExactlyOnce, false, "qos2-payload")
        .await?;
    let publisher_task = tokio::spawn(async move {
        wait_for_packet(&mut publisher_loop, |packet| {
            matches!(packet, Packet::PubComp(_) | Packet::PubAck(_))
        })
        .await
    });
    let message = wait_for_publish(&mut subscriber_loop, topic).await?;
    publisher_task.await??;
    assert_eq!(message.qos, QoS::ExactlyOnce);
    assert_eq!(message.payload.as_ref(), b"qos2-payload");
    Ok(())
}

async fn retain_round_trip(
    host: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let topic = "biubin/advanced/retain";
    let (publisher, mut publisher_loop) = client("advanced-retain-pub", host, port, true, None);
    publisher
        .publish(topic, QoS::AtLeastOnce, true, "retained-payload")
        .await?;
    wait_for_packet(&mut publisher_loop, |packet| {
        matches!(packet, Packet::PubAck(_))
    })
    .await?;

    let (subscriber, mut subscriber_loop) = client("advanced-retain-sub", host, port, true, None);
    subscriber.subscribe(topic, QoS::AtLeastOnce).await?;
    let message = wait_for_publish(&mut subscriber_loop, topic).await?;
    assert!(message.retain);
    assert_eq!(message.payload.as_ref(), b"retained-payload");
    Ok(())
}

async fn will_round_trip(
    host: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let topic = "biubin/advanced/will";
    let (watcher, mut watcher_loop) = client("advanced-will-watch", host, port, true, None);
    watcher.subscribe(topic, QoS::AtLeastOnce).await?;
    wait_for_packet(&mut watcher_loop, |packet| {
        matches!(packet, Packet::SubAck(_))
    })
    .await?;

    let will = LastWill::new(topic, "unexpected disconnect", QoS::AtLeastOnce, false);
    let (willer, mut willer_loop) = client("advanced-will-client", host, port, true, Some(will));
    wait_for_packet(&mut willer_loop, |packet| {
        matches!(packet, Packet::ConnAck(_))
    })
    .await?;
    drop(willer_loop);
    drop(willer);

    let message = wait_for_publish(&mut watcher_loop, topic).await?;
    assert_eq!(message.payload.as_ref(), b"unexpected disconnect");
    Ok(())
}

async fn persistent_session_round_trip(
    host: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_id = "advanced-persistent-client";
    let topic = "biubin/advanced/offline";
    let (session_client, mut session_loop) = client(client_id, host, port, false, None);
    session_client.subscribe(topic, QoS::AtLeastOnce).await?;
    wait_for_packet(&mut session_loop, |packet| {
        matches!(packet, Packet::SubAck(_))
    })
    .await?;
    drop(session_loop);
    drop(session_client);

    let (publisher, mut publisher_loop) = client("advanced-offline-pub", host, port, true, None);
    publisher
        .publish(topic, QoS::AtLeastOnce, false, "offline-payload")
        .await?;
    wait_for_packet(&mut publisher_loop, |packet| {
        matches!(packet, Packet::PubAck(_))
    })
    .await?;

    let (_restored, mut restored_loop) = client(client_id, host, port, false, None);
    let connack = wait_for_packet(&mut restored_loop, |packet| {
        matches!(packet, Packet::ConnAck(_))
    })
    .await?;
    if let Packet::ConnAck(connack) = connack {
        assert!(
            connack.session_present,
            "persistent session was not restored"
        );
    }
    let message = wait_for_publish(&mut restored_loop, topic).await?;
    assert_eq!(message.payload.as_ref(), b"offline-payload");
    Ok(())
}

fn client(
    client_id: &str,
    host: &str,
    port: u16,
    clean_session: bool,
    will: Option<LastWill>,
) -> (AsyncClient, EventLoop) {
    let mut options = MqttOptions::new(client_id, host, port);
    options
        .set_keep_alive(Duration::from_secs(2))
        .set_clean_session(clean_session);
    if let Some(will) = will {
        options.set_last_will(will);
    }
    AsyncClient::new(options, 32)
}

async fn wait_for_publish(
    eventloop: &mut EventLoop,
    topic: &str,
) -> Result<rumqttc::mqttbytes::v4::Publish, Box<dyn std::error::Error + Send + Sync>> {
    let topic = topic.to_owned();
    let packet = wait_for_packet(
        eventloop,
        move |packet| matches!(packet, Packet::Publish(publish) if publish.topic == topic),
    )
    .await?;
    match packet {
        Packet::Publish(publish) => Ok(publish),
        _ => unreachable!("predicate only accepts Publish"),
    }
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
                return Ok::<Packet, rumqttc::ConnectionError>(packet);
            }
        }
    })
    .await??;
    Ok(packet)
}
