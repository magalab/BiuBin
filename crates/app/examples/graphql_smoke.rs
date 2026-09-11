use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{ClientRequestBuilder, Message, http::Uri},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:8080/graphql/ws".to_owned());
    let request =
        ClientRequestBuilder::new(url.parse::<Uri>()?).with_sub_protocol("graphql-transport-ws");
    let (mut socket, _) = connect_async(request).await?;
    socket
        .send(Message::Text(
            r#"{"type":"connection_init"}"#.to_owned().into(),
        ))
        .await?;
    let ack = next_text(&mut socket).await?;
    assert!(ack.contains("connection_ack"), "unexpected ack: {ack}");
    socket
        .send(Message::Text(
            r#"{"id":"1","type":"subscribe","payload":{"query":"subscription { ticker(intervalMs: 10, count: 2) { sequence message } }"}}"#
                .to_owned()
                .into(),
        ))
        .await?;
    let first = next_text(&mut socket).await?;
    let second = next_text(&mut socket).await?;
    assert!(
        first.contains("\"sequence\":1"),
        "unexpected first event: {first}"
    );
    assert!(
        second.contains("\"sequence\":2"),
        "unexpected second event: {second}"
    );
    let complete = next_text(&mut socket).await?;
    assert!(
        complete.contains("complete"),
        "unexpected completion: {complete}"
    );
    println!("graphql websocket subscription passed");
    Ok(())
}

async fn next_text<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Result<String, Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => return Ok(text.to_string()),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(frame) => return Err(format!("websocket closed: {frame:?}").into()),
            _ => {}
        }
    }
    Err("websocket ended before a text message".into())
}
