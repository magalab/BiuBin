use futures_util::StreamExt;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

pub mod proto {
    tonic::include_proto!("biubin.v1");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".to_owned());
    let port = args
        .next()
        .unwrap_or_else(|| "9000".to_owned())
        .parse::<u16>()?;
    let mode = args.next().unwrap_or_else(|| "h2c".to_owned());
    let ca_path = args.next();
    let client_cert_path = args.next();
    let client_key_path = args.next();

    if matches!(mode.as_str(), "tls" | "mtls") {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }

    let scheme = if mode == "h2c" { "http" } else { "https" };
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{host}:{port}"))?
        .connect_timeout(Duration::from_secs(5));
    if matches!(mode.as_str(), "tls" | "mtls") {
        let ca = std::fs::read(ca_path.ok_or("TLS mode requires a CA path")?)?;
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca))
            .domain_name("localhost");
        if mode == "mtls" {
            let cert =
                std::fs::read(client_cert_path.ok_or("mTLS requires a client certificate")?)?;
            let key = std::fs::read(client_key_path.ok_or("mTLS requires a client key")?)?;
            tls = tls.identity(Identity::from_pem(cert, key));
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    let channel = endpoint.connect().await?;
    exercise(channel).await?;
    println!("gRPC {mode} unary/streaming/error smoke passed");
    Ok(())
}

async fn exercise(channel: Channel) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = proto::biubin_client::BiubinClient::new(channel.clone());
    let index = client
        .index(proto::IndexRequest {
            client_name: "grpc-smoke".to_owned(),
        })
        .await?
        .into_inner();
    assert_eq!(index.service, "biubin");

    let mut unary = tonic::Request::new(proto::EchoRequest {
        message: "hello grpc".to_owned(),
        payload: b"payload".to_vec(),
        count: 1,
        interval_ms: 0,
    });
    unary
        .metadata_mut()
        .insert("x-smoke", MetadataValue::from_static("present"));
    let unary = client.echo_unary(unary).await?.into_inner();
    assert_eq!(unary.message, "hello grpc");
    assert_eq!(unary.payload, b"payload");
    assert!(unary.metadata_keys.iter().any(|key| key == "x-smoke"));

    let mut stream = client
        .echo_server_stream(proto::EchoRequest {
            message: "server stream".to_owned(),
            payload: Vec::new(),
            count: 3,
            interval_ms: 0,
        })
        .await?
        .into_inner();
    let mut server_stream_count = 0;
    while let Some(response) = stream.message().await? {
        server_stream_count += 1;
        assert_eq!(response.message, "server stream");
    }
    assert_eq!(server_stream_count, 3);

    let client_stream = tokio_stream::iter(vec![
        proto::EchoRequest {
            message: "one".to_owned(),
            payload: vec![1],
            count: 0,
            interval_ms: 0,
        },
        proto::EchoRequest {
            message: "two".to_owned(),
            payload: vec![2],
            count: 0,
            interval_ms: 0,
        },
    ]);
    let summary = client.echo_client_stream(client_stream).await?.into_inner();
    assert_eq!(summary.count, 2);
    assert_eq!(summary.last_message, "two");

    let bidi_input = tokio_stream::iter(vec![
        proto::EchoRequest {
            message: "bidi one".to_owned(),
            payload: Vec::new(),
            count: 0,
            interval_ms: 0,
        },
        proto::EchoRequest {
            message: "bidi two".to_owned(),
            payload: Vec::new(),
            count: 0,
            interval_ms: 0,
        },
    ]);
    let mut bidi = client.echo_bidi_stream(bidi_input).await?.into_inner();
    let mut bidi_count = 0;
    while let Some(response) = bidi.next().await {
        let response = response?;
        bidi_count += 1;
        assert!(response.message.starts_with("bidi "));
    }
    assert_eq!(bidi_count, 2);

    let error = client
        .error(proto::ErrorRequest {
            code: 3,
            message: "bad input".to_owned(),
            include_trailer: true,
        })
        .await
        .expect_err("Error RPC should return a status");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "bad input");
    assert_eq!(
        error
            .metadata()
            .get("biubin-trailer")
            .and_then(|value| value.to_str().ok()),
        Some("present")
    );
    Ok(())
}
