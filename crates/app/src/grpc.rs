use crate::generated::{GRPC_DESCRIPTOR_SET, proto};
use crate::state::AppState;
use biubin_core::{Config, EventStore};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server as GrpcServer;
use tonic::transport::{Certificate as GrpcCertificate, Identity, ServerTlsConfig};
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status as GrpcStatus, Streaming};

pub(crate) fn validate_grpc_tls_config(
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

pub(crate) async fn run<F>(
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

pub(crate) async fn run_grpc_tls<F>(
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
