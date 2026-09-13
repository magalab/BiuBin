mod addresses;
mod generated;
mod graphql;
mod grpc;
mod http;
mod lifecycle;
mod mqtt;
mod state;
mod tcp;
mod thrift;
mod udp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return lifecycle::healthcheck();
    }
    lifecycle::run().await
}
