pub(crate) const INDEX_HTML: &str = include_str!(concat!(env!("OUT_DIR"), "/index.html"));
pub(crate) const GRPC_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/biubin_descriptor.bin"));

pub(crate) mod embedded_web {
    include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));
}

pub(crate) mod proto {
    tonic::include_proto!("biubin.v1");
}
