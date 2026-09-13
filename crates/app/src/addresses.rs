use serde_json::{Value, json};
use std::net::{IpAddr, SocketAddr};

#[derive(Clone, Debug)]
pub(crate) struct BoundAddresses {
    pub(crate) http: SocketAddr,
    pub(crate) grpc_h2c: SocketAddr,
    pub(crate) grpc_tls: Option<SocketAddr>,
    pub(crate) tcp: SocketAddr,
    pub(crate) udp: SocketAddr,
    pub(crate) thrift: SocketAddr,
    pub(crate) mqtt: Option<MqttAddresses>,
}

#[derive(Clone, Debug)]
pub(crate) struct MqttAddresses {
    pub(crate) tcp: u16,
    pub(crate) auth_tcp: u16,
    pub(crate) v5: u16,
    pub(crate) tls: Option<u16>,
    pub(crate) ws: u16,
}

impl BoundAddresses {
    pub(crate) fn bound_json(&self) -> Value {
        json!({
            "http": self.http.to_string(),
            "grpc_h2c": self.grpc_h2c.to_string(),
            "grpc_tls": self.grpc_tls.map(|address| address.to_string()),
            "tcp": self.tcp.to_string(),
            "udp": self.udp.to_string(),
            "thrift": self.thrift.to_string(),
            "mqtt": self.mqtt.as_ref().map(|mqtt| json!({
                "tcp": SocketAddr::new(self.http.ip(), mqtt.tcp).to_string(),
                "auth_tcp": SocketAddr::new(self.http.ip(), mqtt.auth_tcp).to_string(),
                "v5": SocketAddr::new(self.http.ip(), mqtt.v5).to_string(),
                "tls": mqtt.tls.map(|port| SocketAddr::new(self.http.ip(), port).to_string()),
                "ws": SocketAddr::new(self.http.ip(), mqtt.ws).to_string(),
            })),
        })
    }

    pub(crate) fn advertised_json(&self, host: &str) -> Value {
        let endpoint = |address: SocketAddr| host_port(host, address.port());
        json!({
            "http": endpoint(self.http),
            "grpc_h2c": endpoint(self.grpc_h2c),
            "grpc_tls": self.grpc_tls.map(endpoint),
            "tcp": endpoint(self.tcp),
            "udp": endpoint(self.udp),
            "thrift": endpoint(self.thrift),
            "mqtt": self.mqtt.as_ref().map(|mqtt| json!({
                "tcp": host_port(host, mqtt.tcp),
                "auth_tcp": host_port(host, mqtt.auth_tcp),
                "v5": host_port(host, mqtt.v5),
                "tls": mqtt.tls.map(|port| host_port(host, port)),
                "ws": host_port(host, mqtt.ws),
            })),
        })
    }
}

pub(crate) fn host_port(host: &str, port: u16) -> String {
    if host
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_ipv6())
    {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
