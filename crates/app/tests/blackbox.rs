use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http_request(address: SocketAddr, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(address).expect("connect HTTP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set HTTP read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write HTTP request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read HTTP response");
    response
}

fn wait_until_ready(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if let Ok(response) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            drop(response);
            let response = http_request(address, "/readyz");
            if response.starts_with(b"HTTP/1.1 200 ") {
                return;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("biubin did not become ready at {address}");
}

fn bound_http_address(child: &mut Child) -> SocketAddr {
    let stdout = child.stdout.take().expect("capture biubin startup logs");
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = line.expect("read biubin startup log");
        let Some(value) = line
            .split_once("bind_address=")
            .and_then(|(_, rest)| rest.split_whitespace().next())
        else {
            continue;
        };
        if let Ok(address) = value.parse() {
            return address;
        }
    }
    panic!("biubin did not report its bound HTTP address");
}

#[cfg(unix)]
fn terminate_gracefully(child: &mut Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM to biubin");
    assert!(status.success(), "kill command failed: {status}");
    let status = child.wait().expect("wait for graceful biubin shutdown");
    assert!(status.success(), "biubin did not exit cleanly: {status}");
}

fn body(response: &[u8]) -> &[u8] {
    response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| &response[position + 4..])
        .expect("HTTP response has a body separator")
}

#[test]
fn one_binary_exposes_real_ports_and_socket_protocols() {
    let binary = std::env::var("CARGO_BIN_EXE_biubin").expect("Cargo exposes biubin binary");
    let child = Command::new(binary)
        .env("BIUBIN_BIND_HOST", "127.0.0.1")
        .env("BIUBIN_ADVERTISE_HOST", "127.0.0.1")
        .env("BIUBIN_HTTP_PORT", "0")
        .env("BIUBIN_GRPC_H2C_PORT", "0")
        .env("BIUBIN_TCP_PORT", "0")
        .env("BIUBIN_UDP_PORT", "0")
        .env("BIUBIN_THRIFT_PORT", "0")
        .env("BIUBIN_MQTT_ENABLED", "false")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start biubin binary");
    let mut child = ChildGuard(child);
    let http_address = bound_http_address(&mut child.0);
    assert_ne!(http_address.port(), 0);
    wait_until_ready(http_address);

    let info_response = http_request(http_address, "/api/v1/info");
    assert!(info_response.starts_with(b"HTTP/1.1 200 "));
    let info: serde_json::Value = serde_json::from_slice(body(&info_response)).unwrap();
    assert_eq!(info["name"], "biubin");
    assert_eq!(info["bound_addresses"]["http"], http_address.to_string());
    let tcp_address: SocketAddr = info["bound_addresses"]["tcp"]
        .as_str()
        .expect("TCP bound address")
        .parse()
        .expect("valid TCP bound address");
    let udp_address: SocketAddr = info["bound_addresses"]["udp"]
        .as_str()
        .expect("UDP bound address")
        .parse()
        .expect("valid UDP bound address");
    let thrift_address: SocketAddr = info["bound_addresses"]["thrift"]
        .as_str()
        .expect("Thrift bound address")
        .parse()
        .expect("valid Thrift bound address");
    assert_ne!(tcp_address.port(), 0);
    assert_ne!(udp_address.port(), 0);
    assert_ne!(thrift_address.port(), 0);
    assert_eq!(info["ports"]["tcp"], tcp_address.port());
    assert_eq!(info["ports"]["udp"], udp_address.port());
    assert_eq!(info["ports"]["thrift"], thrift_address.port());

    let index_response = http_request(http_address, "/");
    assert!(index_response.starts_with(b"HTTP/1.1 200 "));
    assert!(String::from_utf8_lossy(body(&index_response)).contains("biubin"));

    let mut line = TcpStream::connect(tcp_address).expect("connect TCP echo");
    line.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set TCP read timeout");
    line.write_all(b"line smoke\n").expect("write line frame");
    let mut echoed_line = [0_u8; 11];
    line.read_exact(&mut echoed_line).expect("read line frame");
    assert_eq!(&echoed_line, b"line smoke\n");

    let mut framed = TcpStream::connect(tcp_address).expect("connect framed TCP");
    framed
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set framed TCP read timeout");
    let payload = b"length smoke";
    framed
        .write_all(&(payload.len() as u32).to_be_bytes())
        .expect("write length");
    framed.write_all(payload).expect("write length payload");
    let mut echoed = vec![0_u8; 4 + payload.len()];
    framed.read_exact(&mut echoed).expect("read length frame");
    assert_eq!(&echoed[..4], &(payload.len() as u32).to_be_bytes());
    assert_eq!(&echoed[4..], payload);

    let udp = UdpSocket::bind("127.0.0.1:0").expect("bind UDP client");
    udp.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set UDP read timeout");
    udp.send_to(b"udp smoke", udp_address)
        .expect("write UDP packet");
    let mut packet = [0_u8; 64];
    let (length, _) = udp.recv_from(&mut packet).expect("read UDP packet");
    assert_eq!(&packet[..length], b"udp smoke");

    #[cfg(unix)]
    terminate_gracefully(&mut child.0);
}
