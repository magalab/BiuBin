use biubin_core::EventStore;
use std::io::{self, Read};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use thrift::protocol::{
    TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory, TFieldIdentifier, TInputProtocol,
    TInputProtocolFactory, TMessageIdentifier, TMessageType, TOutputProtocol,
    TOutputProtocolFactory, TStructIdentifier, TType,
};
use thrift::server::{TProcessor, handle_process_result};
use thrift::transport::{
    TFramedReadTransportFactory, TFramedWriteTransportFactory, TIoChannel, TReadTransportFactory,
    TTcpChannel, TWriteTransportFactory,
};
use tokio::sync::Semaphore;

const MAX_THRIFT_LIST_ITEMS: i32 = 1000;
const MAX_THRIFT_STRING_BYTES: usize = 1024 * 1024;
const MAX_THRIFT_FRAME_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ThriftListener {
    listener: TcpListener,
    address: SocketAddr,
}

impl ThriftListener {
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
}

pub(crate) fn bind(address: SocketAddr) -> Result<ThriftListener, String> {
    let listener = TcpListener::bind(address)
        .map_err(|error| format!("bind Thrift listener {address}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("configure Thrift listener {address}: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("read Thrift listener address: {error}"))?;
    Ok(ThriftListener { listener, address })
}

pub(crate) struct ThriftHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl ThriftHandle {
    pub(crate) fn stop_token(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    pub(crate) fn join(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ThriftHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

pub(crate) fn spawn(
    listener: ThriftListener,
    events: EventStore,
    connection_slots: Arc<Semaphore>,
) -> Result<ThriftHandle, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let join = thread::Builder::new()
        .name("biubin-thrift".to_owned())
        .spawn(move || {
            run_server(listener.listener, events, connection_slots, stop_for_thread);
        })
        .map_err(|error| format!("spawn Thrift server: {error}"))?;
    Ok(ThriftHandle {
        stop,
        join: Some(join),
    })
}

fn run_server(
    listener: TcpListener,
    events: EventStore,
    connection_slots: Arc<Semaphore>,
    stop: Arc<AtomicBool>,
) {
    let connections = Arc::new(Mutex::new(
        std::collections::HashMap::<u64, TcpStream>::new(),
    ));
    let next_connection = AtomicUsize::new(1);
    let mut workers: Vec<JoinHandle<()>> = Vec::new();
    while !stop.load(Ordering::Acquire) {
        let mut live_workers = Vec::with_capacity(workers.len());
        for worker in workers.drain(..) {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                live_workers.push(worker);
            }
        }
        workers = live_workers;
        match listener.accept() {
            Ok((stream, address)) => {
                if let Err(error) = stream.set_nonblocking(false) {
                    tracing::warn!(%error, %address, "configure Thrift connection failed");
                    continue;
                }
                let permit = match connection_slots.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        events.push(
                            "thrift",
                            "connection_rejected",
                            format!("peer={address} reason=max_connections"),
                        );
                        drop(stream);
                        continue;
                    }
                };
                let connections_for_worker = connections.clone();
                let events_for_worker = events.clone();
                let connection_id = next_connection.fetch_add(1, Ordering::Relaxed) as u64;
                let Ok(control_stream) = stream.try_clone() else {
                    drop(stream);
                    continue;
                };
                connections
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(connection_id, control_stream);
                match thread::Builder::new()
                    .name("biubin-thrift-connection".to_owned())
                    .spawn(move || {
                        let _permit = permit;
                        process_connection(stream, address, events_for_worker);
                        connections_for_worker
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&connection_id);
                    }) {
                    Ok(worker) => workers.push(worker),
                    Err(error) => {
                        connections
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&connection_id);
                        tracing::warn!(%error, %address, "spawn Thrift connection worker failed");
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                tracing::error!(%error, "embedded Thrift listener stopped");
                break;
            }
        }
    }
    for stream in connections
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
    {
        let _ = stream.shutdown(Shutdown::Both);
    }
    for worker in workers {
        let _ = worker.join();
    }
}

fn process_connection(stream: TcpStream, address: SocketAddr, events: EventStore) {
    let channel = TTcpChannel::with_stream(stream);
    let Ok((read_channel, write_channel)) = channel.split() else {
        return;
    };
    let read_transport_factory = TFramedReadTransportFactory::new();
    let input_protocol_factory = TBinaryInputProtocolFactory::new();
    let write_transport_factory = TFramedWriteTransportFactory::new();
    let output_protocol_factory = TBinaryOutputProtocolFactory::new();
    let read_transport = read_transport_factory.create(Box::new(LimitedRead {
        inner: read_channel,
        remaining: None,
        header: [0; 4],
        header_read: 0,
        header_sent: 0,
    }));
    let input_protocol = input_protocol_factory.create(read_transport);
    let write_transport = write_transport_factory.create(Box::new(write_channel));
    let output_protocol = output_protocol_factory.create(write_transport);
    let processor = BiubinProcessor { events };
    let mut input = input_protocol;
    let mut output = output_protocol;
    loop {
        match processor.process(&mut *input, &mut *output) {
            Ok(()) => {}
            Err(error) => {
                tracing::debug!(%address, ?error, "Thrift connection closed");
                break;
            }
        }
    }
}

struct LimitedRead<R> {
    inner: R,
    remaining: Option<usize>,
    header: [u8; 4],
    header_read: usize,
    header_sent: usize,
}

impl<R: Read> Read for LimitedRead<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.remaining.is_none() {
            while self.header_read < self.header.len() {
                let read = self.inner.read(&mut self.header[self.header_read..])?;
                if read == 0 {
                    return Ok(0);
                }
                self.header_read += read;
            }
            let frame_size = u32::from_be_bytes(self.header) as usize;
            if frame_size > MAX_THRIFT_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Thrift frame exceeds the configured input limit",
                ));
            }
            self.remaining = Some(frame_size);
        }
        if self.header_sent < self.header.len() {
            let send_limit = output.len().min(self.header.len() - self.header_sent);
            output[..send_limit]
                .copy_from_slice(&self.header[self.header_sent..self.header_sent + send_limit]);
            self.header_sent += send_limit;
            return Ok(send_limit);
        }
        let remaining = self.remaining.expect("frame size was set");
        if remaining == 0 {
            self.remaining = None;
            self.header_read = 0;
            self.header_sent = 0;
            return self.read(output);
        }
        let read_limit = output.len().min(remaining);
        let read = self.inner.read(&mut output[..read_limit])?;
        if read > 0 {
            self.remaining = Some(remaining - read);
        }
        Ok(read)
    }
}

struct BiubinProcessor {
    events: EventStore,
}

impl TProcessor for BiubinProcessor {
    fn process(
        &self,
        input: &mut dyn TInputProtocol,
        output: &mut dyn TOutputProtocol,
    ) -> thrift::Result<()> {
        let message = input.read_message_begin()?;
        let result = if !matches!(
            message.message_type,
            TMessageType::Call | TMessageType::OneWay
        ) {
            Err(format!("unexpected Thrift message type for {}", message.name).into())
        } else {
            match message.name.as_str() {
                "echo" => self.process_echo(&message, input, output),
                "sum" => self.process_sum(&message, input, output),
                "raiseError" => self.process_raise_error(&message, input, output),
                "notify" => self.process_notify(&message, input, output),
                _ => Err(format!("unknown Thrift method: {}", message.name).into()),
            }
        };
        input.read_message_end()?;
        if message.message_type == TMessageType::OneWay {
            result
        } else {
            handle_process_result(&message, result, output)
        }
    }
}

impl BiubinProcessor {
    fn process_echo(
        &self,
        message: &TMessageIdentifier,
        input: &mut dyn TInputProtocol,
        output: &mut dyn TOutputProtocol,
    ) -> thrift::Result<()> {
        let (request_message, payload) = read_echo_args(input)?;
        let request_message = request_message.unwrap_or_default();
        if request_message.len() > MAX_THRIFT_STRING_BYTES
            || payload.len() > MAX_THRIFT_STRING_BYTES
        {
            return Err("echo request exceeds the Thrift input limit".into());
        }
        self.events.push("thrift", "request_received", "echo");
        write_message_begin(output, message, TMessageType::Reply)?;
        output.write_struct_begin(&TStructIdentifier::new("echo_result"))?;
        output.write_field_begin(&TFieldIdentifier::new("success", TType::Struct, 0))?;
        write_echo_response(output, &request_message, &payload)?;
        output.write_field_end()?;
        output.write_field_stop()?;
        output.write_struct_end()?;
        output.write_message_end()?;
        output.flush()
    }

    fn process_sum(
        &self,
        message: &TMessageIdentifier,
        input: &mut dyn TInputProtocol,
        output: &mut dyn TOutputProtocol,
    ) -> thrift::Result<()> {
        let values = read_sum_args(input)?;
        self.events.push(
            "thrift",
            "request_received",
            format!("sum count={}", values.len()),
        );
        write_message_begin(output, message, TMessageType::Reply)?;
        output.write_struct_begin(&TStructIdentifier::new("sum_result"))?;
        output.write_field_begin(&TFieldIdentifier::new("success", TType::I64, 0))?;
        output.write_i64(values.into_iter().sum())?;
        output.write_field_end()?;
        output.write_field_stop()?;
        output.write_struct_end()?;
        output.write_message_end()?;
        output.flush()
    }

    fn process_raise_error(
        &self,
        _message: &TMessageIdentifier,
        input: &mut dyn TInputProtocol,
        _output: &mut dyn TOutputProtocol,
    ) -> thrift::Result<()> {
        let kind = read_error_kind(input)?;
        self.events.push(
            "thrift",
            "request_received",
            format!("raiseError kind={kind}"),
        );
        Err(format!("biubin requested Thrift error kind={kind}").into())
    }

    fn process_notify(
        &self,
        _message: &TMessageIdentifier,
        input: &mut dyn TInputProtocol,
        _output: &mut dyn TOutputProtocol,
    ) -> thrift::Result<()> {
        let message = read_notify_args(input)?;
        if message.len() > MAX_THRIFT_STRING_BYTES {
            return Err("notify message exceeds the Thrift input limit".into());
        }
        self.events.push("thrift", "request_received", "notify");
        Ok(())
    }
}

fn read_echo_args(input: &mut dyn TInputProtocol) -> thrift::Result<(Option<String>, Vec<u8>)> {
    input.read_struct_begin()?;
    let mut request = (None, Vec::new());
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(1) && field.field_type == TType::Struct {
            request = read_echo_request(input)?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    Ok(request)
}

fn read_echo_request(input: &mut dyn TInputProtocol) -> thrift::Result<(Option<String>, Vec<u8>)> {
    input.read_struct_begin()?;
    let mut message = None;
    let mut payload = Vec::new();
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        match (field.id, field.field_type) {
            (Some(1), TType::String) => message = Some(input.read_string()?),
            (Some(2), TType::String) => payload = input.read_bytes()?,
            (_, field_type) => input.skip(field_type)?,
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    Ok((message, payload))
}

fn read_sum_args(input: &mut dyn TInputProtocol) -> thrift::Result<Vec<i64>> {
    input.read_struct_begin()?;
    let mut values = Vec::new();
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(1) && field.field_type == TType::List {
            let list = input.read_list_begin()?;
            if list.size < 0 || list.size > MAX_THRIFT_LIST_ITEMS {
                return Err("sum list exceeds the Thrift input limit".into());
            }
            if list.element_type != TType::I64 {
                return Err("sum values must be i64".into());
            }
            for _ in 0..list.size {
                values.push(input.read_i64()?);
            }
            input.read_list_end()?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    Ok(values)
}

fn read_error_kind(input: &mut dyn TInputProtocol) -> thrift::Result<i32> {
    input.read_struct_begin()?;
    let mut kind = 0;
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(1) && field.field_type == TType::I32 {
            kind = input.read_i32()?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    Ok(kind)
}

fn read_notify_args(input: &mut dyn TInputProtocol) -> thrift::Result<String> {
    input.read_struct_begin()?;
    let mut message = String::new();
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(1) && field.field_type == TType::String {
            message = input.read_string()?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    Ok(message)
}

fn write_message_begin(
    output: &mut dyn TOutputProtocol,
    message: &TMessageIdentifier,
    message_type: TMessageType,
) -> thrift::Result<()> {
    output.write_message_begin(&TMessageIdentifier::new(
        message.name.clone(),
        message_type,
        message.sequence_number,
    ))
}

fn write_echo_response(
    output: &mut dyn TOutputProtocol,
    message: &str,
    payload: &[u8],
) -> thrift::Result<()> {
    output.write_struct_begin(&TStructIdentifier::new("EchoResponse"))?;
    output.write_field_begin(&TFieldIdentifier::new("message", TType::String, 1))?;
    output.write_string(message)?;
    output.write_field_end()?;
    output.write_field_begin(&TFieldIdentifier::new("payload", TType::String, 2))?;
    output.write_bytes(payload)?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()
}
