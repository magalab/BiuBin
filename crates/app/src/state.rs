use crate::addresses::BoundAddresses;
use crate::graphql::BiubinSchema;
use biubin_core::{Config, Event, EventStore, Readiness};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, broadcast};

pub(crate) const MAX_REQUEST_ID_LEN: usize = 128;
pub(crate) const MAX_HEADER_VALUE_LEN: usize = 4096;
pub(crate) const MAX_WS_MESSAGE_SIZE: usize = 64 * 1024;
pub(crate) const MAX_WS_ROOM_NAME_LEN: usize = 128;
pub(crate) const MAX_WS_ROOMS: usize = 100;
pub(crate) const MAX_SOCKET_FRAME_SIZE: usize = 1024 * 1024;
pub(crate) const MAX_SOCKET_READ_SIZE: usize = 4 * 1024 * 1024;
pub(crate) const SOCKET_IDLE_TIMEOUT_SECS: u64 = 60;
pub(crate) const MAX_UDP_PACKET_SIZE: usize = 65_535;
pub(crate) const STREAM_BYTES_CHUNK_SIZE: usize = 16 * 1024;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) bound: BoundAddresses,
    pub(crate) events: EventStore,
    pub(crate) graphql: BiubinSchema,
    pub(crate) readiness: Readiness,
    pub(crate) request_seq: Arc<AtomicU64>,
    pub(crate) connection_slots: Arc<Semaphore>,
    pub(crate) rooms: Arc<Mutex<HashMap<String, broadcast::Sender<WsPayload>>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct WsPayload {
    pub(crate) binary: bool,
    pub(crate) data: Vec<u8>,
}

#[derive(Serialize)]
pub(crate) struct EventsResponse {
    pub(crate) events: Vec<Event>,
    pub(crate) dropped: u64,
}
