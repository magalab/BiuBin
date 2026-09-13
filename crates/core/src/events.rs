use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::broadcast;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub schema_version: u8,
    pub id: String,
    pub seq: u64,
    pub at: String,
    pub protocol: String,
    pub kind: String,
    pub connection_id: Option<String>,
    pub direction: Option<String>,
    pub summary: String,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct EventStore {
    inner: Arc<Mutex<EventStoreInner>>,
    updates: broadcast::Sender<Event>,
}

struct EventStoreInner {
    capacity: usize,
    next_seq: u64,
    dropped: u64,
    events: VecDeque<Event>,
}

impl EventStore {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (updates, _) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(EventStoreInner {
                capacity,
                next_seq: 1,
                dropped: 0,
                events: VecDeque::new(),
            })),
            updates,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.updates.subscribe()
    }

    pub fn push(&self, protocol: &str, kind: &str, summary: impl Into<String>) -> Event {
        self.push_with_attributes(protocol, kind, summary, BTreeMap::new())
    }

    pub fn push_with_attributes(
        &self,
        protocol: &str,
        kind: &str,
        summary: impl Into<String>,
        attributes: BTreeMap<String, Value>,
    ) -> Event {
        let event = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let seq = inner.next_seq;
            inner.next_seq = inner.next_seq.saturating_add(1);
            let event = Event {
                schema_version: 1,
                id: format!("evt_{seq}"),
                seq,
                at: now_rfc3339_like(),
                protocol: protocol.to_owned(),
                kind: kind.to_owned(),
                connection_id: None,
                direction: None,
                summary: summary.into(),
                attributes,
            };
            inner.events.push_back(event.clone());
            while inner.events.len() > inner.capacity {
                inner.events.pop_front();
                inner.dropped = inner.dropped.saturating_add(1);
            }
            event
        };
        let _ = self.updates.send(event.clone());
        event
    }

    pub fn list(&self, limit: usize, protocol: Option<&str>) -> Vec<Event> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .events
            .iter()
            .rev()
            .filter(|event| protocol.is_none_or(|wanted| event.protocol == wanted))
            .take(limit.min(inner.capacity))
            .cloned()
            .collect()
    }

    pub fn dropped(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dropped
    }
}

fn now_rfc3339_like() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_store_is_bounded_and_returns_newest_first() {
        let store = EventStore::new(2);
        store.push("http", "one", "one");
        store.push("http", "two", "two");
        store.push("http", "three", "three");
        let events = store.list(10, None);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].summary, "three");
        assert_eq!(events[1].summary, "two");
        assert_eq!(store.dropped(), 1);
    }

    #[tokio::test]
    async fn event_store_notifies_subscribers_without_blocking_publishers() {
        let store = EventStore::new(2);
        let mut receiver = store.subscribe();
        let event = store.push("http", "request", "request");
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.id, event.id);
        assert_eq!(received.summary, "request");
    }
}
