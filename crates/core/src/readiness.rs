use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Readiness {
    required: Arc<BTreeSet<String>>,
    states: Arc<Mutex<BTreeMap<String, bool>>>,
}

impl Readiness {
    pub fn with_required<I>(required: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let required: BTreeSet<String> = required.into_iter().collect();
        let states = required.iter().map(|name| (name.clone(), false)).collect();
        Self {
            required: Arc::new(required),
            states: Arc::new(Mutex::new(states)),
        }
    }

    pub fn mark(&self, name: impl Into<String>, ready: bool) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        states.insert(name.into(), ready);
    }

    pub fn is_ready(&self) -> bool {
        let states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.required
            .iter()
            .all(|name| states.get(name).copied().unwrap_or(false))
    }

    pub fn snapshot(&self) -> BTreeMap<String, bool> {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_all_required_listeners() {
        let readiness = Readiness::with_required(["http".to_owned(), "grpc".to_owned()]);
        assert!(!readiness.is_ready());
        readiness.mark("http", true);
        assert!(!readiness.is_ready());
        readiness.mark("grpc", true);
        assert!(readiness.is_ready());
    }
}
