/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! The `node-adapter.ts` port.
//!
//! Node's `worker_threads` ports speak `on`/`off` instead of
//! `addEventListener`/`removeEventListener`, so the original ships a small
//! adapter. The same mismatch exists here for any channel that names its
//! subscription methods differently, and the adapter is the same shape: keep a
//! map from the listener the caller handed us to the one we registered, so
//! removal can find it again.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::endpoint::MessagePort;
use crate::protocol::{
    Endpoint, Envelope, EventSource, Listener, ListenerId, Transferable,
};

/// A port that speaks `on`/`off` -- the `NodeEndpoint` interface.
pub trait ThreadEndpoint: Send + Sync {
    fn post_message(&self, message: Envelope, transfer: Vec<Transferable>);
    fn on(&self, event: &str, listener: Listener) -> ListenerId;
    fn off(&self, event: &str, id: ListenerId);
    fn start(&self) {}
    fn close(&self) {}
    fn is_message_port(&self) -> bool {
        false
    }
}

/// Any `MessagePort` is already a thread endpoint; this is the equivalent of
/// handing `parentPort` to `nodeEndpoint()`.
impl ThreadEndpoint for MessagePort {
    fn post_message(&self, message: Envelope, transfer: Vec<Transferable>) {
        Endpoint::post_message(self, message, transfer)
    }

    fn on(&self, _event: &str, listener: Listener) -> ListenerId {
        self.add_event_listener(listener)
    }

    fn off(&self, _event: &str, id: ListenerId) {
        self.remove_event_listener(id)
    }

    fn start(&self) {
        Endpoint::start(self)
    }

    fn close(&self) {
        Endpoint::close(self)
    }

    fn is_message_port(&self) -> bool {
        true
    }
}

/// `nodeEndpoint(nep)` -- adapt an `on`/`off` port to a Comlink `Endpoint`.
pub struct ThreadAdapter {
    inner: Arc<dyn ThreadEndpoint>,
    /// Our listener id -> the inner port's listener id, the `WeakMap` of the
    /// original.
    listeners: Mutex<BTreeMap<ListenerId, ListenerId>>,
    next_id: AtomicUsize,
}

impl EventSource for ThreadAdapter {
    fn add_event_listener(&self, listener: Listener) -> ListenerId {
        let outer = self.next_id.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.on("message", listener);
        self.listeners.lock().unwrap().insert(outer, inner);
        outer
    }

    fn remove_event_listener(&self, id: ListenerId) {
        let inner = self.listeners.lock().unwrap().remove(&id);
        // No registration under this id -- nothing to do, as in the original.
        if let Some(inner) = inner {
            self.inner.off("message", inner);
        }
    }
}

impl Endpoint for ThreadAdapter {
    fn post_message(&self, message: Envelope, transfer: Vec<Transferable>) {
        self.inner.post_message(message, transfer)
    }

    fn start(&self) {
        self.inner.start()
    }

    fn is_message_port(&self) -> bool {
        self.inner.is_message_port()
    }

    fn close(&self) {
        self.inner.close()
    }
}

/// `export default function nodeEndpoint(nep)`.
pub fn thread_endpoint(nep: Arc<dyn ThreadEndpoint>) -> Arc<dyn Endpoint> {
    Arc::new(ThreadAdapter {
        inner: nep,
        listeners: Mutex::new(BTreeMap::new()),
        next_id: AtomicUsize::new(1),
    })
}
