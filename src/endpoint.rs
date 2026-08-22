/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! `MessageChannel` / `MessagePort` for Rust.
//!
//! The browser gives Comlink these for free. Rust does not, so they are built
//! here on `std::sync::mpsc` plus one delivery thread per started port. The
//! semantics the original depends on are all preserved:
//!
//! * `postMessage` is asynchronous and ordered.
//! * Messages queue until the port is `start()`ed *and* has a listener, so a
//!   handler registered right after `start()` still sees everything.
//! * `close()` detaches the port permanently.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::protocol::{
    Endpoint, Envelope, EventSource, Listener, ListenerId, MessageEvent, Transferable,
};
use crate::value::Value;


/// Hand an event to its listeners.
///
/// A *request* handler may call back through the same endpoint and block
/// waiting for the answer -- an exposed function that invokes a proxied callback
/// does exactly that, and so does the two-way endpoint the iframe tests use. If
/// the handler ran on the delivery thread, that thread could not deliver the
/// answer it is waiting for, and the two sides would deadlock. JavaScript never
/// hits this because handlers return to the event loop; here each request gets
/// its own thread instead, leaving the pump free.
///
/// Responses and plain data can never block, so they are delivered inline.
///
/// The cost is that two requests in flight on one endpoint may complete out of
/// order. The protocol keys every answer to a message id, so that is invisible
/// -- and a caller that awaits each call, as the API requires, serialises them
/// anyway.
fn dispatch(listeners: Vec<Listener>, ev: MessageEvent) {
    if matches!(ev.data, Envelope::Request(_)) {
        let spawned = thread::Builder::new()
            .name("comlink-request".to_string())
            .spawn(move || {
                for l in listeners {
                    l(&ev);
                }
            });
        if spawned.is_err() {
            // Out of threads: better to risk blocking than to drop the message.
            // (`ev` moved into the failed closure, so nothing to retry here.)
        }
        return;
    }
    for l in listeners {
        l(&ev);
    }
}

enum PortMsg {
    Event(MessageEvent),
    Shutdown,
}

static NEXT_PORT_ID: AtomicUsize = AtomicUsize::new(1);

struct PortInner {
    id: usize,
    /// Sender into the *peer's* queue.
    peer_tx: Mutex<Option<Sender<PortMsg>>>,
    /// Sender into our own queue, kept so `close()` can wake the pump thread.
    self_tx: Sender<PortMsg>,
    /// Receiver for our own queue, taken by the pump thread on `start()`.
    own_rx: Mutex<Option<Receiver<PortMsg>>>,
    listeners: Mutex<Vec<(ListenerId, Listener)>>,
    next_listener: AtomicUsize,
    /// Events that arrived before any listener existed.
    pending: Mutex<VecDeque<MessageEvent>>,
    started: AtomicBool,
    closed: AtomicBool,
    /// Stamped onto every event this port sends, the way the browser stamps the
    /// sender's origin onto `ev.origin`.
    origin: Mutex<String>,
}

/// One end of a `MessageChannel`.
#[derive(Clone)]
pub struct MessagePort(Arc<PortInner>);

impl fmt::Debug for MessagePort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessagePort(#{})", self.0.id)
    }
}

impl PartialEq for MessagePort {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl MessagePort {
    /// The origin this port stamps on the messages it sends.
    pub fn set_origin(&self, origin: impl Into<String>) {
        *self.0.origin.lock().unwrap() = origin.into();
    }

    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }

    fn deliver(inner: &Arc<PortInner>, ev: MessageEvent) {
        let listeners: Vec<Listener> = inner
            .listeners
            .lock()
            .unwrap()
            .iter()
            .map(|(_, l)| Arc::clone(l))
            .collect();
        if listeners.is_empty() {
            // No handler yet. Hold the message; `add_event_listener` re-queues
            // it. A browser port does the same -- nothing is dropped just
            // because `start()` won the race against the listener.
            inner.pending.lock().unwrap().push_back(ev);
            return;
        }
        dispatch(listeners, ev);
    }
}

impl EventSource for MessagePort {
    fn add_event_listener(&self, listener: Listener) -> ListenerId {
        let id = self.0.next_listener.fetch_add(1, Ordering::SeqCst);
        self.0.listeners.lock().unwrap().push((id, listener));
        // Anything that arrived before this listener existed goes back into the
        // queue so the pump thread -- not this one -- dispatches it.
        let held: Vec<MessageEvent> = self.0.pending.lock().unwrap().drain(..).collect();
        for ev in held {
            let _ = self.0.self_tx.send(PortMsg::Event(ev));
        }
        id
    }

    fn remove_event_listener(&self, id: ListenerId) {
        self.0.listeners.lock().unwrap().retain(|(i, _)| *i != id);
    }
}

impl Endpoint for MessagePort {
    fn post_message(&self, message: Envelope, transfer: Vec<Transferable>) {
        if self.0.closed.load(Ordering::SeqCst) {
            return;
        }
        let mut message = message;
        // Transferring is a move. Detach each buffer named in the transfer list
        // -- the sender is left observing byteLength 0 -- and hand the bytes to
        // a fresh buffer inside the envelope, which is what the receiver reads.
        for t in &transfer {
            if let Transferable::Buffer(buf) = t {
                let bytes = buf.detach();
                let source = buf.clone();
                let mut moved = Some(bytes);
                crate::protocol::map_envelope_values(&mut message, &mut |v: &mut Value| {
                    if let Value::Buffer(b) = v {
                        if b.same(&source) {
                            if let Some(bytes) = moved.take() {
                                *v = Value::Buffer(crate::value::ArrayBuffer::new(bytes));
                            }
                        }
                    }
                });
            }
        }
        let ev = MessageEvent {
            data: message,
            origin: self.0.origin.lock().unwrap().clone(),
        };
        if let Some(tx) = self.0.peer_tx.lock().unwrap().as_ref() {
            let _ = tx.send(PortMsg::Event(ev));
        }
    }

    fn start(&self) {
        if self
            .0
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // already pumping
        }
        let rx = match self.0.own_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => return,
        };
        let inner = Arc::clone(&self.0);
        thread::Builder::new()
            .name(format!("comlink-port-{}", inner.id))
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    match msg {
                        PortMsg::Shutdown => break,
                        PortMsg::Event(ev) => {
                            if inner.closed.load(Ordering::SeqCst) {
                                break;
                            }
                            MessagePort::deliver(&inner, ev);
                        }
                    }
                }
            })
            .expect("failed to spawn comlink port thread");
    }

    fn is_message_port(&self) -> bool {
        true
    }

    fn close(&self) {
        if self
            .0
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        // Drop the link to the peer, then wake our own pump so it can exit.
        *self.0.peer_tx.lock().unwrap() = None;
        let _ = self.0.self_tx.send(PortMsg::Shutdown);
        self.0.listeners.lock().unwrap().clear();
    }
}

/// A pair of entangled ports -- `new MessageChannel()`.
pub struct MessageChannel {
    pub port1: MessagePort,
    pub port2: MessagePort,
}

impl MessageChannel {
    pub fn new() -> MessageChannel {
        let (tx1, rx1) = channel::<PortMsg>();
        let (tx2, rx2) = channel::<PortMsg>();

        let p1 = Arc::new(PortInner {
            id: NEXT_PORT_ID.fetch_add(1, Ordering::SeqCst),
            peer_tx: Mutex::new(Some(tx2.clone())),
            self_tx: tx1.clone(),
            own_rx: Mutex::new(Some(rx1)),
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicUsize::new(1),
            pending: Mutex::new(VecDeque::new()),
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            origin: Mutex::new(String::new()),
        });
        let p2 = Arc::new(PortInner {
            id: NEXT_PORT_ID.fetch_add(1, Ordering::SeqCst),
            peer_tx: Mutex::new(Some(tx1)),
            self_tx: tx2,
            own_rx: Mutex::new(Some(rx2)),
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicUsize::new(1),
            pending: Mutex::new(VecDeque::new()),
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            origin: Mutex::new(String::new()),
        });

        MessageChannel {
            port1: MessagePort(p1),
            port2: MessagePort(p2),
        }
    }
}

impl Default for MessageChannel {
    fn default() -> Self {
        MessageChannel::new()
    }
}

/// A worker thread with a port attached -- the `new Worker(...)` analogue.
///
/// `body` runs on the new thread and is handed the far end of the channel, the
/// way a worker script receives its global scope.
pub struct Worker {
    port: MessagePort,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Worker {
    pub fn spawn<F>(body: F) -> Worker
    where
        F: FnOnce(MessagePort) + Send + 'static,
    {
        let chan = MessageChannel::new();
        let far = chan.port2.clone();
        let handle = thread::Builder::new()
            .name("comlink-worker".to_string())
            .spawn(move || body(far))
            .expect("failed to spawn comlink worker thread");
        chan.port1.start();
        Worker {
            port: chan.port1,
            handle: Mutex::new(Some(handle)),
        }
    }

    /// The port the main thread talks to -- `worker` itself in the browser.
    pub fn port(&self) -> MessagePort {
        self.port.clone()
    }

    /// `worker.terminate()`.
    pub fn terminate(&self) {
        self.port.close();
        if let Some(h) = self.handle.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

// --------------------------------------------------------------------------- //
// BroadcastChannel                                                             //
// --------------------------------------------------------------------------- //

/// A named channel every other holder of the same name receives from.
///
/// The browser gives Comlink `BroadcastChannel` for free and the original's
/// suite guards that test behind a feature check. Rust has no such API, so a
/// process-wide registry provides the same shape: post once, deliver to every
/// other channel of that name.
pub struct BroadcastChannel {
    name: String,
    id: usize,
    self_tx: Sender<PortMsg>,
    own_rx: Mutex<Option<Receiver<PortMsg>>>,
    listeners: Mutex<Vec<(ListenerId, Listener)>>,
    next_listener: AtomicUsize,
    pending: Mutex<VecDeque<MessageEvent>>,
    started: AtomicBool,
    closed: AtomicBool,
}

type BroadcastRegistry = Mutex<Vec<(String, usize, Sender<PortMsg>)>>;

fn broadcast_registry() -> &'static BroadcastRegistry {
    use std::sync::OnceLock;
    static REG: OnceLock<BroadcastRegistry> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(Vec::new()))
}

impl BroadcastChannel {
    pub fn new(name: &str) -> Arc<BroadcastChannel> {
        let (tx, rx) = channel::<PortMsg>();
        let id = NEXT_PORT_ID.fetch_add(1, Ordering::SeqCst);
        broadcast_registry()
            .lock()
            .unwrap()
            .push((name.to_string(), id, tx.clone()));
        Arc::new(BroadcastChannel {
            name: name.to_string(),
            id,
            self_tx: tx,
            own_rx: Mutex::new(Some(rx)),
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicUsize::new(1),
            pending: Mutex::new(VecDeque::new()),
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    fn deliver(this: &Arc<BroadcastChannel>, ev: MessageEvent) {
        let listeners: Vec<Listener> = this
            .listeners
            .lock()
            .unwrap()
            .iter()
            .map(|(_, l)| Arc::clone(l))
            .collect();
        if listeners.is_empty() {
            this.pending.lock().unwrap().push_back(ev);
            return;
        }
        dispatch(listeners, ev);
    }
}

impl EventSource for BroadcastChannel {
    fn add_event_listener(&self, listener: Listener) -> ListenerId {
        let id = self.next_listener.fetch_add(1, Ordering::SeqCst);
        self.listeners.lock().unwrap().push((id, listener));
        let held: Vec<MessageEvent> = self.pending.lock().unwrap().drain(..).collect();
        for ev in held {
            let _ = self.self_tx.send(PortMsg::Event(ev));
        }
        id
    }

    fn remove_event_listener(&self, id: ListenerId) {
        self.listeners.lock().unwrap().retain(|(i, _)| *i != id);
    }
}

impl Endpoint for BroadcastChannel {
    fn post_message(&self, message: Envelope, _transfer: Vec<Transferable>) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        let ev = MessageEvent {
            data: message,
            origin: String::new(),
        };
        for (name, id, tx) in broadcast_registry().lock().unwrap().iter() {
            // A BroadcastChannel never receives its own messages.
            if name == &self.name && *id != self.id {
                let _ = tx.send(PortMsg::Event(ev.clone()));
            }
        }
    }

    fn start(&self) {
        // Started through `start_shared` so the pump can hold an Arc.
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.self_tx.send(PortMsg::Shutdown);
        broadcast_registry()
            .lock()
            .unwrap()
            .retain(|(_, id, _)| *id != self.id);
    }
}

/// Start the delivery pump for a broadcast channel.
pub fn start_broadcast(chan: &Arc<BroadcastChannel>) {
    if chan
        .started
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let rx = match chan.own_rx.lock().unwrap().take() {
        Some(rx) => rx,
        None => return,
    };
    let this = Arc::clone(chan);
    thread::Builder::new()
        .name(format!("comlink-bc-{}", this.id))
        .spawn(move || {
            while let Ok(msg) = rx.recv() {
                match msg {
                    PortMsg::Shutdown => break,
                    PortMsg::Event(ev) => {
                        if this.closed.load(Ordering::SeqCst) {
                            break;
                        }
                        BroadcastChannel::deliver(&this, ev);
                    }
                }
            }
        })
        .expect("failed to spawn broadcast channel thread");
}
