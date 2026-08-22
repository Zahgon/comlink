/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! The wire protocol. A direct translation of `src/protocol.ts`.
//!
//! The original declares `WireValueType::PROXY` and `WireValueType::THROW` but
//! never constructs or matches them -- they are vestigial from v3. They are not
//! carried over; `WireValue` has exactly the two variants the protocol uses.

use std::sync::Arc;

use crate::value::{ArrayBuffer, Value};

pub type MessageId = String;

/// `WireValueType` in the original.
#[derive(Clone, Debug, PartialEq)]
pub enum WireValue {
    /// `WireValueType.RAW`
    Raw { value: Value },
    /// `WireValueType.HANDLER` -- a transfer handler claimed this value and is
    /// named here so the other side can pick the same one.
    Handler { name: String, value: Value },
}

/// `MessageType` in the original.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {
    Get,
    Set,
    Apply,
    Construct,
    Endpoint,
    Release,
}

/// The request half of the protocol -- `Message` in the original.
#[derive(Clone, Debug)]
pub enum Message {
    Get {
        id: MessageId,
        path: Vec<String>,
    },
    Set {
        id: MessageId,
        path: Vec<String>,
        value: WireValue,
    },
    Apply {
        id: MessageId,
        path: Vec<String>,
        argument_list: Vec<WireValue>,
    },
    Construct {
        id: MessageId,
        path: Vec<String>,
        argument_list: Vec<WireValue>,
    },
    Endpoint {
        id: MessageId,
    },
    Release {
        id: MessageId,
    },
}

impl Message {
    pub fn id(&self) -> &str {
        match self {
            Message::Get { id, .. }
            | Message::Set { id, .. }
            | Message::Apply { id, .. }
            | Message::Construct { id, .. }
            | Message::Endpoint { id }
            | Message::Release { id } => id,
        }
    }

    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Get { .. } => MessageType::Get,
            Message::Set { .. } => MessageType::Set,
            Message::Apply { .. } => MessageType::Apply,
            Message::Construct { .. } => MessageType::Construct,
            Message::Endpoint { .. } => MessageType::Endpoint,
            Message::Release { .. } => MessageType::Release,
        }
    }

    pub fn path(&self) -> &[String] {
        match self {
            Message::Get { path, .. }
            | Message::Set { path, .. }
            | Message::Apply { path, .. }
            | Message::Construct { path, .. } => path,
            Message::Endpoint { .. } | Message::Release { .. } => &[],
        }
    }
}

/// What actually travels over an endpoint.
///
/// In JavaScript both halves of the conversation are untyped objects on the same
/// `message` event: `expose` looks at `type`, `wrap` looks at `id`, and each
/// ignores what the other sent. Rust can name the two halves instead, and each
/// side still ignores the variant that is not addressed to it.
#[derive(Clone, Debug)]
pub enum Envelope {
    Request(Message),
    Response { id: MessageId, value: WireValue },
    /// A plain `postMessage(data)` that is not part of an RPC exchange. Comlink
    /// ignores these; application code that shares an endpoint can read them.
    Data(Value),
}

/// An incoming `message` event.
#[derive(Clone, Debug)]
pub struct MessageEvent {
    pub data: Envelope,
    /// Empty for ports and worker threads, just as `ev.origin` is empty for
    /// `MessagePort` messages in the browser.
    pub origin: String,
}

/// Values named in a transfer list. Transferring moves ownership instead of
/// copying.
#[derive(Clone, Debug)]
pub enum Transferable {
    Buffer(ArrayBuffer),
    Port(MessagePortHandle),
}

/// A port in a transfer list, kept behind a handle so `Transferable` does not
/// have to name the concrete endpoint type.
pub type MessagePortHandle = crate::endpoint::MessagePort;

pub type Listener = Arc<dyn Fn(&MessageEvent) + Send + Sync>;
pub type ListenerId = usize;

/// `EventSource` in the original.
pub trait EventSource: Send + Sync {
    fn add_event_listener(&self, listener: Listener) -> ListenerId;
    fn remove_event_listener(&self, id: ListenerId);
}

/// `Endpoint` in the original: anything with a `postMessage`-shaped interface.
pub trait Endpoint: EventSource {
    fn post_message(&self, message: Envelope, transfer: Vec<Transferable>);

    /// Optional in the original (`start?: () => void`); a default no-op here.
    fn start(&self) {}

    /// The original detects real `MessagePort`s by `constructor.name` and closes
    /// only those. An explicit predicate replaces that duck-typing.
    fn is_message_port(&self) -> bool {
        false
    }

    fn close(&self) {}
}

// --------------------------------------------------------------------------- //
// Walking the values inside an envelope                                        //
// --------------------------------------------------------------------------- //

fn map_value(value: &mut Value, f: &mut dyn FnMut(&mut Value)) {
    f(value);
    match value {
        Value::Array(items) => {
            for item in items {
                map_value(item, f);
            }
        }
        Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                map_value(v, f);
            }
        }
        _ => {}
    }
}

fn map_wire_value(wire: &mut WireValue, f: &mut dyn FnMut(&mut Value)) {
    match wire {
        WireValue::Raw { value } | WireValue::Handler { value, .. } => map_value(value, f),
    }
}

/// Apply `f` to every `Value` carried by an envelope.
///
/// Transferring is a move, so a buffer named in the transfer list has to be
/// swapped for one holding the bytes before the envelope is delivered -- the
/// sender is left with an empty buffer and the receiver gets the contents.
pub fn map_envelope_values(envelope: &mut Envelope, f: &mut dyn FnMut(&mut Value)) {
    match envelope {
        Envelope::Data(value) => map_value(value, f),
        Envelope::Response { value, .. } => map_wire_value(value, f),
        Envelope::Request(msg) => match msg {
            Message::Set { value, .. } => map_wire_value(value, f),
            Message::Apply { argument_list, .. } | Message::Construct { argument_list, .. } => {
                for wire in argument_list {
                    map_wire_value(wire, f);
                }
            }
            Message::Get { .. } | Message::Endpoint { .. } | Message::Release { .. } => {}
        },
    }
}
