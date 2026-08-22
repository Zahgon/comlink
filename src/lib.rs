/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! # Comlink
//!
//! Comlink makes worker threads enjoyable. It is an RPC implementation over
//! message passing: values exposed on one thread can be used from another as if
//! they were local.
//!
//! ```
//! use std::sync::Arc;
//! use comlink::{expose, wrap, Func, HostValue, Origin, MessageChannel, Endpoint};
//!
//! let chan = MessageChannel::new();
//! chan.port1.start();
//! chan.port2.start();
//!
//! // The far side exposes a function...
//! let adder = Func::new(|args: Vec<HostValue>| {
//!     let a = args[0].as_f64().unwrap_or(0.0);
//!     let b = args[1].as_f64().unwrap_or(0.0);
//!     Ok(HostValue::from(a + b))
//! });
//! expose(adder, Arc::new(chan.port2.clone()), vec![Origin::Any]);
//!
//! // ...and this side calls it.
//! let remote = wrap(Arc::new(chan.port1.clone()));
//! let sum = remote.call(vec![1.into(), 3.into()]).unwrap();
//! assert_eq!(sum.as_f64(), Some(4.0));
//! ```
//!
//! ## Migrated from TypeScript
//!
//! This crate is a translation of `GoogleChromeLabs/comlink` at commit
//! `114a4a6448a855a613f1cb9a7c89290606c003cf`. The protocol, the transfer
//! handlers, the origin filter and the release/finalizer lifecycle are the
//! same. Two things could not come across unchanged:
//!
//! * **`Proxy`.** The original turns `await remote.a.b()` into a path plus a
//!   trap. Rust has no such interception, so the path is built explicitly with
//!   [`Remote::get`] and sent by a terminal operation ([`Remote::value`],
//!   [`Remote::set`], [`Remote::call`], [`Remote::construct`]).
//! * **`Remote<T>` / `Local<T>`.** Those are conditional mapped types with no
//!   runtime footprint and no Rust equivalent. [`Remote`] is one concrete type
//!   instead, and the compile-time guarantees they gave are not reproduced.
//!
//! One thing came out better: the original releases an endpoint when a
//! `FinalizationRegistry` notices the last proxy was collected, which is
//! best-effort and unobservable. Here [`Remote`] is reference counted and
//! `Drop` releases deterministically.

mod comlink;
pub mod endpoint;
pub mod protocol;
pub mod thread_adapter;
pub mod value;

pub use crate::comlink::{
    expose, proxy, remove_transfer_handler, set_transfer_handler, transfer, wrap, Class,
    Constructor, Func, Getter, Host, HostValue, Method, Obj, Origin, Remote, Setter, Thrown,
    TransferHandler,
};
pub use crate::endpoint::{start_broadcast, BroadcastChannel, MessageChannel, MessagePort, Worker};
pub use crate::protocol::{
    Endpoint, Envelope, EventSource, Listener, ListenerId, Message, MessageEvent, MessageId,
    MessageType, Transferable, WireValue,
};
pub use crate::thread_adapter::{thread_endpoint, ThreadAdapter, ThreadEndpoint};
pub use crate::value::{ArrayBuffer, Value};
