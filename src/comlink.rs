/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! Comlink: an RPC implementation over message passing.
//!
//! ## What changed on the way from TypeScript
//!
//! The original is built on two things Rust does not have: `Proxy`, which turns
//! `await remote.a.b.c()` into a path plus a trap, and a type system able to
//! express `Remote<T>` as a mapped conditional type. The protocol, the transfer
//! handlers, the release/finalizer lifecycle and the error semantics all carry
//! over unchanged. The proxy sugar does not, so the path a `Proxy` would have
//! accumulated is built explicitly:
//!
//! ```text
//!   await remote.counter          ->  remote.get("counter").value()?
//!   await remote.inc(1)           ->  remote.call_method("inc", vec![1.into()])?
//!   remote.x = 4                  ->  remote.set("x", 4.into())?
//!   await new remote()            ->  remote.construct(vec![])?
//! ```
//!
//! `await` becomes a blocking receive: the calling thread parks until the other
//! endpoint answers, which is the same ordering guarantee a promise gives.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::endpoint::{MessageChannel, MessagePort};
use crate::protocol::{
    Endpoint, Envelope, Listener, ListenerId, Message, MessageEvent, MessageId, MessageType,
    Transferable, WireValue,
};
use crate::value::Value;

/// How long a call waits for its answer before giving up. The original relies on
/// a promise that may never settle; a blocking call needs a ceiling so a broken
/// endpoint fails loudly instead of parking a thread forever.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Releasing waits only briefly for its acknowledgement. A release is a local
/// round trip, and `Drop` must not park a thread when the far end has already
/// gone away -- which is exactly when the acknowledgement never arrives.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

// --------------------------------------------------------------------------- //
// Thrown values                                                                //
// --------------------------------------------------------------------------- //

/// Something that came back as a rejection.
///
/// JavaScript can throw any value at all, and the original's tests check that
/// scalars, `null` and plain objects survive the trip as themselves rather than
/// being coerced into `Error`. Both shapes are kept.
#[derive(Clone, Debug, PartialEq)]
pub enum Thrown {
    Error {
        name: String,
        message: String,
        stack: String,
    },
    Value(Value),
}

impl Thrown {
    pub fn error(message: impl Into<String>) -> Thrown {
        let message = message.into();
        Thrown::Error {
            stack: format!("Error: {}\n    at <comlink>", message),
            name: "Error".to_string(),
            message,
        }
    }

    pub fn type_error(message: impl Into<String>) -> Thrown {
        let message = message.into();
        Thrown::Error {
            stack: format!("TypeError: {}\n    at <comlink>", message),
            name: "TypeError".to_string(),
            message,
        }
    }

    /// An error carrying a stack the caller chose, so a test can assert the
    /// stack survived the round trip.
    pub fn error_with_stack(message: impl Into<String>, stack: impl Into<String>) -> Thrown {
        Thrown::Error {
            name: "Error".to_string(),
            message: message.into(),
            stack: stack.into(),
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            Thrown::Error { message, .. } => Some(message),
            Thrown::Value(_) => None,
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Thrown::Error { name, .. } => Some(name),
            Thrown::Value(_) => None,
        }
    }

    pub fn stack(&self) -> Option<&str> {
        match self {
            Thrown::Error { stack, .. } => Some(stack),
            Thrown::Value(_) => None,
        }
    }

    pub fn value(&self) -> Option<&Value> {
        match self {
            Thrown::Value(v) => Some(v),
            Thrown::Error { .. } => None,
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Thrown::Error { .. })
    }
}

impl fmt::Display for Thrown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Thrown::Error { name, message, .. } => write!(f, "{}: {}", name, message),
            Thrown::Value(v) => write!(f, "{:?}", v),
        }
    }
}

impl std::error::Error for Thrown {}

// --------------------------------------------------------------------------- //
// Values crossing the API boundary                                             //
// --------------------------------------------------------------------------- //

/// What callers hand to, and receive from, Comlink.
///
/// `HostValue` is where the original's `Comlink.proxy()` marker and its
/// `transferCache` WeakMap end up. JavaScript can stamp a symbol onto any object
/// and look it up later by identity; Rust says it in the type instead.
#[derive(Clone)]
pub enum HostValue {
    /// A structured-cloneable value.
    Value(Value),
    /// `Comlink.proxy(obj)` -- send a proxy, do not clone.
    Proxied(Arc<dyn Host>),
    /// A proxy that arrived from the other side.
    Remote(Remote),
    /// The internal throw marker.
    Thrown(Thrown),
    /// A value claimed by a user-registered transfer handler.
    Tagged { name: String, value: Value },
    /// `Comlink.transfer(value, transfer)`.
    Transferred {
        value: Box<HostValue>,
        transfer: Vec<Transferable>,
    },
}

impl fmt::Debug for HostValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostValue::Value(v) => write!(f, "Value({:?})", v),
            HostValue::Proxied(_) => write!(f, "Proxied(..)"),
            HostValue::Remote(r) => write!(f, "Remote({:?})", r.path()),
            HostValue::Thrown(t) => write!(f, "Thrown({})", t),
            HostValue::Tagged { name, value } => write!(f, "Tagged({}, {:?})", name, value),
            HostValue::Transferred { value, .. } => write!(f, "Transferred({:?})", value),
        }
    }
}

impl HostValue {
    pub fn undefined() -> HostValue {
        HostValue::Value(Value::Undefined)
    }

    /// The plain value, if this is one.
    pub fn as_value(&self) -> Option<&Value> {
        match self {
            HostValue::Value(v) => Some(v),
            HostValue::Transferred { value, .. } => value.as_value(),
            _ => None,
        }
    }

    pub fn into_value(self) -> Result<Value, Thrown> {
        match self {
            HostValue::Value(v) => Ok(v),
            HostValue::Transferred { value, .. } => value.into_value(),
            other => Err(Thrown::type_error(format!(
                "{:?} is not a structured-cloneable value",
                other
            ))),
        }
    }

    pub fn as_remote(&self) -> Option<&Remote> {
        match self {
            HostValue::Remote(r) => Some(r),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        self.as_value().and_then(|v| v.as_f64())
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_value().and_then(|v| v.as_str())
    }

    pub fn as_bool(&self) -> Option<bool> {
        self.as_value().and_then(|v| v.as_bool())
    }
}

impl<T: Into<Value>> From<T> for HostValue {
    fn from(v: T) -> HostValue {
        HostValue::Value(v.into())
    }
}

/// `Comlink.proxy(value)` -- neither copy nor transfer; send a proxy.
pub fn proxy(obj: Arc<dyn Host>) -> HostValue {
    HostValue::Proxied(obj)
}

/// `Comlink.transfer(value, transfers)`.
///
/// The original keys a `WeakMap` on object identity; a Rust value has no such
/// identity, so the transfer list rides along with the value instead.
pub fn transfer(value: impl Into<HostValue>, transfer: Vec<Transferable>) -> HostValue {
    HostValue::Transferred {
        value: Box::new(value.into()),
        transfer,
    }
}

// --------------------------------------------------------------------------- //
// The exposed object model                                                     //
// --------------------------------------------------------------------------- //

/// Something that can be exposed on an endpoint.
///
/// In JavaScript `expose` walks the path with `path.reduce((o, p) => o[p], obj)`
/// against whatever object it was given. Rust has no universal property access,
/// so the four operations the protocol needs are a trait. `Obj` below implements
/// it for the common case.
pub trait Host: Send + Sync {
    fn get(&self, _path: &[String]) -> Result<HostValue, Thrown> {
        Ok(HostValue::undefined())
    }

    fn set(&self, _path: &[String], _value: HostValue) -> Result<(), Thrown> {
        Err(Thrown::type_error("cannot set on this object"))
    }

    fn apply(&self, path: &[String], _args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        Err(Thrown::type_error(format!(
            "{} is not a function",
            path.join(".")
        )))
    }

    fn construct(&self, path: &[String], _args: Vec<HostValue>) -> Result<Arc<dyn Host>, Thrown> {
        Err(Thrown::type_error(format!(
            "{} is not a constructor",
            path.join(".")
        )))
    }

    /// `Comlink.finalizer` -- invoked once, when the proxy is released.
    fn finalizer(&self) {}
}

pub type Method = Arc<dyn Fn(Vec<HostValue>) -> Result<HostValue, Thrown> + Send + Sync>;
pub type Getter = Arc<dyn Fn() -> Result<HostValue, Thrown> + Send + Sync>;
pub type Setter = Arc<dyn Fn(HostValue) -> Result<(), Thrown> + Send + Sync>;
pub type Constructor =
    Arc<dyn Fn(Vec<HostValue>) -> Result<Arc<dyn Host>, Thrown> + Send + Sync>;

enum Entry {
    Value(HostValue),
    Accessor {
        get: Option<Getter>,
        set: Option<Setter>,
    },
    Method(Method),
    Child(Arc<dyn Host>),
    /// A property whose value is marked with `Comlink.proxy()`.
    Proxied(Arc<dyn Host>),
    Class(Arc<Class>),
}

/// A plain exposed object -- the analogue of the object literals the original's
/// tests expose.
pub struct Obj {
    entries: Mutex<BTreeMap<String, Entry>>,
    finalizer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Default for Obj {
    fn default() -> Self {
        Obj {
            entries: Mutex::new(BTreeMap::new()),
            finalizer: Mutex::new(None),
        }
    }
}

impl Obj {
    pub fn new() -> Arc<Obj> {
        Arc::new(Obj::default())
    }

    /// A plain data property.
    pub fn put(&self, name: &str, value: impl Into<HostValue>) -> &Self {
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), Entry::Value(value.into()));
        self
    }

    pub fn put_getter<F>(&self, name: &str, get: F) -> &Self
    where
        F: Fn() -> Result<HostValue, Thrown> + Send + Sync + 'static,
    {
        self.entries.lock().unwrap().insert(
            name.to_string(),
            Entry::Accessor {
                get: Some(Arc::new(get)),
                set: None,
            },
        );
        self
    }

    pub fn put_accessor<G, S>(&self, name: &str, get: G, set: S) -> &Self
    where
        G: Fn() -> Result<HostValue, Thrown> + Send + Sync + 'static,
        S: Fn(HostValue) -> Result<(), Thrown> + Send + Sync + 'static,
    {
        self.entries.lock().unwrap().insert(
            name.to_string(),
            Entry::Accessor {
                get: Some(Arc::new(get)),
                set: Some(Arc::new(set)),
            },
        );
        self
    }

    pub fn put_method<F>(&self, name: &str, f: F) -> &Self
    where
        F: Fn(Vec<HostValue>) -> Result<HostValue, Thrown> + Send + Sync + 'static,
    {
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), Entry::Method(Arc::new(f)));
        self
    }

    pub fn put_child(&self, name: &str, child: Arc<dyn Host>) -> &Self {
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), Entry::Child(child));
        self
    }

    /// A nested value marked with `Comlink.proxy()`.
    pub fn put_proxied(&self, name: &str, child: Arc<dyn Host>) -> &Self {
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), Entry::Proxied(child));
        self
    }

    pub fn put_class(&self, name: &str, class: Arc<Class>) -> &Self {
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), Entry::Class(class));
        self
    }

    pub fn put_finalizer<F>(&self, f: F) -> &Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.finalizer.lock().unwrap() = Some(Arc::new(f));
        self
    }

    /// Read a property back locally, without going over the wire. Exposed
    /// objects whose methods act on their own properties need this.
    pub fn raw(&self, name: &str) -> Option<HostValue> {
        match self.entries.lock().unwrap().get(name) {
            Some(Entry::Value(v)) => Some(v.clone()),
            Some(Entry::Proxied(h)) => Some(HostValue::Proxied(Arc::clone(h))),
            _ => None,
        }
    }

    /// The object's own data properties, as one cloneable value. This is what a
    /// structured clone of the object would contain.
    fn snapshot(&self) -> Value {
        let mut out = BTreeMap::new();
        for (k, e) in self.entries.lock().unwrap().iter() {
            if let Entry::Value(HostValue::Value(v)) = e {
                out.insert(k.clone(), v.clone());
            }
        }
        Value::Object(out)
    }
}

impl Host for Obj {
    fn get(&self, path: &[String]) -> Result<HostValue, Thrown> {
        if path.is_empty() {
            return Ok(HostValue::Value(self.snapshot()));
        }
        let entries = self.entries.lock().unwrap();
        let entry = match entries.get(&path[0]) {
            Some(e) => e,
            // Reading a property that is not there yields `undefined`, exactly
            // as the reduce over a plain object would.
            None => return Ok(HostValue::undefined()),
        };
        if path.len() == 1 {
            return match entry {
                Entry::Value(v) => Ok(v.clone()),
                Entry::Accessor { get: Some(g), .. } => {
                    let g = Arc::clone(g);
                    drop(entries);
                    g()
                }
                Entry::Accessor { get: None, .. } => Ok(HostValue::undefined()),
                // Returning a function from `postMessage` is a DataCloneError;
                // the original turns that into this exact TypeError.
                Entry::Method(_) | Entry::Class(_) => {
                    Err(Thrown::type_error("Unserializable return value"))
                }
                Entry::Child(h) => {
                    let h = Arc::clone(h);
                    drop(entries);
                    h.get(&[])
                }
                Entry::Proxied(h) => Ok(HostValue::Proxied(Arc::clone(h))),
            };
        }
        let rest = path[1..].to_vec();
        match entry {
            Entry::Child(h) | Entry::Proxied(h) => {
                let h = Arc::clone(h);
                drop(entries);
                h.get(&rest)
            }
            Entry::Class(c) => {
                let c = Arc::clone(c);
                drop(entries);
                c.get(&rest)
            }
            // A plain value keeps being walked, the way the original's reduce
            // does: `{ a: { v: 4 } }` answers `a.v` with 4, not undefined.
            Entry::Value(hv) => {
                let walked = hv
                    .as_value()
                    .and_then(|v| v.path(&rest))
                    .cloned()
                    .unwrap_or(Value::Undefined);
                Ok(HostValue::Value(walked))
            }
            Entry::Accessor { get: Some(g), .. } => {
                let g = Arc::clone(g);
                drop(entries);
                let got = g()?;
                let walked = got
                    .as_value()
                    .and_then(|v| v.path(&rest))
                    .cloned()
                    .unwrap_or(Value::Undefined);
                Ok(HostValue::Value(walked))
            }
            _ => Ok(HostValue::undefined()),
        }
    }

    fn set(&self, path: &[String], value: HostValue) -> Result<(), Thrown> {
        if path.is_empty() {
            return Err(Thrown::type_error("cannot assign to the root object"));
        }
        if path.len() == 1 {
            let mut entries = self.entries.lock().unwrap();
            if let Some(Entry::Accessor { set: Some(s), .. }) = entries.get(&path[0]) {
                let s = Arc::clone(s);
                drop(entries);
                return s(value);
            }
            entries.insert(path[0].clone(), Entry::Value(value));
            return Ok(());
        }
        let mut entries = self.entries.lock().unwrap();
        let rest = path[1..].to_vec();
        match entries.get(&path[0]) {
            Some(Entry::Child(h)) | Some(Entry::Proxied(h)) => {
                let h = Arc::clone(h);
                drop(entries);
                h.set(&rest, value)
            }
            Some(Entry::Class(c)) => {
                let c = Arc::clone(c);
                drop(entries);
                c.set(&rest, value)
            }
            // Assigning into a cloned value, as `parent[last] = v` would.
            Some(Entry::Value(HostValue::Value(current))) => {
                let mut updated = current.clone();
                let new_value = value.into_value()?;
                if !updated.set_path(&rest, new_value) {
                    return Err(Thrown::type_error(format!(
                        "cannot set {}",
                        path.join(".")
                    )));
                }
                entries.insert(path[0].clone(), Entry::Value(HostValue::Value(updated)));
                Ok(())
            }
            _ => Err(Thrown::type_error(format!(
                "cannot set {}",
                path.join(".")
            ))),
        }
    }

    fn apply(&self, path: &[String], args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        if path.is_empty() {
            return Err(Thrown::type_error("the exposed object is not a function"));
        }
        let entries = self.entries.lock().unwrap();
        let entry = entries
            .get(&path[0])
            .ok_or_else(|| Thrown::type_error(format!("{} is not a function", path.join("."))))?;
        if path.len() == 1 {
            return match entry {
                Entry::Method(m) => {
                    let m = Arc::clone(m);
                    drop(entries);
                    m(args)
                }
                _ => Err(Thrown::type_error(format!(
                    "{} is not a function",
                    path.join(".")
                ))),
            };
        }
        let rest = path[1..].to_vec();
        match entry {
            Entry::Child(h) | Entry::Proxied(h) => {
                let h = Arc::clone(h);
                drop(entries);
                h.apply(&rest, args)
            }
            Entry::Class(c) => {
                let c = Arc::clone(c);
                drop(entries);
                c.apply(&rest, args)
            }
            _ => Err(Thrown::type_error(format!(
                "{} is not a function",
                path.join(".")
            ))),
        }
    }

    fn construct(&self, path: &[String], args: Vec<HostValue>) -> Result<Arc<dyn Host>, Thrown> {
        if path.is_empty() {
            return Err(Thrown::type_error("the exposed object is not a constructor"));
        }
        let entries = self.entries.lock().unwrap();
        let entry = entries.get(&path[0]).ok_or_else(|| {
            Thrown::type_error(format!("{} is not a constructor", path.join(".")))
        })?;
        let rest = path[1..].to_vec();
        match entry {
            Entry::Class(c) => {
                let c = Arc::clone(c);
                drop(entries);
                c.construct(&rest, args)
            }
            Entry::Child(h) | Entry::Proxied(h) => {
                let h = Arc::clone(h);
                drop(entries);
                h.construct(&rest, args)
            }
            _ => Err(Thrown::type_error(format!(
                "{} is not a constructor",
                path.join(".")
            ))),
        }
    }

    fn finalizer(&self) {
        let f = self.finalizer.lock().unwrap().clone();
        if let Some(f) = f {
            f();
        }
    }
}

/// A single exposed function -- `Comlink.expose((a, b) => a + b, ep)`.
pub struct Func(Method);

impl Func {
    pub fn new<F>(f: F) -> Arc<Func>
    where
        F: Fn(Vec<HostValue>) -> Result<HostValue, Thrown> + Send + Sync + 'static,
    {
        Arc::new(Func(Arc::new(f)))
    }
}

impl Host for Func {
    fn apply(&self, path: &[String], args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        if path.is_empty() {
            (self.0)(args)
        } else {
            Err(Thrown::type_error(format!(
                "{} is not a function",
                path.join(".")
            )))
        }
    }

    fn get(&self, path: &[String]) -> Result<HostValue, Thrown> {
        if path.is_empty() {
            Err(Thrown::type_error("Unserializable return value"))
        } else {
            Ok(HostValue::undefined())
        }
    }
}

/// An exposed class -- static members plus a constructor.
pub struct Class {
    statics: Arc<Obj>,
    ctor: Constructor,
}

impl Class {
    pub fn new<F>(ctor: F) -> Arc<Class>
    where
        F: Fn(Vec<HostValue>) -> Result<Arc<dyn Host>, Thrown> + Send + Sync + 'static,
    {
        Arc::new(Class {
            statics: Obj::new(),
            ctor: Arc::new(ctor),
        })
    }

    /// The object holding the class's static members.
    pub fn statics(&self) -> Arc<Obj> {
        Arc::clone(&self.statics)
    }
}

impl Host for Class {
    fn get(&self, path: &[String]) -> Result<HostValue, Thrown> {
        self.statics.get(path)
    }

    fn set(&self, path: &[String], value: HostValue) -> Result<(), Thrown> {
        self.statics.set(path, value)
    }

    fn apply(&self, path: &[String], args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        self.statics.apply(path, args)
    }

    fn construct(&self, path: &[String], args: Vec<HostValue>) -> Result<Arc<dyn Host>, Thrown> {
        if path.is_empty() {
            (self.ctor)(args)
        } else {
            self.statics.construct(path, args)
        }
    }
}

// --------------------------------------------------------------------------- //
// Transfer handlers                                                            //
// --------------------------------------------------------------------------- //

/// Customises the serialisation of certain values, as determined by
/// `can_handle`. A handler has to be registered on *both* sides of the channel,
/// under the same name.
pub trait TransferHandler: Send + Sync {
    fn can_handle(&self, value: &HostValue) -> bool;
    fn serialize(&self, value: HostValue) -> (Value, Vec<Transferable>);
    /// Deserialising may reject -- that is how the throw handler rethrows.
    fn deserialize(&self, value: Value) -> Result<HostValue, Thrown>;
}

/// The internal handler for values marked with `Comlink.proxy()`.
struct ProxyTransferHandler;

impl TransferHandler for ProxyTransferHandler {
    fn can_handle(&self, value: &HostValue) -> bool {
        matches!(value, HostValue::Proxied(_))
    }

    fn serialize(&self, value: HostValue) -> (Value, Vec<Transferable>) {
        let obj = match value {
            HostValue::Proxied(o) => o,
            _ => unreachable!("can_handle guarantees Proxied"),
        };
        let chan = MessageChannel::new();
        let port1: Arc<dyn Endpoint> = Arc::new(chan.port1.clone());
        expose(obj, port1, vec![Origin::Any]);
        (
            Value::Port(chan.port2.clone()),
            vec![Transferable::Port(chan.port2)],
        )
    }

    fn deserialize(&self, value: Value) -> Result<HostValue, Thrown> {
        match value {
            Value::Port(port) => {
                port.start();
                Ok(HostValue::Remote(wrap(Arc::new(port))))
            }
            other => Err(Thrown::type_error(format!(
                "proxy handler expected a port, got {:?}",
                other
            ))),
        }
    }
}

/// The internal handler for thrown exceptions.
struct ThrowTransferHandler;

impl TransferHandler for ThrowTransferHandler {
    fn can_handle(&self, value: &HostValue) -> bool {
        matches!(value, HostValue::Thrown(_))
    }

    fn serialize(&self, value: HostValue) -> (Value, Vec<Transferable>) {
        let thrown = match value {
            HostValue::Thrown(t) => t,
            _ => unreachable!("can_handle guarantees Thrown"),
        };
        let serialized = match thrown {
            Thrown::Error {
                name,
                message,
                stack,
            } => Value::object(vec![
                ("isError", Value::Bool(true)),
                (
                    "value",
                    Value::object(vec![
                        ("message", Value::String(message)),
                        ("name", Value::String(name)),
                        ("stack", Value::String(stack)),
                    ]),
                ),
            ]),
            Thrown::Value(v) => Value::object(vec![("isError", Value::Bool(false)), ("value", v)]),
        };
        (serialized, Vec::new())
    }

    fn deserialize(&self, value: Value) -> Result<HostValue, Thrown> {
        let is_error = value
            .get("isError")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let inner = value.get("value").cloned().unwrap_or(Value::Undefined);
        if is_error {
            Err(Thrown::Error {
                message: inner
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                name: inner
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Error")
                    .to_string(),
                stack: inner
                    .get("stack")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
            })
        } else {
            Err(Thrown::Value(inner))
        }
    }
}

struct HandlerRegistry {
    handlers: Mutex<Vec<(String, Arc<dyn TransferHandler>)>>,
}

impl HandlerRegistry {
    fn get(&self, name: &str) -> Option<Arc<dyn TransferHandler>> {
        self.handlers
            .lock()
            .unwrap()
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, h)| Arc::clone(h))
    }

    fn snapshot(&self) -> Vec<(String, Arc<dyn TransferHandler>)> {
        self.handlers.lock().unwrap().clone()
    }

    /// `transferHandlers.set(name, handler)` -- replaces in place if the name is
    /// already taken, so registration order stays stable.
    fn set(&self, name: &str, handler: Arc<dyn TransferHandler>) {
        let mut hs = self.handlers.lock().unwrap();
        if let Some(slot) = hs.iter_mut().find(|(n, _)| n == name) {
            slot.1 = handler;
        } else {
            hs.push((name.to_string(), handler));
        }
    }

    fn remove(&self, name: &str) {
        self.handlers.lock().unwrap().retain(|(n, _)| n != name);
    }
}

fn registry() -> &'static HandlerRegistry {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<HandlerRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| HandlerRegistry {
        handlers: Mutex::new(vec![
            (
                "proxy".to_string(),
                Arc::new(ProxyTransferHandler) as Arc<dyn TransferHandler>,
            ),
            (
                "throw".to_string(),
                Arc::new(ThrowTransferHandler) as Arc<dyn TransferHandler>,
            ),
        ]),
    })
}

/// `Comlink.transferHandlers` -- register a handler under a name.
pub fn set_transfer_handler(name: &str, handler: Arc<dyn TransferHandler>) {
    registry().set(name, handler);
}

/// Remove a previously registered handler.
pub fn remove_transfer_handler(name: &str) {
    registry().remove(name);
}

fn to_wire_value(value: HostValue) -> (WireValue, Vec<Transferable>) {
    if let HostValue::Transferred { value, transfer } = value {
        let (wire, mut collected) = to_wire_value(*value);
        collected.extend(transfer);
        return (wire, collected);
    }
    for (name, handler) in registry().snapshot() {
        if handler.can_handle(&value) {
            let (serialized, transfer) = handler.serialize(value);
            return (
                WireValue::Handler {
                    name,
                    value: serialized,
                },
                transfer,
            );
        }
    }
    match value {
        HostValue::Value(v) => (WireValue::Raw { value: v }, Vec::new()),
        // A Remote cannot itself be put back on the wire; the original has the
        // same limitation, since a proxy is not structured-cloneable.
        other => (
            WireValue::Raw {
                value: Value::String(format!("{:?}", other)),
            },
            Vec::new(),
        ),
    }
}

fn from_wire_value(value: WireValue) -> Result<HostValue, Thrown> {
    match value {
        WireValue::Handler { name, value } => match registry().get(&name) {
            Some(h) => h.deserialize(value),
            None => Err(Thrown::type_error(format!(
                "no transfer handler registered under '{}'",
                name
            ))),
        },
        WireValue::Raw { value } => Ok(HostValue::Value(value)),
    }
}

// --------------------------------------------------------------------------- //
// Origin filtering                                                             //
// --------------------------------------------------------------------------- //

/// An entry in `expose`'s allowed-origin list.
///
/// The original accepts strings and `RegExp`s. Rust's standard library has no
/// regular expressions and this crate has no dependencies, so an arbitrary
/// predicate stands in for the `RegExp` case.
#[derive(Clone)]
pub enum Origin {
    Any,
    Exact(String),
    Predicate(Arc<dyn Fn(&str) -> bool + Send + Sync>),
}

impl Origin {
    pub fn predicate<F>(f: F) -> Origin
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Origin::Predicate(Arc::new(f))
    }
}

fn is_allowed_origin(allowed: &[Origin], origin: &str) -> bool {
    for entry in allowed {
        match entry {
            Origin::Any => return true,
            Origin::Exact(s) => {
                if s == origin || s == "*" {
                    return true;
                }
            }
            Origin::Predicate(f) => {
                if f(origin) {
                    return true;
                }
            }
        }
    }
    false
}

// --------------------------------------------------------------------------- //
// expose                                                                       //
// --------------------------------------------------------------------------- //

/// `Comlink.expose(value, endpoint, allowedOrigins)`.
pub fn expose(obj: Arc<dyn Host>, ep: Arc<dyn Endpoint>, allowed_origins: Vec<Origin>) {
    let listener_id: Arc<Mutex<Option<ListenerId>>> = Arc::new(Mutex::new(None));

    let obj_for_cb = Arc::clone(&obj);
    let ep_for_cb = Arc::clone(&ep);
    let lid_for_cb = Arc::clone(&listener_id);
    let allowed = allowed_origins.clone();

    let listener: Listener = Arc::new(move |ev: &MessageEvent| {
        if !is_allowed_origin(&allowed, &ev.origin) {
            eprintln!("Invalid origin '{}' for comlink proxy", ev.origin);
            return;
        }
        let msg = match &ev.data {
            Envelope::Request(m) => m.clone(),
            // A response addressed to a `wrap` on this same endpoint, or a
            // plain data message that is none of our business.
            Envelope::Response { .. } | Envelope::Data(_) => return,
        };

        let id: MessageId = msg.id().to_string();
        let ty = msg.message_type();
        let path: Vec<String> = msg.path().to_vec();

        let outcome: Result<HostValue, Thrown> = (|| {
            let args = match &msg {
                Message::Apply { argument_list, .. }
                | Message::Construct { argument_list, .. } => argument_list
                    .iter()
                    .cloned()
                    .map(from_wire_value)
                    .collect::<Result<Vec<_>, _>>()?,
                _ => Vec::new(),
            };
            match &msg {
                Message::Get { .. } => obj_for_cb.get(&path),
                Message::Set { value, .. } => {
                    let v = from_wire_value(value.clone())?;
                    obj_for_cb.set(&path, v)?;
                    Ok(HostValue::Value(Value::Bool(true)))
                }
                Message::Apply { .. } => obj_for_cb.apply(&path, args),
                // The return of a construct signature is always proxied,
                // whether it was marked or not.
                Message::Construct { .. } => {
                    obj_for_cb.construct(&path, args).map(HostValue::Proxied)
                }
                Message::Endpoint { .. } => {
                    let chan = MessageChannel::new();
                    let far: Arc<dyn Endpoint> = Arc::new(chan.port2.clone());
                    expose(Arc::clone(&obj_for_cb), far, vec![Origin::Any]);
                    Ok(HostValue::Transferred {
                        value: Box::new(HostValue::Value(Value::Port(chan.port1.clone()))),
                        transfer: vec![Transferable::Port(chan.port1)],
                    })
                }
                Message::Release { .. } => Ok(HostValue::undefined()),
            }
        })();

        let return_value = match outcome {
            Ok(v) => v,
            Err(t) => HostValue::Thrown(t),
        };
        let (wire, transfer) = to_wire_value(return_value);
        ep_for_cb.post_message(Envelope::Response { id, value: wire }, transfer);

        if ty == MessageType::Release {
            // Detach and deactivate, after the release response above.
            if let Some(lid) = *lid_for_cb.lock().unwrap() {
                ep_for_cb.remove_event_listener(lid);
            }
            if ep_for_cb.is_message_port() {
                ep_for_cb.close();
            }
            obj_for_cb.finalizer();
        }
    });

    let id = ep.add_event_listener(listener);
    *listener_id.lock().unwrap() = Some(id);
    ep.start();
}

// --------------------------------------------------------------------------- //
// wrap / Remote                                                                //
// --------------------------------------------------------------------------- //

struct EndpointState {
    ep: Arc<dyn Endpoint>,
    pending: Mutex<BTreeMap<MessageId, std::sync::mpsc::Sender<WireValue>>>,
    released: AtomicBool,
    /// The original counts live proxies per endpoint in a `WeakMap` and releases
    /// when a `FinalizationRegistry` observes the last one collected. Rust drops
    /// deterministically, so the same count lives here and `Drop` does the work
    /// the GC callback used to.
    proxy_count: AtomicUsize,
}

impl EndpointState {
    fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let id = generate_uuid();
        let (tx, rx) = channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        self.ep.start();
        self.ep
            .post_message(Envelope::Request(Message::Release { id }), Vec::new());
        // Wait for the release response so the far side has run its finalizer
        // before the endpoint goes away.
        let _ = rx.recv_timeout(RELEASE_TIMEOUT);
        if self.ep.is_message_port() {
            self.ep.close();
        }
        self.pending.lock().unwrap().clear();
    }
}

/// A proxy for a value living behind an endpoint -- what `Comlink.wrap()`
/// returns.
///
/// Property access does not travel: `get` only extends the path. The path is
/// sent when a terminal operation asks for it -- `value`, `set`, `call`,
/// `construct` -- which is exactly when the original's `Proxy` traps fire.
pub struct Remote {
    state: Arc<EndpointState>,
    path: Vec<String>,
}

impl fmt::Debug for Remote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Remote({})", self.path.join("."))
    }
}

impl Clone for Remote {
    fn clone(&self) -> Remote {
        self.state.proxy_count.fetch_add(1, Ordering::SeqCst);
        Remote {
            state: Arc::clone(&self.state),
            path: self.path.clone(),
        }
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        // When the last proxy for an endpoint goes away, release the endpoint --
        // the deterministic equivalent of the original's FinalizationRegistry.
        if self.state.proxy_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.state.release();
        }
    }
}

/// `Comlink.wrap(endpoint)`.
pub fn wrap(ep: Arc<dyn Endpoint>) -> Remote {
    let state = Arc::new(EndpointState {
        ep: Arc::clone(&ep),
        pending: Mutex::new(BTreeMap::new()),
        released: AtomicBool::new(false),
        proxy_count: AtomicUsize::new(1),
    });

    let weak: Weak<EndpointState> = Arc::downgrade(&state);
    let listener: Listener = Arc::new(move |ev: &MessageEvent| {
        let state = match weak.upgrade() {
            Some(s) => s,
            None => return,
        };
        let (id, value) = match &ev.data {
            Envelope::Response { id, value } => (id.clone(), value.clone()),
            Envelope::Request(_) | Envelope::Data(_) => return,
        };
        let resolver = state.pending.lock().unwrap().remove(&id);
        if let Some(tx) = resolver {
            let _ = tx.send(value);
        }
    });
    ep.add_event_listener(listener);

    Remote {
        state,
        path: Vec::new(),
    }
}

impl Remote {
    pub fn path(&self) -> &[String] {
        &self.path
    }

    fn throw_if_released(&self) -> Result<(), Thrown> {
        if self.state.released.load(Ordering::SeqCst) {
            return Err(Thrown::error("Proxy has been released and is not useable"));
        }
        Ok(())
    }

    fn request(
        &self,
        make: impl FnOnce(MessageId) -> Message,
        transfer: Vec<Transferable>,
    ) -> Result<HostValue, Thrown> {
        self.throw_if_released()?;
        let id = generate_uuid();
        let (tx, rx) = channel();
        self.state.pending.lock().unwrap().insert(id.clone(), tx);
        self.state.ep.start();
        self.state
            .ep
            .post_message(Envelope::Request(make(id.clone())), transfer);
        match rx.recv_timeout(CALL_TIMEOUT) {
            Ok(wire) => from_wire_value(wire),
            Err(_) => {
                self.state.pending.lock().unwrap().remove(&id);
                Err(Thrown::error("comlink: no response from the endpoint"))
            }
        }
    }

    /// Extend the path -- the `get` trap. Nothing is sent.
    pub fn get(&self, prop: &str) -> Remote {
        self.state.proxy_count.fetch_add(1, Ordering::SeqCst);
        let mut path = self.path.clone();
        path.push(prop.to_string());
        Remote {
            state: Arc::clone(&self.state),
            path,
        }
    }

    /// Resolve the path -- what `await`ing the proxy does.
    pub fn value(&self) -> Result<HostValue, Thrown> {
        let path = self.path.clone();
        self.request(|id| Message::Get { id, path }, Vec::new())
    }

    /// `await remote.some.path` as a number.
    pub fn number(&self) -> Result<f64, Thrown> {
        let v = self.value()?;
        v.as_f64()
            .ok_or_else(|| Thrown::type_error(format!("{:?} is not a number", v)))
    }

    pub fn string(&self) -> Result<String, Thrown> {
        let v = self.value()?;
        v.as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Thrown::type_error(format!("{:?} is not a string", v)))
    }

    pub fn boolean(&self) -> Result<bool, Thrown> {
        let v = self.value()?;
        v.as_bool()
            .ok_or_else(|| Thrown::type_error(format!("{:?} is not a boolean", v)))
    }

    /// Read a property in one step: `remote.get(p).value()`.
    pub fn get_value(&self, prop: &str) -> Result<HostValue, Thrown> {
        self.get(prop).value()
    }

    /// `remote[prop] = value` -- the `set` trap.
    pub fn set(&self, prop: &str, value: impl Into<HostValue>) -> Result<HostValue, Thrown> {
        let mut path = self.path.clone();
        path.push(prop.to_string());
        let (wire, transfer) = to_wire_value(value.into());
        self.request(
            move |id| Message::Set {
                id,
                path,
                value: wire,
            },
            transfer,
        )
    }

    /// Call the value at this path -- the `apply` trap.
    pub fn call(&self, args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        self.throw_if_released()?;
        // The original pretends `bind()` never happened; so does this.
        if self.path.last().map(|s| s.as_str()) == Some("bind") {
            let mut path = self.path.clone();
            path.pop();
            self.state.proxy_count.fetch_add(1, Ordering::SeqCst);
            return Ok(HostValue::Remote(Remote {
                state: Arc::clone(&self.state),
                path,
            }));
        }
        let (argument_list, transfer) = process_arguments(args);
        let path = self.path.clone();
        self.request(
            move |id| Message::Apply {
                id,
                path,
                argument_list,
            },
            transfer,
        )
    }

    /// `await remote.method(args)`.
    pub fn call_method(&self, prop: &str, args: Vec<HostValue>) -> Result<HostValue, Thrown> {
        self.get(prop).call(args)
    }

    /// `await new remote(args)` -- the `construct` trap. The result is always a
    /// proxy.
    pub fn construct(&self, args: Vec<HostValue>) -> Result<Remote, Thrown> {
        let (argument_list, transfer) = process_arguments(args);
        let path = self.path.clone();
        let result = self.request(
            move |id| Message::Construct {
                id,
                path,
                argument_list,
            },
            transfer,
        )?;
        match result {
            HostValue::Remote(r) => Ok(r),
            other => Err(Thrown::type_error(format!(
                "construct did not return a proxy, got {:?}",
                other
            ))),
        }
    }

    /// `proxy[Comlink.createEndpoint]()` -- a fresh port onto the same object.
    pub fn create_endpoint(&self) -> Result<MessagePort, Thrown> {
        let result = self.request(|id| Message::Endpoint { id }, Vec::new())?;
        match result.as_value() {
            Some(Value::Port(p)) => Ok(p.clone()),
            _ => Err(Thrown::type_error("createEndpoint did not return a port")),
        }
    }

    /// Drop `bind` from the path, the way the original's apply trap does.
    pub fn bind(&self) -> Remote {
        if self.path.last().map(|s| s.as_str()) == Some("bind") {
            let mut path = self.path.clone();
            path.pop();
            self.state.proxy_count.fetch_add(1, Ordering::SeqCst);
            return Remote {
                state: Arc::clone(&self.state),
                path,
            };
        }
        self.clone()
    }

    /// `proxy[Comlink.releaseProxy]()`.
    pub fn release(&self) {
        self.state.release();
    }

    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::SeqCst)
    }
}

fn process_arguments(args: Vec<HostValue>) -> (Vec<WireValue>, Vec<Transferable>) {
    let mut wires = Vec::with_capacity(args.len());
    let mut transfer = Vec::new();
    for a in args {
        let (w, t) = to_wire_value(a);
        wires.push(w);
        transfer.extend(t);
    }
    (wires, transfer)
}

// --------------------------------------------------------------------------- //
// Message ids                                                                  //
// --------------------------------------------------------------------------- //

/// Four hex chunks joined by dashes, like the original's `generateUUID`.
///
/// The original uses `Math.random()`; this uses a process-wide counter mixed
/// with the clock, which is at least as collision-resistant and does not need a
/// dependency.
fn generate_uuid() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let tid = format!("{:?}", std::thread::current().id());
    let mut hash: u128 = 0;
    for b in tid.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(b as u128);
    }
    format!(
        "{:x}-{:x}-{:x}-{:x}",
        nanos & 0xffff_ffff_ffff,
        seq,
        hash & 0xffff_ffff,
        (nanos >> 48) ^ seq
    )
}
