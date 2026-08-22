/*!
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//! The structured-clone value model.
//!
//! JavaScript hands `postMessage` a dynamic value and lets the structured clone
//! algorithm decide what survives the trip. Rust has no such universal value, so
//! the set of things Comlink can put on the wire is spelled out here. The
//! variants mirror the structured-clone table the original ships in
//! `structured-clone-table.md`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::endpoint::MessagePort;

/// The `ArrayBuffer` analogue: bytes that can be *transferred* rather than
/// copied.
///
/// Transferring detaches the buffer -- the sender is left holding an empty one,
/// exactly like a transferred `ArrayBuffer` whose `byteLength` drops to 0. The
/// shared `Arc<Mutex<..>>` is what makes that observable from the sending side
/// after the value has left.
#[derive(Clone, Debug)]
pub struct ArrayBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl ArrayBuffer {
    pub fn new(bytes: Vec<u8>) -> Self {
        ArrayBuffer {
            inner: Arc::new(Mutex::new(bytes)),
        }
    }

    pub fn byte_length(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.inner.lock().unwrap().clone()
    }

    /// Hand out the bytes and leave the buffer empty. This is what `postMessage`
    /// does to a buffer named in the transfer list.
    pub fn detach(&self) -> Vec<u8> {
        std::mem::take(&mut *self.inner.lock().unwrap())
    }

    /// Identity, not contents: is this the very same buffer?
    pub fn same(&self, other: &ArrayBuffer) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn is_detached(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl PartialEq for ArrayBuffer {
    fn eq(&self, other: &Self) -> bool {
        // Identity first: comparing a buffer with itself must not lock twice.
        Arc::ptr_eq(&self.inner, &other.inner) || self.to_vec() == other.to_vec()
    }
}

/// A value that survives the trip between two endpoints.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Undefined,
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    /// A copied byte sequence -- the `TypedArray` analogue.
    Bytes(Vec<u8>),
    /// A transferable byte buffer -- the `ArrayBuffer` analogue.
    Buffer(ArrayBuffer),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    /// A transferred port. This is how `Comlink.proxy()` crosses the wire.
    Port(MessagePort),
}

impl Value {
    pub fn number(n: impl Into<f64>) -> Value {
        Value::Number(n.into())
    }

    pub fn string(s: impl Into<String>) -> Value {
        Value::String(s.into())
    }

    pub fn object(pairs: Vec<(&str, Value)>) -> Value {
        Value::Object(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_port(&self) -> Option<&MessagePort> {
        match self {
            Value::Port(p) => Some(p),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(map) => map.get(key),
            _ => None,
        }
    }

    pub fn is_undefined(&self) -> bool {
        matches!(self, Value::Undefined)
    }

    /// Walk a property path into this value.
    ///
    /// `expose` resolves a path with `path.reduce((o, p) => o[p], obj)`, which
    /// keeps descending through plain objects and arrays. Anything already
    /// cloned onto this side has to be walked the same way, or `remote.a.v`
    /// would come back undefined for `{ a: { v: 4 } }`.
    pub fn path(&self, path: &[String]) -> Option<&Value> {
        let mut current = self;
        for key in path {
            current = match current {
                Value::Object(map) => map.get(key)?,
                Value::Array(items) => items.get(key.parse::<usize>().ok()?)?,
                Value::Bytes(_) | Value::Buffer(_) if key == "length" || key == "byteLength" => {
                    return None
                }
                _ => return None,
            };
        }
        Some(current)
    }

    /// Assign through a property path, creating intermediate objects the way a
    /// plain assignment would not -- missing links make the write fail instead.
    pub fn set_path(&mut self, path: &[String], new_value: Value) -> bool {
        let (last, parents) = match path.split_last() {
            Some(split) => split,
            None => return false,
        };
        let mut current = self;
        for key in parents {
            current = match current {
                Value::Object(map) => match map.get_mut(key) {
                    Some(v) => v,
                    None => return false,
                },
                Value::Array(items) => match key.parse::<usize>().ok().and_then(|i| items.get_mut(i))
                {
                    Some(v) => v,
                    None => return false,
                },
                _ => return false,
            };
        }
        match current {
            Value::Object(map) => {
                map.insert(last.clone(), new_value);
                true
            }
            Value::Array(items) => match last.parse::<usize>().ok() {
                Some(i) if i < items.len() => {
                    items[i] = new_value;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// The result of JavaScript's `typeof`, which the original's rethrow tests
    /// assert on directly (`typeof err === "object"` for a thrown `null`).
    pub fn type_of(&self) -> &'static str {
        match self {
            Value::Undefined => "undefined",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            // `typeof null === "object"` -- a JavaScript quirk the wire format
            // has to reproduce, because the tests observe it.
            Value::Null
            | Value::Bytes(_)
            | Value::Buffer(_)
            | Value::Array(_)
            | Value::Object(_)
            | Value::Port(_) => "object",
        }
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Value {
        Value::Number(n)
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Value {
        Value::Number(n as f64)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Bool(b)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Value {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::String(s)
    }
}

/// String coercion that matches JavaScript's, so a Rust program and the
/// TypeScript original print the same thing for the same value.
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Undefined => write!(f, "undefined"),
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Number(n) => {
                if n.is_finite() && n.fract() == 0.0 && n.abs() < 1e21 {
                    write!(f, "{}", *n as i64)
                } else if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    write!(f, "{}", if *n > 0.0 { "Infinity" } else { "-Infinity" })
                } else {
                    write!(f, "{}", n)
                }
            }
            Value::String(s) => write!(f, "{}", s),
            // `String([1,2,3])` is "1,2,3", and a TypedArray coerces the same way.
            Value::Bytes(bytes) => {
                let parts: Vec<String> = bytes.iter().map(|b| b.to_string()).collect();
                write!(f, "{}", parts.join(","))
            }
            Value::Array(items) => {
                let parts: Vec<String> = items.iter().map(|i| i.to_string()).collect();
                write!(f, "{}", parts.join(","))
            }
            Value::Buffer(b) => write!(f, "[object ArrayBuffer({})]", b.byte_length()),
            Value::Object(_) => write!(f, "[object Object]"),
            Value::Port(_) => write!(f, "[object MessagePort]"),
        }
    }
}
