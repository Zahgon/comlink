/*!
 * Copyright 2017 Google Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! The structured-clone table, checked.
//!
//! The original documents what survives `postMessage` in
//! `structured-clone-table.md` and leans on the browser to enforce it. Here the
//! table is a type, so it can be tested directly.

use comlink::{ArrayBuffer, Endpoint, MessageChannel, Value};

#[test]
fn typeof_matches_javascript() {
    assert_eq!(Value::Undefined.type_of(), "undefined");
    assert_eq!(Value::Bool(true).type_of(), "boolean");
    assert_eq!(Value::Number(1.0).type_of(), "number");
    assert_eq!(Value::string("x").type_of(), "string");
    // `typeof null === "object"` -- the quirk the rethrow tests observe.
    assert_eq!(Value::Null.type_of(), "object");
    assert_eq!(Value::Array(vec![]).type_of(), "object");
    assert_eq!(Value::object(vec![]).type_of(), "object");
    assert_eq!(Value::Bytes(vec![1]).type_of(), "object");
}

#[test]
fn string_coercion_matches_javascript() {
    assert_eq!(Value::Undefined.to_string(), "undefined");
    assert_eq!(Value::Null.to_string(), "null");
    assert_eq!(Value::Bool(false).to_string(), "false");
    // `String(4)` is "4", not "4.0".
    assert_eq!(Value::Number(4.0).to_string(), "4");
    assert_eq!(Value::Number(4.5).to_string(), "4.5");
    assert_eq!(Value::Number(f64::NAN).to_string(), "NaN");
    assert_eq!(Value::Number(f64::INFINITY).to_string(), "Infinity");
    assert_eq!(Value::Number(f64::NEG_INFINITY).to_string(), "-Infinity");
    assert_eq!(Value::string("hi").to_string(), "hi");
    // `String([1,2,3])` is "1,2,3".
    assert_eq!(
        Value::Array(vec![Value::Number(1.0), Value::Number(2.0)]).to_string(),
        "1,2"
    );
    assert_eq!(Value::Bytes(vec![1, 2, 3]).to_string(), "1,2,3");
    assert_eq!(Value::object(vec![]).to_string(), "[object Object]");
}

#[test]
fn accessors_return_none_for_the_wrong_variant() {
    assert_eq!(Value::Number(1.0).as_f64(), Some(1.0));
    assert_eq!(Value::string("a").as_f64(), None);
    assert_eq!(Value::string("a").as_str(), Some("a"));
    assert_eq!(Value::Number(1.0).as_str(), None);
    assert_eq!(Value::Bool(true).as_bool(), Some(true));
    assert_eq!(Value::Null.as_bool(), None);
    assert_eq!(Value::Null.as_port(), None);
    assert!(Value::Undefined.is_undefined());
    assert!(!Value::Null.is_undefined());
}

#[test]
fn object_lookup_only_works_on_objects() {
    let obj = Value::object(vec![("a", Value::Number(1.0))]);
    assert_eq!(obj.get("a"), Some(&Value::Number(1.0)));
    assert_eq!(obj.get("missing"), None);
    assert_eq!(Value::Number(1.0).get("a"), None);
}

#[test]
fn conversions_cover_the_scalar_types() {
    assert_eq!(Value::from(4i32), Value::Number(4.0));
    assert_eq!(Value::from(4.5f64), Value::Number(4.5));
    assert_eq!(Value::from(true), Value::Bool(true));
    assert_eq!(Value::from("s"), Value::string("s"));
    assert_eq!(Value::from(String::from("s")), Value::string("s"));
    assert_eq!(Value::number(2i32), Value::Number(2.0));
}

#[test]
fn transferring_a_buffer_detaches_it() {
    let buf = ArrayBuffer::new(vec![1, 2, 3]);
    assert_eq!(buf.byte_length(), 3);
    assert!(!buf.is_detached());
    assert_eq!(buf.to_vec(), vec![1, 2, 3]);

    let taken = buf.detach();
    assert_eq!(taken, vec![1, 2, 3]);
    assert_eq!(buf.byte_length(), 0);
    assert!(buf.is_detached());
}

#[test]
fn buffer_identity_is_distinct_from_equality() {
    let a = ArrayBuffer::new(vec![1, 2]);
    let b = ArrayBuffer::new(vec![1, 2]);
    // Same contents compare equal...
    assert_eq!(a, b);
    // ...but they are not the same buffer, so detaching one leaves the other.
    assert!(!a.same(&b));
    assert!(a.same(&a.clone()));
    a.detach();
    assert_eq!(a.byte_length(), 0);
    assert_eq!(b.byte_length(), 2);
}

#[test]
fn ports_compare_by_identity() {
    let chan = MessageChannel::new();
    assert_eq!(chan.port1, chan.port1.clone());
    assert_ne!(chan.port1, chan.port2);
    assert!(!chan.port1.is_closed());
    chan.port1.close();
    assert!(chan.port1.is_closed());
}
