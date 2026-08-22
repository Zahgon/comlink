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

//! Translated from `tests/cross-origin.comlink.test.js` -- "Comlink origin
//! filtering", driven by `tests/fixtures/attack-iframe.html`.
//!
//! The original's attacker walks `__proto__` to reach `Object.prototype`, and
//! the assertions are about whether that pollution landed. Rust objects have no
//! prototype chain, so the payload cannot be the same -- but the thing actually
//! under test is: does `expose` act on a message whose origin is not allowed?
//! The attack here writes a property directly, and the assertions are the same
//! shape: rejected from a foreign origin, applied from an allowed one.

mod common;

use std::sync::Arc;

use comlink::{
    expose, Endpoint, Envelope, Host, HostValue, Message, MessageChannel, Obj, Origin, Value,
    WireValue,
};

use common::settle;

/// `[/^http:\/\/localhost(:[0-9]+)?\/?$/]`
fn localhost_only() -> Vec<Origin> {
    vec![Origin::predicate(|origin| {
        let rest = match origin.strip_prefix("http://localhost") {
            Some(rest) => rest,
            None => return false,
        };
        let rest = rest.strip_suffix('/').unwrap_or(rest);
        rest.is_empty()
            || (rest.starts_with(':') && rest[1..].chars().all(|c| c.is_ascii_digit()))
    })]
}

/// The crafted message `attack-iframe.html` posts.
fn attack_message() -> Envelope {
    Envelope::Request(Message::Set {
        id: "attack".to_string(),
        path: vec!["foo".to_string()],
        value: WireValue::Raw {
            value: Value::string("x"),
        },
    })
}

#[test]
fn rejects_messages_from_unknown_origin() {
    let chan = MessageChannel::new();
    let obj = Obj::new();
    obj.put("my", Value::string("value"));
    expose(
        Arc::clone(&obj) as Arc<dyn Host>,
        Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>,
        localhost_only(),
    );
    chan.port1.start();

    // A sandboxed iframe has the opaque origin "null".
    chan.port2.set_origin("null");
    chan.port2.post_message(attack_message(), vec![]);
    settle();

    // The attack failed: nothing was written, and the object is untouched.
    assert!(matches!(obj.raw("foo"), None));
    assert_eq!(
        obj.raw("my").and_then(|v| v.as_str().map(str::to_string)),
        Some("value".to_string())
    );
}

#[test]
fn accepts_messages_from_matching_origin() {
    let chan = MessageChannel::new();
    let obj = Obj::new();
    obj.put("my", Value::string("value"));
    expose(
        Arc::clone(&obj) as Arc<dyn Host>,
        Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>,
        localhost_only(),
    );
    chan.port1.start();

    chan.port2.set_origin("http://localhost:9876");
    chan.port2.post_message(attack_message(), vec![]);
    settle();

    // The write went through, and the pre-existing property is still there.
    assert_eq!(
        obj.raw("foo").and_then(|v| v.as_str().map(str::to_string)),
        Some("x".to_string())
    );
    assert_eq!(
        obj.raw("my").and_then(|v| v.as_str().map(str::to_string)),
        Some("value".to_string())
    );
}

#[test]
fn default_allows_every_origin() {
    // `expose(obj, ep)` defaults to `["*"]` in the original.
    let chan = MessageChannel::new();
    let obj = Obj::new();
    expose(
        Arc::clone(&obj) as Arc<dyn Host>,
        Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    chan.port1.start();
    chan.port2.set_origin("https://example.invalid");
    chan.port2.post_message(attack_message(), vec![]);
    settle();
    assert_eq!(
        obj.raw("foo").and_then(|v| v.as_str().map(str::to_string)),
        Some("x".to_string())
    );
}

#[test]
fn unknown_message_types_are_ignored() {
    // A response envelope on an exposed endpoint is not ours; the original
    // falls through its switch and returns.
    let chan = MessageChannel::new();
    let obj = Obj::new();
    obj.put("my", Value::string("value"));
    expose(
        Arc::clone(&obj) as Arc<dyn Host>,
        Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    chan.port1.start();
    chan.port2.post_message(
        Envelope::Response {
            id: "nobody".to_string(),
            value: WireValue::Raw {
                value: Value::Undefined,
            },
        },
        vec![],
    );
    settle();
    assert_eq!(
        obj.raw("my").and_then(|v| v.as_str().map(str::to_string)),
        Some("value".to_string())
    );
    let _ = HostValue::undefined();
}
