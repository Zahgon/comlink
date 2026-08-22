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

//! Translated from `tests/iframe.comlink.test.js` and
//! `tests/two-way-iframe.comlink.test.js` -- "Comlink across iframes".
//!
//! `windowEndpoint()` exists because a window's `postMessage` takes a target
//! origin and messages arrive on a *different* object than the one you post to.
//! A `MessagePort` already has both halves, so the interesting part of these
//! tests is what they actually exercise: one endpoint carrying `expose` and
//! `wrap` at the same time, in both directions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Func, HostValue, MessagePort, Origin};

/// `tests/fixtures/iframe.html`:
/// `Comlink.expose((a, b) => a + b, Comlink.windowEndpoint(self.parent))`
fn iframe_body(port: MessagePort) {
    let keepalive = port.clone();
    expose(
        Func::new(|args| {
            let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(HostValue::from(a + b))
        }),
        Arc::new(port) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    while !keepalive.is_closed() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// `tests/fixtures/two-way-iframe.html`:
/// wraps the parent and calls back into it while serving its own calls.
fn two_way_iframe_body(port: MessagePort) {
    let keepalive = port.clone();
    let endpoint: Arc<dyn Endpoint> = Arc::new(port);
    let wrapped_parent = wrap(Arc::clone(&endpoint));
    expose(
        Func::new(move |args| {
            let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let from_parent = wrapped_parent.call(vec![b.into()])?.as_f64().unwrap_or(0.0);
            Ok(HostValue::from(a + from_parent))
        }),
        endpoint,
        vec![Origin::Any],
    );
    while !keepalive.is_closed() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn can_communicate() {
    let iframe = comlink::Worker::spawn(iframe_body);
    let proxy = wrap(Arc::new(iframe.port()) as Arc<dyn Endpoint>);
    assert_eq!(
        proxy.call(vec![1.into(), 3.into()]).unwrap().as_f64(),
        Some(4.0)
    );
    drop(proxy);
    iframe.terminate();
}

#[test]
fn can_communicate_both_ways() {
    let called = Arc::new(AtomicBool::new(false));
    let iframe = comlink::Worker::spawn(two_way_iframe_body);
    let endpoint: Arc<dyn Endpoint> = Arc::new(iframe.port());

    // This side exposes `(a) => { called = true; return ++a; }` on the very
    // same endpoint it wraps.
    let flag = Arc::clone(&called);
    expose(
        Func::new(move |args| {
            flag.store(true, Ordering::SeqCst);
            let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(HostValue::from(a + 1.0))
        }),
        Arc::clone(&endpoint),
        vec![Origin::Any],
    );

    let proxy = wrap(Arc::clone(&endpoint));
    assert_eq!(
        proxy.call(vec![1.into(), 3.into()]).unwrap().as_f64(),
        Some(5.0)
    );
    assert!(called.load(Ordering::SeqCst));
    drop(proxy);
    iframe.terminate();
}
