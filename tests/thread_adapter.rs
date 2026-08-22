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

//! Translated from `tests/node/main.mjs` and `tests/node/worker.mjs` --
//! "node > Comlink across workers", which drive Comlink through the
//! `nodeEndpoint()` adapter.

use std::sync::Arc;

use comlink::{
    expose, thread_endpoint, wrap, Endpoint, Func, HostValue, MessagePort, Origin, ThreadEndpoint,
    Worker,
};

/// `tests/node/worker.mjs`:
/// `Comlink.expose((a, b) => a + b, nodeEndpoint(parentPort))`
fn worker_body(port: MessagePort) {
    let keepalive = port.clone();
    let adapted = thread_endpoint(Arc::new(port) as Arc<dyn ThreadEndpoint>);
    expose(
        Func::new(|args| {
            let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(HostValue::from(a + b))
        }),
        adapted,
        vec![Origin::Any],
    );
    while !keepalive.is_closed() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn adapted_port(port: MessagePort) -> Arc<dyn Endpoint> {
    thread_endpoint(Arc::new(port) as Arc<dyn ThreadEndpoint>)
}

#[test]
fn can_communicate() {
    let worker = Worker::spawn(worker_body);
    let proxy = wrap(adapted_port(worker.port()));
    assert_eq!(
        proxy.call(vec![1.into(), 3.into()]).unwrap().as_f64(),
        Some(4.0)
    );
    drop(proxy);
    worker.terminate();
}

#[test]
fn can_tunnels_a_new_endpoint_with_create_endpoint() {
    let worker = Worker::spawn(worker_body);
    let proxy = wrap(adapted_port(worker.port()));
    let other_ep = proxy.create_endpoint().unwrap();
    Endpoint::start(&other_ep);
    let other_proxy = wrap(Arc::new(other_ep) as Arc<dyn Endpoint>);
    assert_eq!(
        other_proxy.call(vec![20.into(), 1.into()]).unwrap().as_f64(),
        Some(21.0)
    );
    drop(other_proxy);
    drop(proxy);
    worker.terminate();
}

#[test]
fn release_proxy_closes_message_port_created_by_create_endpoint() {
    let worker = Worker::spawn(worker_body);
    let proxy = wrap(adapted_port(worker.port()));
    let other_ep = proxy.create_endpoint().unwrap();
    Endpoint::start(&other_ep);
    let other_proxy = wrap(Arc::new(other_ep.clone()) as Arc<dyn Endpoint>);
    assert_eq!(
        other_proxy.call(vec![20.into(), 1.into()]).unwrap().as_f64(),
        Some(21.0)
    );
    assert!(!other_ep.is_closed());
    other_proxy.release();
    assert!(other_ep.is_closed());
    drop(other_proxy);
    drop(proxy);
    worker.terminate();
}

#[test]
fn adapter_removes_listeners_it_registered() {
    // `removeEventListener` on an unknown handler is a no-op in the original;
    // the adapter has to keep the mapping to find the real one.
    let chan = comlink::MessageChannel::new();
    let adapted = adapted_port(chan.port1.clone());
    let id = adapted.add_event_listener(Arc::new(|_| {}));
    adapted.remove_event_listener(id);
    adapted.remove_event_listener(9999); // unknown -- must not panic
}
