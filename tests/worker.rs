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

//! Translated from `tests/worker.comlink.test.js` -- "Comlink across workers".
//! The worker body is `tests/fixtures/worker.js`: `Comlink.expose((a, b) => a + b)`.

use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Func, HostValue, MessagePort, Origin, Worker};

/// `tests/fixtures/worker.js`
fn worker_body(port: MessagePort) {
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
    // Keep the worker alive while the endpoint is in use, the way a worker's
    // global scope outlives the script that set it up. `terminate()` closes the
    // port, which is what ends this loop.
    while !keepalive.is_closed() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn can_communicate() {
    let worker = Worker::spawn(worker_body);
    let proxy = wrap(Arc::new(worker.port()) as Arc<dyn Endpoint>);
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
    let proxy = wrap(Arc::new(worker.port()) as Arc<dyn Endpoint>);
    let other_ep = proxy.create_endpoint().unwrap();
    other_ep.start();
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
    let proxy = wrap(Arc::new(worker.port()) as Arc<dyn Endpoint>);
    let other_ep = proxy.create_endpoint().unwrap();
    other_ep.start();
    let other_proxy = wrap(Arc::new(other_ep.clone()) as Arc<dyn Endpoint>);
    assert_eq!(
        other_proxy.call(vec![20.into(), 1.into()]).unwrap().as_f64(),
        Some(21.0)
    );

    assert!(!other_ep.is_closed());
    // Release the proxy, which should close the MessagePort.
    other_proxy.release();
    assert!(other_ep.is_closed());

    drop(other_proxy);
    drop(proxy);
    worker.terminate();
}
