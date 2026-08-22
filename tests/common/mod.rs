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

//! Shared fixtures. `SampleClass` is the one the original's suite defines at the
//! top of `tests/same_window.comlink.test.js`.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use comlink::{Class, Host, HostValue, Obj, Thrown, Value};

/// `{ counter: 0, inc() { this.counter++ } }` -- the object the original marks
/// with `Comlink.proxy()` in several tests.
pub fn counter_object() -> Arc<Obj> {
    let obj = Obj::new();
    obj.put("counter", Value::Number(0.0));
    let weak = Arc::downgrade(&obj);
    obj.put_method("inc", move |_args| {
        if let Some(o) = weak.upgrade() {
            let current = o
                .raw("counter")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            o.put("counter", Value::Number(current + 1.0));
        }
        Ok(HostValue::undefined())
    });
    obj
}

/// An instance of `SampleClass`.
fn sample_instance(counter_init: f64) -> Arc<dyn Host> {
    let counter = Arc::new(Mutex::new(counter_init));
    let obj = Obj::new();

    // `this._counter`, readable and writable.
    {
        let g = Arc::clone(&counter);
        let s = Arc::clone(&counter);
        obj.put_accessor(
            "_counter",
            move || Ok(HostValue::from(*g.lock().unwrap())),
            move |v| {
                *s.lock().unwrap() = v.as_f64().unwrap_or(0.0);
                Ok(())
            },
        );
    }

    // `this._promise = Promise.resolve(4)` -- awaiting the property yields 4.
    obj.put("_promise", Value::Number(4.0));

    // `get counter()` / `set counter(value)`
    {
        let g = Arc::clone(&counter);
        let s = Arc::clone(&counter);
        obj.put_accessor(
            "counter",
            move || Ok(HostValue::from(*g.lock().unwrap())),
            move |v| {
                *s.lock().unwrap() = v.as_f64().unwrap_or(0.0);
                Ok(())
            },
        );
    }

    // `get promise()`
    obj.put_getter("promise", || Ok(HostValue::from(4.0)));

    obj.put_method("method", |_| Ok(HostValue::from(4.0)));

    {
        let c = Arc::clone(&counter);
        obj.put_method("increaseCounter", move |args| {
            // `increaseCounter(delta = 1)`
            let delta = args.first().and_then(|a| a.as_f64()).unwrap_or(1.0);
            *c.lock().unwrap() += delta;
            Ok(HostValue::undefined())
        });
    }

    // `promiseFunc()` -- resolves to 4 after a delay.
    obj.put_method("promiseFunc", |_| {
        thread::sleep(Duration::from_millis(100));
        Ok(HostValue::from(4.0))
    });

    // `proxyFunc()` -- returns a value marked with `Comlink.proxy()`.
    obj.put_method("proxyFunc", |_| {
        Ok(HostValue::Proxied(counter_object() as Arc<dyn Host>))
    });

    obj.put_method("throwsAnError", |_| Err(Thrown::error("OMG")));

    obj as Arc<dyn Host>
}

/// `class SampleClass { ... }`, statics included.
pub fn sample_class() -> Arc<Class> {
    let class = Class::new(|args: Vec<HostValue>| {
        let init = args.first().and_then(|a| a.as_f64()).unwrap_or(1.0);
        Ok(sample_instance(init))
    });
    // `static get SOME_NUMBER()` and `static ADD(a, b)`
    class.statics().put("SOME_NUMBER", Value::Number(4.0));
    class.statics().put_method("ADD", |args| {
        let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
        let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(HostValue::from(a + b))
    });
    class
}

/// The original waits a tick for events to drain; so does this.
pub fn settle() {
    thread::sleep(Duration::from_millis(50));
}
