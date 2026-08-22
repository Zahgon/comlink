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

//! Translated from `tests/same_window.comlink.test.js`.
//! "Comlink in the same realm".

mod common;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use comlink::{
    expose, transfer, wrap, ArrayBuffer, Endpoint, Envelope, EventSource, Func, Host, HostValue,
    MessageChannel, MessageEvent, MessagePort, Obj, Origin, Thrown, TransferHandler, Transferable,
    Value,
};

use common::{counter_object, sample_class, settle};

/// `beforeEach`: a started channel.
fn pair() -> (MessagePort, MessagePort) {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();
    (chan.port1, chan.port2)
}

fn ep(port: MessagePort) -> Arc<dyn Endpoint> {
    Arc::new(port)
}

#[test]
fn can_work_with_objects() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("value", Value::Number(4.0));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(thing.get("value").number().unwrap(), 4.0);
}

#[test]
fn can_work_with_functions_on_an_object() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put_method("f", |_| Ok(HostValue::from(4.0)));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(
        thing.call_method("f", vec![]).unwrap().as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_work_with_functions() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| Ok(HostValue::from(4.0))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    assert_eq!(thing.call(vec![]).unwrap().as_f64(), Some(4.0));
}

#[test]
fn can_work_with_objects_that_have_undefined_properties() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("x", Value::Undefined);
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let x = thing.get("x").value().unwrap();
    assert!(x.as_value().unwrap().is_undefined());
}

#[test]
fn can_keep_the_stack_and_message_of_thrown_errors() {
    let (p1, p2) = pair();
    let stack = "Error: OMG\n    at knownFrame (tests/same_thread.rs:1:1)";
    let captured = stack.to_string();
    expose(
        Func::new(move |_| Err(Thrown::error_with_stack("OMG", captured.clone()))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let err = thing.call(vec![]).unwrap_err();
    assert_eq!(err.message(), Some("OMG"));
    assert_eq!(err.stack(), Some(stack));
}

#[test]
fn can_forward_an_async_function_error() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put_method("throwError", |_| Err(Thrown::error("Should have thrown")));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let err = thing.call_method("throwError", vec![]).unwrap_err();
    assert_eq!(err.message(), Some("Should have thrown"));
}

#[test]
fn can_rethrow_non_error_objects() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| {
            Err(Thrown::Value(Value::object(vec![(
                "test",
                Value::Bool(true),
            )])))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let err = thing.call(vec![]).unwrap_err();
    assert!(!err.is_error());
    assert_eq!(err.value().unwrap().get("test"), Some(&Value::Bool(true)));
}

#[test]
fn can_rethrow_scalars() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| Err(Thrown::Value(Value::string("oops")))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let err = thing.call(vec![]).unwrap_err();
    assert_eq!(err.value(), Some(&Value::string("oops")));
    assert_eq!(err.value().unwrap().type_of(), "string");
}

#[test]
fn can_rethrow_null() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| Err(Thrown::Value(Value::Null))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let err = thing.call(vec![]).unwrap_err();
    assert_eq!(err.value(), Some(&Value::Null));
    // `typeof null === "object"`.
    assert_eq!(err.value().unwrap().type_of(), "object");
}

#[test]
fn can_work_with_parameterized_functions() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(HostValue::from(a + b))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    assert_eq!(
        thing.call(vec![1.into(), 3.into()]).unwrap().as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_work_with_functions_that_return_promises() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| {
            std::thread::sleep(Duration::from_millis(100));
            Ok(HostValue::from(4.0))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    assert_eq!(thing.call(vec![]).unwrap().as_f64(), Some(4.0));
}

#[test]
fn can_work_with_classes() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(
        instance.call_method("method", vec![]).unwrap().as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_pass_parameters_to_class_constructor() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![23.into()]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 23.0);
}

#[test]
fn can_access_a_class_in_an_object() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put_class("SampleClass", sample_class());
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.get("SampleClass").construct(vec![]).unwrap();
    assert_eq!(
        instance.call_method("method", vec![]).unwrap().as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_work_with_class_instance_properties() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("_counter").number().unwrap(), 1.0);
}

#[test]
fn can_set_class_instance_properties() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("_counter").number().unwrap(), 1.0);
    instance.set("_counter", 4.0).unwrap();
    assert_eq!(instance.get("_counter").number().unwrap(), 4.0);
}

#[test]
fn can_work_with_class_instance_methods() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 1.0);
    instance.call_method("increaseCounter", vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 2.0);
}

#[test]
fn can_handle_throwing_class_instance_methods() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    let err = instance.call_method("throwsAnError", vec![]).unwrap_err();
    assert_eq!(err.message(), Some("OMG"));
}

#[test]
fn can_work_with_class_instance_methods_multiple_times() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 1.0);
    instance.call_method("increaseCounter", vec![]).unwrap();
    instance
        .call_method("increaseCounter", vec![5.into()])
        .unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 7.0);
}

#[test]
fn can_work_with_class_instance_methods_that_return_promises() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(
        instance.call_method("promiseFunc", vec![]).unwrap().as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_work_with_class_instance_properties_that_are_promises() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("_promise").number().unwrap(), 4.0);
}

#[test]
fn can_work_with_class_instance_getters_that_are_promises() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("promise").number().unwrap(), 4.0);
}

#[test]
fn can_work_with_static_class_properties() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(thing.get("SOME_NUMBER").number().unwrap(), 4.0);
}

#[test]
fn can_work_with_static_class_methods() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(
        thing
            .call_method("ADD", vec![1.into(), 3.into()])
            .unwrap()
            .as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_work_with_bound_class_instance_methods() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 1.0);
    // `instance.increaseCounter.bind(instance)` -- comlink pretends the bind
    // never happened and hands back the same path.
    let bound = instance
        .get("increaseCounter")
        .get("bind")
        .call(vec![])
        .unwrap();
    let method = bound.as_remote().expect("bind returns a proxy").clone();
    method.call(vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 2.0);
}

#[test]
fn can_work_with_class_instance_getters() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 1.0);
    instance.call_method("increaseCounter", vec![]).unwrap();
    assert_eq!(instance.get("counter").number().unwrap(), 2.0);
}

#[test]
fn can_work_with_class_instance_setters() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    assert_eq!(instance.get("_counter").number().unwrap(), 1.0);
    instance.set("counter", 4.0).unwrap();
    assert_eq!(instance.get("_counter").number().unwrap(), 4.0);
}

#[test]
fn will_work_with_broadcast_channel() {
    use comlink::{start_broadcast, BroadcastChannel};
    let b1 = BroadcastChannel::new("comlink_bc_test");
    let b2 = BroadcastChannel::new("comlink_bc_test");
    start_broadcast(&b1);
    start_broadcast(&b2);
    expose(
        Func::new(|args| {
            let b = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(HostValue::from(40.0 + b))
        }),
        b2.clone() as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let thing = wrap(b1.clone() as Arc<dyn Endpoint>);
    assert_eq!(thing.call(vec![2.into()]).unwrap().as_f64(), Some(42.0));
    // Release the proxy before tearing the channels down, so the release
    // handshake still has somewhere to go.
    drop(thing);
    b1.close();
    b2.close();
}

#[test]
fn will_transfer_buffers() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let len = match args.first().and_then(|v| v.as_value()) {
                Some(Value::Buffer(b)) => b.byte_length(),
                Some(Value::Bytes(b)) => b.len(),
                _ => 0,
            };
            Ok(HostValue::from(len as f64))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let buffer = ArrayBuffer::new(vec![1, 2, 3]);
    let result = thing
        .call(vec![transfer(
            Value::Buffer(buffer.clone()),
            vec![Transferable::Buffer(buffer.clone())],
        )])
        .unwrap();
    assert_eq!(result.as_f64(), Some(3.0));
    // The buffer was transferred, not copied: the sender's is now detached.
    assert_eq!(buffer.byte_length(), 0);
}

#[test]
fn will_copy_typed_arrays() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| Ok(args.into_iter().next().unwrap_or_else(HostValue::undefined))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let array = vec![1u8, 2, 3];
    let received = thing
        .call(vec![Value::Bytes(array.clone()).into()])
        .unwrap();
    match received.as_value() {
        Some(Value::Bytes(got)) => {
            assert_eq!(got.len(), array.len());
            assert_eq!(got, &array);
        }
        other => panic!("expected bytes back, got {:?}", other),
    }
    // A copy, so the original is untouched.
    assert_eq!(array, vec![1u8, 2, 3]);
}

#[test]
fn will_copy_nested_typed_arrays() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| Ok(args.into_iter().next().unwrap_or_else(HostValue::undefined))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let array = vec![1u8, 2, 3];
    let received = thing
        .call(vec![Value::object(vec![
            ("v", Value::Number(1.0)),
            ("array", Value::Bytes(array.clone())),
        ])
        .into()])
        .unwrap();
    let got = received.as_value().and_then(|v| v.get("array")).cloned();
    assert_eq!(got, Some(Value::Bytes(array.clone())));
    assert_eq!(array, vec![1u8, 2, 3]);
}

#[test]
fn will_transfer_deeply_nested_buffers() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let len = args
                .first()
                .and_then(|v| v.as_value())
                .and_then(|v| v.get("b"))
                .and_then(|v| v.get("c"))
                .and_then(|v| v.get("d"))
                .map(|v| match v {
                    Value::Buffer(b) => b.byte_length(),
                    Value::Bytes(b) => b.len(),
                    _ => 0,
                })
                .unwrap_or(0);
            Ok(HostValue::from(len as f64))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let buffer = ArrayBuffer::new(vec![1, 2, 3]);
    let nested = Value::object(vec![(
        "b",
        Value::object(vec![(
            "c",
            Value::object(vec![("d", Value::Buffer(buffer.clone()))]),
        )]),
    )]);
    let result = thing
        .call(vec![transfer(
            nested,
            vec![Transferable::Buffer(buffer.clone())],
        )])
        .unwrap();
    assert_eq!(result.as_f64(), Some(3.0));
    assert_eq!(buffer.byte_length(), 0);
}

#[test]
fn will_transfer_a_message_port() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let port = args
                .first()
                .and_then(|v| v.as_value())
                .and_then(|v| v.as_port())
                .cloned()
                .expect("a port was transferred");
            port.post_message(Envelope::Data(Value::string("ohai")), vec![]);
            Ok(HostValue::undefined())
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));

    let chan = MessageChannel::new();
    let (tx, rx) = mpsc::channel();
    chan.port1.add_event_listener(Arc::new(move |ev: &MessageEvent| {
        if let Envelope::Data(Value::String(s)) = &ev.data {
            let _ = tx.send(s.clone());
        }
    }));
    chan.port1.start();

    thing
        .call(vec![transfer(
            Value::Port(chan.port2.clone()),
            vec![Transferable::Port(chan.port2.clone())],
        )])
        .unwrap();

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "ohai".to_string()
    );
}

#[test]
fn will_wrap_marked_return_values() {
    let (p1, p2) = pair();
    expose(
        Func::new(|_| Ok(HostValue::Proxied(counter_object() as Arc<dyn Host>))),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let obj = thing.call(vec![]).unwrap();
    let obj = obj.as_remote().expect("a proxy came back").clone();
    assert_eq!(obj.get("counter").number().unwrap(), 0.0);
    obj.call_method("inc", vec![]).unwrap();
    assert_eq!(obj.get("counter").number().unwrap(), 1.0);
}

#[test]
fn will_wrap_marked_return_values_from_class_instance_methods() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    let obj = instance.call_method("proxyFunc", vec![]).unwrap();
    let obj = obj.as_remote().expect("a proxy came back").clone();
    assert_eq!(obj.get("counter").number().unwrap(), 0.0);
    obj.call_method("inc", vec![]).unwrap();
    assert_eq!(obj.get("counter").number().unwrap(), 1.0);
}

#[test]
fn will_wrap_marked_parameter_values() {
    let (p1, p2) = pair();
    let local = counter_object();
    expose(
        Func::new(|args| {
            let f = args
                .first()
                .and_then(|v| v.as_remote())
                .expect("a proxied argument")
                .clone();
            f.call_method("inc", vec![])?;
            Ok(HostValue::undefined())
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    assert_eq!(
        local.raw("counter").and_then(|v| v.as_f64()),
        Some(0.0)
    );
    thing
        .call(vec![HostValue::Proxied(
            Arc::clone(&local) as Arc<dyn Host>
        )])
        .unwrap();
    assert_eq!(
        local.raw("counter").and_then(|v| v.as_f64()),
        Some(1.0)
    );
}

#[test]
fn will_wrap_marked_assignments() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("onready", Value::Null);
    let weak = Arc::downgrade(&obj);
    obj.put_method("call", move |_| {
        let o = weak.upgrade().expect("object still alive");
        match o.raw("onready") {
            Some(HostValue::Remote(cb)) => {
                cb.call(vec![])?;
                Ok(HostValue::undefined())
            }
            other => Err(Thrown::type_error(format!(
                "onready is not callable: {:?}",
                other
            ))),
        }
    });
    expose(Arc::clone(&obj) as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    let (tx, rx) = mpsc::channel();
    let done = Arc::new(Mutex::new(Some(tx)));
    let callback = Func::new(move |_| {
        if let Some(tx) = done.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(HostValue::undefined())
    });

    let thing = wrap(ep(p1));
    thing
        .set("onready", HostValue::Proxied(callback as Arc<dyn Host>))
        .unwrap();
    thing.call_method("call", vec![]).unwrap();
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the marked assignment was invoked");
}

#[test]
fn will_wrap_marked_parameter_values_simple_function() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let f = args
                .first()
                .and_then(|v| v.as_remote())
                .expect("a proxied argument")
                .clone();
            f.call(vec![])?;
            Ok(HostValue::undefined())
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));

    let (tx, rx) = mpsc::channel();
    let done = Arc::new(Mutex::new(Some(tx)));
    let callback = Func::new(move |_| {
        if let Some(tx) = done.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(HostValue::undefined())
    });
    thing
        .call(vec![HostValue::Proxied(callback as Arc<dyn Host>)])
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the proxied callback ran");
}

#[test]
fn will_wrap_multiple_marked_parameter_values_simple_function() {
    let (p1, p2) = pair();
    expose(
        Func::new(|args| {
            let mut total = 0.0;
            for a in &args {
                let f = a.as_remote().expect("a proxied argument").clone();
                total += f.call(vec![])?.as_f64().unwrap_or(0.0);
            }
            Ok(HostValue::from(total))
        }),
        ep(p2),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p1));
    let mk = |n: f64| Func::new(move |_| Ok(HostValue::from(n))) as Arc<dyn Host>;
    let result = thing
        .call(vec![
            HostValue::Proxied(mk(1.0)),
            HostValue::Proxied(mk(2.0)),
            HostValue::Proxied(mk(3.0)),
        ])
        .unwrap();
    assert_eq!(result.as_f64(), Some(6.0));
}

#[test]
fn will_proxy_deeply_nested_values() {
    let (p1, p2) = pair();
    let plain = Obj::new();
    plain.put("v", Value::Number(4.0));
    let marked = Obj::new();
    marked.put("v", Value::Number(5.0));

    let obj = Obj::new();
    obj.put_child("a", plain as Arc<dyn Host>);
    obj.put_proxied("b", Arc::clone(&marked) as Arc<dyn Host>);
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    let thing = wrap(ep(p1));
    // `a` is cloned; `b` comes back as a proxy.
    let a = thing.get("a").value().unwrap();
    let b = thing.get("b").value().unwrap();
    assert_eq!(
        a.as_value().and_then(|v| v.get("v")).and_then(|v| v.as_f64()),
        Some(4.0)
    );
    let b = b.as_remote().expect("b is proxied").clone();
    assert_eq!(b.get("v").number().unwrap(), 5.0);

    // Writing through the clone changes nothing on the far side; writing
    // through the proxy does.
    b.set("v", 9.0).unwrap();
    assert_eq!(thing.get("a").get("v").number().unwrap(), 4.0);
    assert_eq!(thing.get("b").get("v").number().unwrap(), 9.0);
}

#[test]
fn will_handle_undefined_parameters() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put_method("f", |_| Ok(HostValue::from(4.0)));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(
        thing
            .call_method("f", vec![HostValue::undefined()])
            .unwrap()
            .as_f64(),
        Some(4.0)
    );
}

#[test]
fn can_handle_destructuring() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("a", Value::Number(4.0));
    obj.put_getter("b", || Ok(HostValue::from(5.0)));
    obj.put_method("c", |_| Ok(HostValue::from(6.0)));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    let thing = wrap(ep(p1));
    // `const { a, b, c } = Comlink.wrap(port)` -- three independent paths.
    let a = thing.get("a");
    let b = thing.get("b");
    let c = thing.get("c");
    assert_eq!(a.number().unwrap(), 4.0);
    assert_eq!(b.number().unwrap(), 5.0);
    assert_eq!(c.call(vec![]).unwrap().as_f64(), Some(6.0));
}

#[test]
fn lets_users_define_transfer_handlers() {
    struct EventHandler;
    impl TransferHandler for EventHandler {
        fn can_handle(&self, value: &HostValue) -> bool {
            matches!(value, HostValue::Tagged { name, .. } if name == "event")
        }
        fn serialize(&self, value: HostValue) -> (Value, Vec<Transferable>) {
            // `serialize: (ev) => [ev.data, []]`
            match value {
                HostValue::Tagged { value, .. } => (
                    value.get("data").cloned().unwrap_or(Value::Undefined),
                    Vec::new(),
                ),
                _ => (Value::Undefined, Vec::new()),
            }
        }
        fn deserialize(&self, value: Value) -> Result<HostValue, Thrown> {
            // `deserialize: (data) => new MessageEvent("message", { data })`
            Ok(HostValue::Tagged {
                name: "event".to_string(),
                value: Value::object(vec![("data", value)]),
            })
        }
    }
    comlink::set_transfer_handler("event", Arc::new(EventHandler));

    let (p1, p2) = pair();
    let (tx, rx) = mpsc::channel();
    let seen = Arc::new(Mutex::new(Some(tx)));
    expose(
        Func::new(move |args| {
            let ev = args.first().cloned().expect("one argument");
            match &ev {
                HostValue::Tagged { name, value } => {
                    assert_eq!(name, "event");
                    assert_eq!(
                        value.get("data").and_then(|d| d.get("a")),
                        Some(&Value::Number(1.0))
                    );
                }
                other => panic!("expected an event, got {:?}", other),
            }
            if let Some(tx) = seen.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Ok(HostValue::undefined())
        }),
        ep(p1),
        vec![Origin::Any],
    );
    let thing = wrap(ep(p2));
    thing
        .call(vec![HostValue::Tagged {
            name: "event".to_string(),
            value: Value::object(vec![(
                "data",
                Value::object(vec![("a", Value::Number(1.0))]),
            )]),
        }])
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the handler ran");
    comlink::remove_transfer_handler("event");
}

#[test]
fn can_tunnels_a_new_endpoint_with_create_endpoint() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("a", Value::Number(4.0));
    obj.put_method("c", |_| Ok(HostValue::from(5.0)));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    let proxy = wrap(ep(p1));
    let other_ep = proxy.create_endpoint().unwrap();
    other_ep.start();
    let other_proxy = wrap(Arc::new(other_ep) as Arc<dyn Endpoint>);

    assert_eq!(other_proxy.get("a").number().unwrap(), 4.0);
    assert_eq!(proxy.get("a").number().unwrap(), 4.0);
    assert_eq!(
        other_proxy.call_method("c", vec![]).unwrap().as_f64(),
        Some(5.0)
    );
    assert_eq!(proxy.call_method("c", vec![]).unwrap().as_f64(), Some(5.0));
}

#[test]
fn released_proxy_should_no_longer_be_useable_and_throw_an_exception() {
    let (p1, p2) = pair();
    expose(sample_class(), ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let instance = thing.construct(vec![]).unwrap();
    instance.release();
    let err = instance.call_method("method", vec![]).unwrap_err();
    assert_eq!(
        err.message(),
        Some("Proxy has been released and is not useable")
    );
}

#[test]
fn released_proxy_should_invoke_finalizer() {
    let (p1, p2) = pair();
    let finalized = Arc::new(Mutex::new(false));
    let obj = Obj::new();
    obj.put("a", Value::string("thing"));
    {
        let flag = Arc::clone(&finalized);
        obj.put_finalizer(move || *flag.lock().unwrap() = true);
    }
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    let instance = wrap(ep(p1));
    assert_eq!(instance.get("a").string().unwrap(), "thing");
    instance.release();
    // Wait a beat to let the events process.
    settle();
    assert!(*finalized.lock().unwrap());
}

#[test]
fn released_proxy_via_gc_should_invoke_finalizer() {
    // Same scenario as the original's `it.skip`ped case: release happens on its
    // own, with no explicit call. The original skips it because it depends on
    // when a garbage collection runs. Rust's equivalent is `Drop`, which is
    // deterministic -- so the behaviour the original could only hope for is
    // actually asserted here.
    let (p1, p2) = pair();
    let finalized = Arc::new(Mutex::new(false));
    let obj = Obj::new();
    obj.put("a", Value::string("thing"));
    {
        let flag = Arc::clone(&finalized);
        obj.put_finalizer(move || *flag.lock().unwrap() = true);
    }
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);

    {
        let instance = wrap(ep(p1));
        assert_eq!(instance.get("a").string().unwrap(), "thing");
        assert!(!*finalized.lock().unwrap());
    } // last proxy dropped here

    settle();
    assert!(*finalized.lock().unwrap());
}

#[test]
fn can_proxy_with_a_given_target() {
    // The original's second `wrap()` argument only supplies the object the
    // `Proxy` is built over; it never changes what crosses the wire. There is
    // no `Proxy` here, so there is nothing for it to configure -- the
    // observable behaviour it was asserted against is what is checked.
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("value", Value::Number(4.0));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    assert_eq!(thing.get("value").number().unwrap(), 4.0);
}

#[test]
fn can_handle_unserializable_types() {
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put_method("value", |_| Ok(HostValue::from("boom")));
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));
    let err = thing.get("value").value().unwrap_err();
    assert_eq!(err.message(), Some("Unserializable return value"));
}

#[test]
fn can_walk_into_plain_nested_values() {
    // `Comlink.expose({ a: { v: 4 }, list: [10, 20] })` -- the original resolves
    // `a.v` and `list[0]` by reducing the path over the object, so a nested
    // plain value has to keep being walked rather than stopping at the first
    // segment.
    let (p1, p2) = pair();
    let obj = Obj::new();
    obj.put("a", Value::object(vec![("v", Value::Number(4.0))]));
    obj.put(
        "list",
        Value::Array(vec![Value::Number(10.0), Value::Number(20.0)]),
    );
    expose(obj as Arc<dyn Host>, ep(p2), vec![Origin::Any]);
    let thing = wrap(ep(p1));

    assert_eq!(thing.get("a").get("v").number().unwrap(), 4.0);
    assert_eq!(thing.get("list").get("0").number().unwrap(), 10.0);
    assert_eq!(thing.get("list").get("1").number().unwrap(), 20.0);
    // Out of range and missing keys are `undefined`, not errors.
    assert!(thing
        .get("list")
        .get("9")
        .value()
        .unwrap()
        .as_value()
        .unwrap()
        .is_undefined());
    assert!(thing
        .get("a")
        .get("nope")
        .value()
        .unwrap()
        .as_value()
        .unwrap()
        .is_undefined());

    // ...and assignment reaches the same place.
    thing.get("a").set("v", 9.0).unwrap();
    assert_eq!(thing.get("a").get("v").number().unwrap(), 9.0);
}
