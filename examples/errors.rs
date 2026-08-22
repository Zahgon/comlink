//! Behaviour probe: how thrown values survive the round trip.
//! Paired with `verification/probes/errors.mjs`.

use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Host, HostValue, MessageChannel, Obj, Origin, Thrown, Value};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    let obj = Obj::new();
    obj.put_method("throwsError", |_| Err(Thrown::error("OMG")));
    obj.put_method("throwsScalar", |_| Err(Thrown::Value(Value::string("oops"))));
    obj.put_method("throwsNull", |_| Err(Thrown::Value(Value::Null)));
    obj.put_method("throwsObject", |_| {
        Err(Thrown::Value(Value::object(vec![(
            "test",
            Value::Bool(true),
        )])))
    });
    expose(
        obj as Arc<dyn Host>,
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);

    let err = remote.call_method("throwsError", vec![]).unwrap_err();
    println!("error-message={}", err.message().unwrap_or_default());
    println!("error-name={}", err.name().unwrap_or_default());

    let err = remote.call_method("throwsScalar", vec![]).unwrap_err();
    let v = err.value().cloned().unwrap_or(Value::Undefined);
    println!("scalar={}", v);
    println!("scalar-type={}", v.type_of());

    let err = remote.call_method("throwsNull", vec![]).unwrap_err();
    let v = err.value().cloned().unwrap_or(Value::Undefined);
    println!("null={}", v);
    println!("null-type={}", v.type_of());

    let err = remote.call_method("throwsObject", vec![]).unwrap_err();
    let v = err.value().cloned().unwrap_or(Value::Undefined);
    println!(
        "object-test={}",
        v.get("test").cloned().unwrap_or(Value::Undefined)
    );

    drop(remote);
    chan.port1.close();
    chan.port2.close();
    let _ = HostValue::undefined();
}
