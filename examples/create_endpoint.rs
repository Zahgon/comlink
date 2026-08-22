//! Behaviour probe: tunnelling a second endpoint onto the same object.
//! Paired with `verification/probes/create_endpoint.mjs`.

use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Host, HostValue, MessageChannel, Obj, Origin, Value};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    let obj = Obj::new();
    obj.put("a", Value::Number(4.0));
    obj.put_method("c", |_| Ok(HostValue::from(5.0)));
    expose(
        obj as Arc<dyn Host>,
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );

    let proxy = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);
    let other = proxy.create_endpoint().unwrap();
    other.start();
    let other_proxy = wrap(Arc::new(other) as Arc<dyn Endpoint>);
    let show = |v: HostValue| v.as_value().cloned().unwrap_or(Value::Undefined).to_string();

    println!("a={}", show(other_proxy.get("a").value().unwrap()));
    println!("c={}", show(other_proxy.call_method("c", vec![]).unwrap()));
    println!("orig-a={}", show(proxy.get("a").value().unwrap()));

    drop(other_proxy);
    drop(proxy);
    chan.port1.close();
    chan.port2.close();
}
