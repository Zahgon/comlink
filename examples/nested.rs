//! Behaviour probe: walking and assigning through nested plain values.
//! Paired with `verification/probes/nested.mjs`.

use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Host, HostValue, MessageChannel, Obj, Origin, Value};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    let obj = Obj::new();
    obj.put(
        "a",
        Value::object(vec![
            ("v", Value::Number(4.0)),
            ("deep", Value::object(vec![("x", Value::Number(7.0))])),
        ]),
    );
    obj.put(
        "list",
        Value::Array(vec![Value::Number(10.0), Value::Number(20.0)]),
    );
    obj.put("top", Value::Number(1.0));

    expose(
        obj as Arc<dyn Host>,
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let r = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);
    let show = |v: HostValue| v.as_value().cloned().unwrap_or(Value::Undefined).to_string();

    println!("a.v={}", show(r.get("a").get("v").value().unwrap()));
    println!(
        "a.deep.x={}",
        show(r.get("a").get("deep").get("x").value().unwrap())
    );
    println!("list.0={}", show(r.get("list").get("0").value().unwrap()));
    println!("list.1={}", show(r.get("list").get("1").value().unwrap()));
    println!("list.9={}", show(r.get("list").get("9").value().unwrap()));
    println!(
        "a.missing={}",
        show(r.get("a").get("missing").value().unwrap())
    );
    r.get("a").set("v", 9.0).unwrap();
    println!("a.v-after={}", show(r.get("a").get("v").value().unwrap()));
    r.set("top", 42.0).unwrap();
    println!("top-after={}", show(r.get("top").value().unwrap()));

    drop(r);
    chan.port1.close();
    chan.port2.close();
}
