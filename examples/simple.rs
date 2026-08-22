//! Behaviour probe: basic property read / method call / write-back.
//! Paired with `verification/probes/simple.mjs`; both must print the same thing.

use std::sync::{Arc, Mutex};

use comlink::{expose, wrap, Endpoint, Host, HostValue, MessageChannel, Obj, Origin, Value};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    let counter = Arc::new(Mutex::new(0.0f64));
    let obj = Obj::new();
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
    {
        let c = Arc::clone(&counter);
        obj.put_method("inc", move |_| {
            *c.lock().unwrap() += 1.0;
            Ok(HostValue::undefined())
        });
    }
    obj.put_method("add", |args| {
        let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
        let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(HostValue::from(a + b))
    });

    expose(
        obj as Arc<dyn Host>,
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);

    let show = |v: HostValue| v.as_value().cloned().unwrap_or(Value::Undefined).to_string();

    println!("counter={}", show(remote.get("counter").value().unwrap()));
    remote.call_method("inc", vec![]).unwrap();
    println!("after-inc={}", show(remote.get("counter").value().unwrap()));
    println!(
        "add={}",
        show(remote.call_method("add", vec![1.into(), 3.into()]).unwrap())
    );
    println!(
        "missing={}",
        show(remote.get("counter_missing").value().unwrap())
    );
    remote.set("counter", 40.0).unwrap();
    println!("after-set={}", show(remote.get("counter").value().unwrap()));

    drop(remote);
    chan.port1.close();
    chan.port2.close();
}
