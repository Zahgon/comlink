//! Behaviour probe: constructing over the wire, statics, getters and setters.
//! Paired with `verification/probes/classes.mjs`.

use std::sync::{Arc, Mutex};

use comlink::{expose, wrap, Class, Endpoint, Host, HostValue, MessageChannel, Obj, Origin, Value};

/// `class Counter { constructor(init = 1) { this._c = init } ... }`
fn counter_class() -> Arc<Class> {
    let class = Class::new(|args: Vec<HostValue>| {
        let init = args.first().and_then(|v| v.as_f64()).unwrap_or(1.0);
        let state = Arc::new(Mutex::new(init));
        let obj = Obj::new();
        {
            let g = Arc::clone(&state);
            let s = Arc::clone(&state);
            obj.put_accessor(
                "_c",
                move || Ok(HostValue::from(*g.lock().unwrap())),
                move |v| {
                    *s.lock().unwrap() = v.as_f64().unwrap_or(0.0);
                    Ok(())
                },
            );
        }
        {
            let g = Arc::clone(&state);
            let s = Arc::clone(&state);
            obj.put_accessor(
                "value",
                move || Ok(HostValue::from(*g.lock().unwrap())),
                move |v| {
                    *s.lock().unwrap() = v.as_f64().unwrap_or(0.0);
                    Ok(())
                },
            );
        }
        {
            let c = Arc::clone(&state);
            obj.put_method("inc", move |args| {
                let d = args.first().and_then(|v| v.as_f64()).unwrap_or(1.0);
                *c.lock().unwrap() += d;
                Ok(HostValue::undefined())
            });
        }
        Ok(obj as Arc<dyn Host>)
    });
    class.statics().put("SOME", Value::Number(4.0));
    class.statics().put_method("ADD", |args| {
        let a = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
        let b = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(HostValue::from(a + b))
    });
    class
}

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    expose(
        counter_class(),
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);
    let show = |v: HostValue| v.as_value().cloned().unwrap_or(Value::Undefined).to_string();

    println!("static-prop={}", show(remote.get("SOME").value().unwrap()));
    println!(
        "static-method={}",
        show(remote.call_method("ADD", vec![1.into(), 3.into()]).unwrap())
    );

    let inst = remote.construct(vec![5.into()]).unwrap();
    println!("initial={}", show(inst.get("value").value().unwrap()));
    inst.call_method("inc", vec![]).unwrap();
    inst.call_method("inc", vec![10.into()]).unwrap();
    println!("after-inc={}", show(inst.get("value").value().unwrap()));
    inst.set("value", 2.0).unwrap();
    println!("after-set={}", show(inst.get("_c").value().unwrap()));

    drop(inst);
    drop(remote);
    chan.port1.close();
    chan.port2.close();
}
