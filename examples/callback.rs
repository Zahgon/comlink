//! Behaviour probe: values marked with `proxy()` as call arguments.
//! Paired with `verification/probes/callback.mjs`.

use std::sync::Arc;

use comlink::{expose, wrap, Endpoint, Func, Host, HostValue, MessageChannel, Obj, Origin, Value};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    expose(
        Func::new(|args| {
            let cb = args.first().and_then(|v| v.as_remote()).unwrap().clone();
            let other = args.get(1).and_then(|v| v.as_remote()).unwrap().clone();
            let a = cb.call(vec![2.into()])?.as_f64().unwrap_or(0.0);
            let b = other
                .call_method("double", vec![21.into()])?
                .as_f64()
                .unwrap_or(0.0);
            Ok(HostValue::from(a + b))
        }),
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);

    let increment = Func::new(|args| {
        let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(HostValue::from(x + 1.0))
    });
    let helper = Obj::new();
    helper.put_method("double", |args| {
        let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(HostValue::from(x * 2.0))
    });

    let result = remote
        .call(vec![
            HostValue::Proxied(increment as Arc<dyn Host>),
            HostValue::Proxied(helper as Arc<dyn Host>),
        ])
        .unwrap();
    println!(
        "result={}",
        result.as_value().cloned().unwrap_or(Value::Undefined)
    );

    drop(remote);
    chan.port1.close();
    chan.port2.close();
}
