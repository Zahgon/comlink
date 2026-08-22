//! Behaviour probe: transferring a buffer detaches it on the sending side.
//! Paired with `verification/probes/transfer.mjs`.

use std::sync::Arc;

use comlink::{
    expose, transfer, wrap, ArrayBuffer, Endpoint, Func, HostValue, MessageChannel, Origin,
    Transferable, Value,
};

fn main() {
    let chan = MessageChannel::new();
    chan.port1.start();
    chan.port2.start();

    expose(
        Func::new(|args| {
            let len = match args.first().and_then(|v| v.as_value()) {
                Some(Value::Buffer(b)) => b.byte_length(),
                Some(Value::Bytes(b)) => b.len(),
                _ => 0,
            };
            Ok(HostValue::from(len as f64))
        }),
        Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
        vec![Origin::Any],
    );
    let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);

    let buffer = ArrayBuffer::new(vec![1, 2, 3, 4, 5]);
    println!("before={}", buffer.byte_length());
    let received = remote
        .call(vec![transfer(
            Value::Buffer(buffer.clone()),
            vec![Transferable::Buffer(buffer.clone())],
        )])
        .unwrap();
    println!(
        "received={}",
        received.as_value().cloned().unwrap_or(Value::Undefined)
    );
    println!("after={}", buffer.byte_length());

    drop(remote);
    chan.port1.close();
    chan.port2.close();
}
