# Comlink

Comlink makes worker threads enjoyable. It is a **dependency-free crate** that
removes the mental barrier of thinking about message passing and hides the fact
that you are working with another thread.

At a more abstract level it is an RPC implementation for channel endpoints.

```toml
[dependencies]
comlink = "4.4"
```

> **This crate is a migration.** It is a translation of
> [GoogleChromeLabs/comlink][upstream] (TypeScript) at commit
> `114a4a6448a855a613f1cb9a7c89290606c003cf`. See
> [What changed on the way from TypeScript](#what-changed-on-the-way-from-typescript).

## Introduction

Keeping the main thread idle matters: it should be free to respond to whatever
the program's users are doing. Threads let you run code elsewhere, and channels
let the two sides talk — but a channel gives you `post` and `recv`, not a value
you can use.

Comlink turns that message-based API into something friendlier: values from one
thread can be used from the other almost like local values.

## Examples

### Running a simple function

```rust
use std::sync::Arc;
use comlink::{expose, wrap, Endpoint, Func, HostValue, MessageChannel, Origin};

let chan = MessageChannel::new();
chan.port1.start();
chan.port2.start();

// The far side exposes a function.
expose(
    Func::new(|args: Vec<HostValue>| {
        let a = args[0].as_f64().unwrap_or(0.0);
        let b = args[1].as_f64().unwrap_or(0.0);
        Ok(HostValue::from(a + b))
    }),
    Arc::new(chan.port2.clone()) as Arc<dyn Endpoint>,
    vec![Origin::Any],
);

// This side calls it.
let remote = wrap(Arc::new(chan.port1.clone()) as Arc<dyn Endpoint>);
assert_eq!(remote.call(vec![1.into(), 3.into()]).unwrap().as_f64(), Some(4.0));
```

### Working with an object

```rust
use comlink::{Obj, Value};

let obj = Obj::new();
obj.put("counter", Value::Number(0.0));
obj.put_method("inc", |_| Ok(HostValue::undefined()));
expose(obj as Arc<dyn Host>, endpoint, vec![Origin::Any]);

// on the other side
let counter = remote.get("counter").number()?;
remote.call_method("inc", vec![])?;
```

### Callbacks

A value cannot be cloned into another thread if it is a closure, so send a proxy
instead:

```rust
let callback = Func::new(|args| { /* ... */ Ok(HostValue::undefined()) });
remote.call(vec![HostValue::Proxied(callback as Arc<dyn Host>)])?;
```

Further worked examples live in [`examples/`](./examples): `simple`, `errors`,
`classes`, `callback`, `transfer` and `create_endpoint`. Run one with
`cargo run --example simple`.

## API

### `expose(value, endpoint, allowed_origins)` and `wrap(endpoint)`

`expose` publishes a value on an endpoint. `wrap` takes the *other* end and
returns a [`Remote`]. Every access through the proxy is fallible and blocking:
a call that returns a number returns `Result<HostValue, Thrown>`. Errors raised
on the far side are re-raised here.

`allowed_origins` filters by the origin stamped on incoming messages —
`vec![Origin::Any]` is the permissive default the original uses.

### Building a path

There is no `Proxy` in Rust, so the path an access would have accumulated is
built explicitly and sent by a terminal operation:

| TypeScript | Rust |
|---|---|
| `await remote.counter` | `remote.get("counter").value()?` |
| `await remote.inc(1)` | `remote.call_method("inc", vec![1.into()])?` |
| `remote.x = 4` | `remote.set("x", 4.into())?` |
| `await new remote(a)` | `remote.construct(vec![a])?` |
| `await remote[createEndpoint]()` | `remote.create_endpoint()?` |
| `remote[releaseProxy]()` | `remote.release()` |

### `transfer(value, transferables)` and `proxy(value)`

By default every argument, return value and property is copied. Wrap a value in
`transfer()` to move it instead — a transferred [`ArrayBuffer`] is detached on
the sending side, and its `byte_length()` drops to zero.

`proxy(value)` sends neither a copy nor the bytes, but a proxy: both sides work
on the same value. This is what callbacks need.

### Transfer handlers

Register a [`TransferHandler`] under a name on **both** sides to customise how a
value is serialised:

```rust
comlink::set_transfer_handler("event", Arc::new(MyEventHandler));
```

### `Host`

Anything exposed implements [`Host`]: `get`, `set`, `apply`, `construct` and an
optional `finalizer`. [`Obj`], [`Func`] and [`Class`] cover the usual cases;
implement the trait directly for anything else.

## What changed on the way from TypeScript

Most of the library carried over unchanged: the wire protocol, the transfer
handlers, the origin filter, the release and finalizer lifecycle, and the error
semantics — including that a thrown scalar, `null` or plain object arrives as
itself rather than being coerced into an error.

Three things could not come across as they were:

* **`Proxy`.** The original turns `await remote.a.b()` into a path plus a trap.
  Rust cannot intercept field access, so the path is built explicitly. This is
  the one place the API had to change shape.
* **`Remote<T>` and `Local<T>`.** Those are conditional mapped types with no
  runtime footprint. Rust has no equivalent, so [`Remote`] is a single concrete
  type and the compile-time guarantees the original offered are not reproduced.
* **`await`.** A call blocks the calling thread until the answer arrives. The
  ordering guarantee is the same one a promise gives; the cost is a parked
  thread instead of a suspended task.

Two smaller differences worth knowing:

* **`set` returns the protocol's answer, not the assigned value.** In JavaScript
  `await (remote.x = 5)` evaluates to `5`, because an assignment *expression* has
  the right-hand side as its value — comlink itself answers a `SET` with `true`.
  `Remote::set` hands back that `true`. Nothing in the original's suite depends
  on the difference.
* **Error text for a call through a missing property differs.** The original
  reports `Cannot read properties of undefined (reading 'f')`; this reports
  `a.f is not a function`. Both are `TypeError`s, both reject, and both leave the
  endpoint usable.

Two things came out better:

* **Release is deterministic.** The original releases an endpoint when a
  `FinalizationRegistry` notices the last proxy was collected — best-effort, and
  the matching test is `it.skip`ped because it depends on when a GC runs. Here
  [`Remote`] is reference counted and `Drop` releases, so that test is a real
  one.
* **The wire format has no dead variants.** `WireValueType.PROXY` and
  `WireValueType.THROW` are declared but never used upstream; they are not here.

The structured-clone table has a Rust counterpart in
[`structured-clone-table.md`](./structured-clone-table.md).

## Tests

```
cargo test
```

The suite is a translation of the original's, case for case: `same_thread.rs`
from `same_window.comlink.test.js`, `worker.rs` from `worker.comlink.test.js`,
`thread_adapter.rs` from `tests/node/`, `origin_filter.rs` from
`cross-origin.comlink.test.js`, and `cross_context.rs` from the two iframe
suites.

[upstream]: https://github.com/GoogleChromeLabs/comlink

---

License Apache-2.0
