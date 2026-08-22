# TypeScript -> Rust

This crate is a language migration of `GoogleChromeLabs/comlink` at commit
`114a4a6448a855a613f1cb9a7c89290606c003cf` (v4.4.2 plus PR #678). The version
number is kept in step with the original so the two can be compared.

- `Comlink.wrap()` returns a `Remote` whose path is built explicitly, because
  Rust has no `Proxy`. `remote.get("a").get("b").call(args)` replaces
  `await remote.a.b(...)`.
- `Remote<T>` and `Local<T>` have no Rust equivalent and are gone. `Remote` is
  one concrete type.
- `await` becomes a blocking call. Every operation returns
  `Result<HostValue, Thrown>`.
- `Comlink.transfer()` takes the transfer list alongside the value rather than
  through a `WeakMap` keyed on object identity.
- Releasing is deterministic: `Remote` is reference counted and `Drop` releases
  the endpoint, replacing the original's `FinalizationRegistry`.
- `WireValueType.PROXY` and `WireValueType.THROW`, declared but unused upstream,
  are not carried over.
- `Remote::set` returns the `SET` response (`true`) rather than the assigned
  value. JavaScript's `await (remote.x = 5)` yields `5` because of assignment-
  expression semantics, not because comlink returns it.
- Error messages on the failure path differ in wording (not in type or effect):
  a call through a missing property reports `a.f is not a function` rather than
  `Cannot read properties of undefined (reading 'f')`.
- `windowEndpoint()` has no counterpart: a `MessagePort` already carries both
  directions. The behaviour its tests covered is in `tests/cross_context.rs`.
- Added: `BroadcastChannel`, a `MessageChannel`/`MessagePort` implementation and
  a `Worker`, all of which the browser supplied to the original for free.
