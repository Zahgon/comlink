# What can be sent over an endpoint

The TypeScript original inherits this table from the browser's structured clone
algorithm. Rust has no such algorithm, so the crate names the set explicitly:
the [`Value`] enum *is* the table.

| `Value` variant | JavaScript counterpart | Copied | Transferable |
| --------------- | ---------------------- | ------ | ------------ |
| `Undefined`     | `undefined`            | yes    | no           |
| `Null`          | `null`                 | yes    | no           |
| `Bool`          | `boolean`              | yes    | no           |
| `Number`        | `number`               | yes    | no           |
| `String`        | `string`               | yes    | no           |
| `Bytes`         | `TypedArray`           | yes    | no           |
| `Buffer`        | `ArrayBuffer`          | yes    | **yes**      |
| `Array`         | `Array`                | yes    | no           |
| `Object`        | plain object           | yes    | no           |
| `Port`          | `MessagePort`          | no     | **yes**      |

Anything outside this set has to go through a
[`TransferHandler`](./README.md#transfer-handlers), or be sent as a proxy with
`proxy()`.

## Notes

* `Bytes` is a copy; `Buffer` can be moved. Naming a `Buffer` in a transfer list
  detaches it: the sender is left with `byte_length() == 0` and the receiver
  gets the bytes. That mirrors a transferred `ArrayBuffer` exactly.
* A `Port` is never copied. Sending one hands the other side the same endpoint,
  which is how `proxy()` and `create_endpoint()` both work.
* Functions are not in the table, in either language. Returning one produces
  `TypeError: Unserializable return value` — the same message the original
  produces when `postMessage` raises a `DataCloneError`.
