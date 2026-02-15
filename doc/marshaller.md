# Marshaller

The marshaller converts between JavaScript/TypeScript objects and DynamoDB's `AttributeValue` format. It is used internally by `DataMapper`, but can also be used directly for advanced scenarios.

## marshallItem

Convert a full JS object to a DynamoDB item, using the schema to determine types:

```typescript
import { marshallItem } from "@fgiova/mini-dynamo-mapper";

const schema = {
  pk: { type: "String", keyType: "HASH" },
  name: { type: "String" },
  age: { type: "Number" },
  active: { type: "Boolean" },
};

const dynamoItem = marshallItem(schema, {
  pk: "USER#1",
  name: "Alice",
  age: 30,
  active: true,
});
// { pk: { S: "USER#1" }, name: { S: "Alice" }, age: { N: "30" }, active: { BOOL: true } }
```

- Fields with `attributeName` are stored under the mapped name
- Fields with `defaultProvider` get their default when the value is `undefined`
- `undefined` fields are omitted from the result

## marshallKey

Extract and marshall only the key fields (attributes with `keyType`) from an object:

```typescript
import { marshallKey } from "@fgiova/mini-dynamo-mapper";

const key = marshallKey(schema, { pk: "USER#1", name: "ignored" });
// { pk: { S: "USER#1" } }
```

## marshallValue

Marshall a single value according to its schema type:

```typescript
import { marshallValue } from "@fgiova/mini-dynamo-mapper";

marshallValue({ type: "String" }, "hello");    // { S: "hello" }
marshallValue({ type: "Number" }, 42);          // { N: "42" }
marshallValue({ type: "Boolean" }, true);       // { BOOL: true }
marshallValue({ type: "Date" }, new Date());    // { N: "1700000000" } (unix seconds)
marshallValue({ type: "Binary" }, buffer);      // { B: "base64..." }
marshallValue({ type: "Null" }, null);          // { NULL: true }
```

## autoMarshallValue

Automatically detect the JS type and marshall accordingly. Used for `Any`, `Collection`, and `Hash` types:

```typescript
import { autoMarshallValue } from "@fgiova/mini-dynamo-mapper";

autoMarshallValue("hello");           // { S: "hello" }
autoMarshallValue(42);                // { N: "42" }
autoMarshallValue(true);              // { BOOL: true }
autoMarshallValue(new Date());        // { N: "unix_seconds" }
autoMarshallValue(new Uint8Array()); // { B: "base64" }
autoMarshallValue([1, 2, 3]);        // { L: [{ N: "1" }, { N: "2" }, { N: "3" }] }
autoMarshallValue({ a: 1 });          // { M: { a: { N: "1" } } }
autoMarshallValue(new Set(["a"]));   // { SS: ["a"] }
autoMarshallValue(new Set([1, 2]));  // { NS: ["1", "2"] }
autoMarshallValue(null);              // { NULL: true }
```

## unmarshallItem

Convert a DynamoDB item back to a JS object:

```typescript
import { unmarshallItem } from "@fgiova/mini-dynamo-mapper";

const item = unmarshallItem(schema, {
  pk: { S: "USER#1" },
  name: { S: "Alice" },
  age: { N: "30" },
});
// { pk: "USER#1", name: "Alice", age: 30 }
```

For `Document` types with a `valueConstructor`, the unmarshalled object is instantiated via that constructor.

## unmarshallValue

Unmarshall a single `AttributeValue`:

```typescript
import { unmarshallValue } from "@fgiova/mini-dynamo-mapper";

unmarshallValue({ type: "String" }, { S: "hello" });  // "hello"
unmarshallValue({ type: "Number" }, { N: "42" });       // 42
unmarshallValue({ type: "Date" }, { N: "1700000000" }); // Date object
unmarshallValue({ type: "Set", memberType: "Number" }, { NS: ["1", "2"] }); // Set {1, 2}
```

## Type mapping reference

| Schema Type | JS Type (input) | DynamoDB Type | JS Type (output) |
|-------------|-----------------|---------------|-------------------|
| `String` | `string` | `S` | `string` |
| `Number` | `number` | `N` | `number` |
| `Boolean` | `boolean` | `BOOL` | `boolean` |
| `Binary` | `Uint8Array / Buffer` | `B` (base64) | `Uint8Array` |
| `Date` | `Date` | `N` (unix seconds) | `Date` |
| `Null` | `null` | `NULL` | `null` |
| `List` | `T[]` | `L` | `T[]` |
| `Map` | `Map<string, T>` | `M` | `Map<string, T>` |
| `Set` (String) | `Set<string>` | `SS` | `Set<string>` |
| `Set` (Number) | `Set<number>` | `NS` | `Set<number>` |
| `Set` (Binary) | `Set<Uint8Array>` | `BS` | `Set<Uint8Array>` |
| `Document` | `object` | `M` | `object` |
| `Tuple` | `[T1, T2, ...]` | `L` | `[T1, T2, ...]` |
| `Collection` | `any[]` | `L` | `any[]` |
| `Hash` | `Record<string, any>` | `M` | `Record<string, any>` |
| `Any` | any | auto-detected | auto-detected |
| `Custom` | `T` | user-defined | `T` |
