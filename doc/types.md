# Types

The schema type system defines how each attribute is stored in DynamoDB and inferred in TypeScript.

## Keyable types

These types can be used as table or index keys (`keyType`, `indexKeyConfigurations`):

### String

```typescript
{ type: "String" }
{ type: "String", keyType: "HASH" }
{ type: "String", keyType: "RANGE", attributeName: "SK" }
```

Stored as DynamoDB `S`. Inferred as `string`.

### Number

```typescript
{ type: "Number" }
{ type: "Number", keyType: "RANGE" }
{ type: "Number", versionAttribute: true }
```

Stored as DynamoDB `N`. Inferred as `number`.

The `versionAttribute` option enables automatic optimistic locking (see [DataMapper](mapper.md)).

### Binary

```typescript
{ type: "Binary" }
```

Stored as DynamoDB `B` (base64-encoded). Accepts `Uint8Array` or `Buffer`. Inferred as `Uint8Array`.

### Date

```typescript
{ type: "Date" }
{ type: "Date", keyType: "RANGE" }
```

Stored as DynamoDB `N` (Unix seconds). Accepts and returns `Date` objects. Inferred as `Date`.

## Non-keyable types

### Boolean

```typescript
{ type: "Boolean" }
```

Stored as DynamoDB `BOOL`. Inferred as `boolean`.

### Null

```typescript
{ type: "Null" }
```

Stored as DynamoDB `NULL`. Inferred as `null`.

## Collection types

### List

Typed array where each element follows the same schema:

```typescript
{
  type: "List",
  memberType: { type: "String" }
}
```

Stored as DynamoDB `L`. Inferred as `T[]` where `T` is the member type.

```typescript
// List of documents
{
  type: "List",
  memberType: {
    type: "Document",
    members: {
      street: { type: "String" },
      city: { type: "String" },
    }
  }
}
```

### Map

A `Map<string, T>` where all values follow the same schema:

```typescript
{
  type: "Map",
  memberType: { type: "Number" }
}
```

Stored as DynamoDB `M`. Inferred as `Map<string, T>`.

### Set

A `Set` of homogeneous values:

```typescript
{ type: "Set", memberType: "String" }  // Set<string>  → SS
{ type: "Set", memberType: "Number" }  // Set<number>  → NS
{ type: "Set", memberType: "Binary" }  // Set<Uint8Array> → BS
```

Empty sets are stored as `NULL`.

### Tuple

Fixed-length array with typed positions:

```typescript
{
  type: "Tuple",
  members: [
    { type: "String" },
    { type: "Number" },
    { type: "Boolean" },
  ]
}
```

Stored as DynamoDB `L`. Inferred as `[string, number, boolean]`.

## Flexible types

These types use auto-detection for marshalling/unmarshalling.

### Collection

An untyped array (each element is auto-marshalled):

```typescript
{
  type: "Collection",
  onEmpty: "nullify",  // or "leave"
  onInvalid: "omit",   // or "throw"
}
```

Stored as DynamoDB `L`. Inferred as `any[]`.

### Hash

An untyped object (each value is auto-marshalled):

```typescript
{
  type: "Hash",
  onEmpty: "nullify",
  onInvalid: "omit",
}
```

Stored as DynamoDB `M`. Inferred as `Record<string, any>`.

### Any

A single value of any type (auto-detected):

```typescript
{
  type: "Any",
  onEmpty: "nullify",
  onInvalid: "omit",
  unwrapNumbers: true,
}
```

Stored as auto-detected DynamoDB type. Inferred as `any`.

#### Options for flexible types

| Option | Values | Description |
|--------|--------|-------------|
| `onEmpty` | `"nullify" \| "leave"` | Behavior when value is empty |
| `onInvalid` | `"throw" \| "omit"` | Behavior when value is invalid |
| `unwrapNumbers` | `boolean` | (Any only) Whether to unwrap number strings |

## Custom type

User-defined marshalling and unmarshalling:

```typescript
import type { AttributeValue } from "@fgiova/mini-dynamo-client";

{
  type: "Custom",
  marshall: (value: MyType): AttributeValue => {
    return { S: JSON.stringify(value) };
  },
  unmarshall: (attr: AttributeValue): MyType => {
    return JSON.parse(attr.S!);
  },
  attributeType: "S",  // optional hint
}
```

Inferred as `any` (use `InferSchema` with explicit types for better inference).

## Document type

A nested object with its own schema:

```typescript
{
  type: "Document",
  members: {
    street: { type: "String" },
    city: { type: "String" },
    zip: { type: "String" },
  },
  valueConstructor: Address,  // optional
}
```

Stored as DynamoDB `M`. Inferred as the shape of `members`.

When `valueConstructor` is provided, the unmarshalled object is created with `new valueConstructor()` and fields are assigned via `Object.assign`.

## Common options

All schema types support these base options:

| Option | Type | Description |
|--------|------|-------------|
| `attributeName` | `string` | Override DynamoDB attribute name |
| `defaultProvider` | `() => any` | Provide default value on marshall when `undefined` |

Keyable types additionally support:

| Option | Type | Description |
|--------|------|-------------|
| `keyType` | `"HASH" \| "RANGE"` | Mark as table key |
| `indexKeyConfigurations` | `Record<string, "HASH" \| "RANGE">` | Mark as index key |

## TypeScript inference

The library infers TypeScript types from schema definitions:

```typescript
import type { InferSchema, InferKey, InferTuple } from "@fgiova/mini-dynamo-mapper";

const schema = {
  pk: { type: "String" as const, keyType: "HASH" as const },
  sk: { type: "String" as const, keyType: "RANGE" as const },
  name: { type: "String" as const },
  age: { type: "Number" as const },
  active: { type: "Boolean" as const },
  tags: { type: "Set" as const, memberType: "String" as const },
  createdAt: { type: "Date" as const },
};

type Item = InferSchema<typeof schema>;
// {
//   pk: string;
//   sk: string;
//   name: string;
//   age: number;
//   active: boolean;
//   tags: Set<string>;
//   createdAt: Date;
// }

type Key = InferKey<typeof schema>;
// { pk: string; sk: string }
```

When using `defineModel`, the types are inferred automatically without needing `as const`.
