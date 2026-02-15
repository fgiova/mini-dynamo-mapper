# Model & Schema

A model binds a DynamoDB table name, a schema definition, and optional secondary indexes into a single typed object.

## defineModel

```typescript
import { defineModel } from "@fgiova/mini-dynamo-mapper";

const UserModel = defineModel({
  tableName: "Users",
  schema: {
    pk: { type: "String", keyType: "HASH" },
    sk: { type: "String", keyType: "RANGE" },
    name: { type: "String" },
    email: { type: "String" },
    age: { type: "Number" },
    active: { type: "Boolean" },
    version: { type: "Number", versionAttribute: true },
  },
  indexes: {
    byEmail: {
      type: "global",
      hashKey: "email",
    },
    byAge: {
      type: "local",
      hashKey: "pk",
      rangeKey: "age",
    },
  },
});
```

### ModelDefinition

| Field | Type | Description |
|-------|------|-------------|
| `tableName` | `string` | DynamoDB table name |
| `schema` | `Schema` | Attribute definitions (see [Types](types.md)) |
| `indexes` | `Record<string, IndexDefinition>` | Optional secondary indexes |

### IndexDefinition

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"global" \| "local"` | GSI or LSI |
| `hashKey` | `string` | Hash key attribute name |
| `rangeKey` | `string?` | Optional range key attribute name |

## Schema attributes

Each attribute in the schema is a `SchemaType` with common options:

| Option | Type | Description |
|--------|------|-------------|
| `type` | `string` | Required. The data type (see [Types](types.md)) |
| `attributeName` | `string?` | Override the DynamoDB attribute name |
| `keyType` | `"HASH" \| "RANGE"` | Mark as table key (only keyable types) |
| `indexKeyConfigurations` | `Record<string, "HASH" \| "RANGE">` | Mark as key in secondary indexes |
| `defaultProvider` | `() => any` | Function that returns a default value |
| `versionAttribute` | `boolean` | Mark as version field for optimistic locking (Number only) |

### Attribute name mapping

Map a JS field to a different DynamoDB attribute name:

```typescript
const schema = {
  id: { type: "String", keyType: "HASH", attributeName: "PK" },
  sortKey: { type: "String", keyType: "RANGE", attributeName: "SK" },
};
```

In DynamoDB the attributes will be stored as `PK` and `SK`, but in your code you reference them as `id` and `sortKey`.

### Default values

```typescript
const schema = {
  id: { type: "String", keyType: "HASH" },
  createdAt: {
    type: "Date",
    defaultProvider: () => new Date(),
  },
  status: {
    type: "String",
    defaultProvider: () => "pending",
  },
};
```

The `defaultProvider` is called during `marshallItem` when the field value is `undefined`.

## Type inference

Extract TypeScript types from a model definition:

```typescript
import type { InferModel, InferModelKey } from "@fgiova/mini-dynamo-mapper";

// Full item type
type User = InferModel<typeof UserModel>;
// { pk: string; sk: string; name: string; email: string; age: number; active: boolean; version: number }

// Key-only type (HASH + RANGE fields)
type UserKey = InferModelKey<typeof UserModel>;
// { pk: string; sk: string }
```

You can also use the lower-level schema inference utilities:

```typescript
import type { InferSchema, InferKey } from "@fgiova/mini-dynamo-mapper";

type Item = InferSchema<typeof UserModel.schema>;
type Key = InferKey<typeof UserModel.schema>;
```
