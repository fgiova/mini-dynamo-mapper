# Batch & Transactions

## Batch operations

Batch methods return `AsyncIterableIterator` and handle unprocessed items with automatic retry and exponential backoff (up to 10 retries, max 30s delay).

### batchGet

Retrieve multiple items by key:

```typescript
const keys = [
  { pk: "USER#1", sk: "PROFILE" },
  { pk: "USER#2", sk: "PROFILE" },
  { pk: "USER#3", sk: "PROFILE" },
];

for await (const user of mapper.batchGet(UserModel, keys)) {
  console.log(user.name);
}
```

#### BatchGetOptions

| Option | Type | Description |
|--------|------|-------------|
| `consistentRead` | `boolean` | Override default consistency |
| `projection` | `ProjectionExpression` | Attributes to return |
| `returnConsumedCapacity` | `"INDEXES" \| "TOTAL" \| "NONE"` | Track capacity |

### batchPut

Insert multiple items. Version attributes are automatically incremented.

```typescript
const users = [
  { pk: "USER#1", sk: "PROFILE", name: "Alice", version: 0 },
  { pk: "USER#2", sk: "PROFILE", name: "Bob", version: 0 },
];

for await (const user of mapper.batchPut(UserModel, users)) {
  console.log(`Inserted: ${user.name}, version: ${user.version}`);
}
```

### batchDelete

Delete multiple items by key:

```typescript
const keys = [
  { pk: "USER#1", sk: "PROFILE" },
  { pk: "USER#2", sk: "PROFILE" },
];

for await (const key of mapper.batchDelete(UserModel, keys)) {
  console.log(`Deleted: ${key.pk}`);
}
```

### batchWrite

Mixed put and delete operations on a single table:

```typescript
import type { BatchWriteOperation } from "@fgiova/mini-dynamo-mapper";

const operations: BatchWriteOperation<typeof UserModel.schema>[] = [
  { type: "put", item: { pk: "USER#3", sk: "PROFILE", name: "Charlie", version: 0 } },
  { type: "delete", key: { pk: "USER#1", sk: "PROFILE" } },
];

for await (const op of mapper.batchWrite(UserModel, operations)) {
  console.log(op.type);
}
```

#### BatchWriteOptions

| Option | Type | Description |
|--------|------|-------------|
| `returnConsumedCapacity` | `"INDEXES" \| "TOTAL" \| "NONE"` | Track capacity |
| `returnItemCollectionMetrics` | `"SIZE" \| "NONE"` | Collection metrics |

### Retry behavior

Batch writes automatically retry unprocessed items with exponential backoff:

- Max retries: **10**
- Delay formula: `min(2^attempt * 100, 30000)` ms
- Unprocessed items from `BatchGetItem` and `BatchWriteItem` are collected and retried

## Transactions

### transactGet

Read multiple items atomically across tables:

```typescript
const results = await mapper.transactGet([
  { model: UserModel, key: { pk: "USER#1", sk: "PROFILE" } },
  { model: OrderModel, key: { pk: "ORDER#100", sk: "DETAIL" } },
]);

const user = results[0];  // typed as InferModel<typeof UserModel>
const order = results[1];  // typed as InferModel<typeof OrderModel>
```

#### TransactGetItem

| Field | Type | Description |
|-------|------|-------------|
| `model` | `ModelDefinition` | The model definition |
| `key` | `InferModelKey<M>` | Item key |
| `projection` | `ProjectionExpression?` | Attributes to return |

### transactWrite

Write multiple items atomically. Supports `Put`, `Delete`, `Update`, and `ConditionCheck`:

```typescript
import { equals, set, updateExpression } from "@fgiova/mini-dynamo-mapper";

await mapper.transactWrite([
  {
    type: "Put",
    model: UserModel,
    item: { pk: "USER#4", sk: "PROFILE", name: "Diana", version: 1 },
  },
  {
    type: "Update",
    model: UserModel,
    key: { pk: "USER#1", sk: "PROFILE" },
    updates: updateExpression(set("status", "active")),
  },
  {
    type: "Delete",
    model: OrderModel,
    key: { pk: "ORDER#100", sk: "DETAIL" },
    condition: equals("status", "cancelled"),
  },
  {
    type: "ConditionCheck",
    model: UserModel,
    key: { pk: "USER#1", sk: "PROFILE" },
    condition: equals("status", "active"),
  },
]);
```

#### Transaction item types

**TransactPutItem**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"Put"` | Operation type |
| `model` | `ModelDefinition` | Model definition |
| `item` | `InferModel<M>` | Full item to put |
| `condition` | `ConditionExpression?` | Optional condition |

**TransactDeleteItem**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"Delete"` | Operation type |
| `model` | `ModelDefinition` | Model definition |
| `key` | `InferModelKey<M>` | Item key |
| `condition` | `ConditionExpression?` | Optional condition |

**TransactUpdateItem**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"Update"` | Operation type |
| `model` | `ModelDefinition` | Model definition |
| `key` | `InferModelKey<M>` | Item key |
| `updates` | `UpdateExpression` | Update actions |
| `condition` | `ConditionExpression?` | Optional condition |

**TransactConditionCheck**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"ConditionCheck"` | Operation type |
| `model` | `ModelDefinition` | Model definition |
| `key` | `InferModelKey<M>` | Item key |
| `condition` | `ConditionExpression` | Required condition |

#### TransactWriteOptions

| Option | Type | Description |
|--------|------|-------------|
| `clientRequestToken` | `string` | Idempotency token |
| `returnConsumedCapacity` | `"INDEXES" \| "TOTAL" \| "NONE"` | Track capacity |
| `returnItemCollectionMetrics` | `"SIZE" \| "NONE"` | Collection metrics |
