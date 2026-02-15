# DataMapper

The `DataMapper` class is the main entry point for all DynamoDB operations. It provides type-safe CRUD, query, scan, batch, and transaction methods.

## Configuration

```typescript
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { DataMapper } from "@fgiova/mini-dynamo-mapper";

const client = new MiniDynamoClient({ region: "eu-west-1" });

const mapper = new DataMapper({
  client,
  readConsistency: "eventual",   // default: "eventual"
  skipVersionCheck: false,        // default: false
  tableNamePrefix: "prod-",      // default: ""
});
```

### DataMapperConfiguration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `MiniDynamoClient` | required | The DynamoDB client instance |
| `readConsistency` | `"eventual" \| "strong"` | `"eventual"` | Default read consistency for get/query/scan |
| `skipVersionCheck` | `boolean` | `false` | Disable optimistic locking globally |
| `tableNamePrefix` | `string` | `""` | Prefix prepended to all table names |

## put

Insert or replace an item. If the schema has a `versionAttribute`, the version is automatically incremented and a condition is added to prevent overwriting newer versions.

```typescript
const user = await mapper.put(UserModel, {
  pk: "USER#1",
  sk: "PROFILE",
  name: "Alice",
  age: 30,
  version: 0,
});
// user.version === 1 (auto-incremented)
```

### PutOptions

| Option | Type | Description |
|--------|------|-------------|
| `condition` | `ConditionExpression` | Additional condition expression |
| `returnValues` | `"NONE" \| "ALL_OLD"` | What to return |
| `skipVersionCheck` | `boolean` | Override global version check setting |

```typescript
import { attributeNotExists } from "@fgiova/mini-dynamo-mapper";

// Only insert if the item doesn't exist
await mapper.put(UserModel, item, {
  condition: attributeNotExists("pk"),
});
```

## get

Retrieve a single item by its key. Returns `undefined` if the item does not exist.

```typescript
const user = await mapper.get(UserModel, { pk: "USER#1", sk: "PROFILE" });

if (user) {
  console.log(user.name); // fully typed
}
```

### GetOptions

| Option | Type | Description |
|--------|------|-------------|
| `projection` | `ProjectionExpression` | Attributes to return |
| `consistentRead` | `boolean` | Override default read consistency |

```typescript
import { projection } from "@fgiova/mini-dynamo-mapper";

const user = await mapper.get(UserModel, key, {
  projection: projection("name", "age"),
  consistentRead: true,
});
```

## update

Update specific attributes of an existing item. The version attribute is automatically incremented.

```typescript
import { set, remove, increment, updateExpression } from "@fgiova/mini-dynamo-mapper";

const updated = await mapper.update(
  UserModel,
  { pk: "USER#1", sk: "PROFILE" },
  updateExpression(
    set("name", "Bob"),
    increment("age"),
    remove("tempField")
  )
);
```

Returns the updated item (default `ALL_NEW`).

### UpdateOptions

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `condition` | `ConditionExpression` | - | Additional condition |
| `returnValues` | `"NONE" \| "ALL_OLD" \| "UPDATED_OLD" \| "ALL_NEW" \| "UPDATED_NEW"` | `"ALL_NEW"` | What to return |
| `skipVersionCheck` | `boolean` | - | Override version check |
| `onMissing` | `"remove" \| "skip"` | - | How to handle missing attributes |

## delete

Delete an item by key. Returns the deleted item by default (`ALL_OLD`).

```typescript
const deleted = await mapper.delete(UserModel, {
  pk: "USER#1",
  sk: "PROFILE",
});
```

### DeleteOptions

| Option | Type | Description |
|--------|------|-------------|
| `condition` | `ConditionExpression` | Additional condition |
| `returnValues` | `"NONE" \| "ALL_OLD"` | What to return |
| `skipVersionCheck` | `boolean` | Override version check |

```typescript
import { equals } from "@fgiova/mini-dynamo-mapper";

// Only delete if status is "inactive"
await mapper.delete(UserModel, key, {
  condition: equals("status", "inactive"),
});
```

## Optimistic locking

When a schema contains a `Number` attribute with `versionAttribute: true`, the mapper automatically:

1. **On put** - Increments the version and adds a condition ensuring the current version matches
2. **On update** - Adds a `SET version = version + 1` action and a version condition
3. **On delete** - Adds a version condition to ensure you're deleting the expected version

```typescript
const schema = {
  pk: { type: "String", keyType: "HASH" },
  version: { type: "Number", versionAttribute: true },
};
```

If a concurrent write has changed the version, the operation fails with a `ConditionalCheckFailedException`.

You can disable version checking per-operation:

```typescript
await mapper.put(UserModel, item, { skipVersionCheck: true });
```

Or globally:

```typescript
const mapper = new DataMapper({ client, skipVersionCheck: true });
```
