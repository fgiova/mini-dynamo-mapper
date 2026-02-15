# Expressions

Expression builders provide a fluent API for DynamoDB condition, update, and projection expressions. All builders return typed objects that are serialized automatically by the `DataMapper`.

## Condition expressions

Condition expressions are used in `put`, `delete`, `update`, queries, and transactions.

### Comparison operators

```typescript
import {
  equals,
  notEquals,
  lessThan,
  lessThanOrEqual,
  greaterThan,
  greaterThanOrEqual,
} from "@fgiova/mini-dynamo-mapper";

equals("status", "active")           // status = :val
notEquals("status", "deleted")       // status <> :val
lessThan("age", 18)                  // age < :val
lessThanOrEqual("age", 65)           // age <= :val
greaterThan("score", 100)            // score > :val
greaterThanOrEqual("score", 0)       // score >= :val
```

### Range operators

```typescript
import { between, inList } from "@fgiova/mini-dynamo-mapper";

between("age", 18, 65)              // age BETWEEN :lo AND :hi
inList("status", ["active", "pending"]) // status IN (:v0, :v1)
```

### Attribute functions

```typescript
import {
  attributeExists,
  attributeNotExists,
  attributeType,
  beginsWith,
  contains,
} from "@fgiova/mini-dynamo-mapper";

attributeExists("email")            // attribute_exists(email)
attributeNotExists("deletedAt")     // attribute_not_exists(deletedAt)
attributeType("data", "M")          // attribute_type(data, :val)
beginsWith("sk", "ORDER#")          // begins_with(sk, :val)
contains("tags", "urgent")          // contains(tags, :val)
```

### Logical operators

Combine conditions with `and`, `or`, and `not`:

```typescript
import { and, or, not, equals, greaterThan } from "@fgiova/mini-dynamo-mapper";

// (status = "active") AND (age > 18)
and(equals("status", "active"), greaterThan("age", 18))

// (role = "admin") OR (role = "superadmin")
or(equals("role", "admin"), equals("role", "superadmin"))

// NOT (status = "deleted")
not(equals("status", "deleted"))

// Complex nested
and(
  equals("status", "active"),
  or(
    greaterThan("age", 18),
    equals("parentConsent", true)
  )
)
```

### Nested attribute paths

Use dot notation and bracket notation for nested attributes:

```typescript
equals("address.city", "Rome")
equals("tags[0]", "primary")
equals("metadata.nested.deep", "value")
```

## Update expressions

Update expressions are used with `mapper.update()` and transactions.

### Set actions

```typescript
import {
  set,
  increment,
  decrement,
  ifNotExists,
  listAppend,
  listPrepend,
  updateExpression,
} from "@fgiova/mini-dynamo-mapper";

// SET name = :val
set("name", "Alice")

// SET age = age + 1
increment("age")       // default increment by 1
increment("score", 10) // increment by 10

// SET age = age - 1
decrement("age")
decrement("score", 5)

// SET visits = if_not_exists(visits, :zero)
// (use with set() for: SET visits = if_not_exists(visits, 0))
set("visits", ifNotExists("visits", 0))

// SET tags = list_append(tags, :newItems)
set("tags", listAppend("tags", ["new-tag"]))

// SET tags = list_append(:newItems, tags)  (prepend)
set("tags", listPrepend("tags", ["first-tag"]))
```

### Remove actions

```typescript
import { remove } from "@fgiova/mini-dynamo-mapper";

// REMOVE tempField
remove("tempField")
```

### Add actions

Add a number to a numeric attribute, or add elements to a set:

```typescript
import { add } from "@fgiova/mini-dynamo-mapper";

// ADD counter :val
add("counter", 5)

// ADD tags :val (where val is a set)
add("tags", new Set(["tag1", "tag2"]))
```

### Delete from set

Remove elements from a set:

```typescript
import { deleteFromSet } from "@fgiova/mini-dynamo-mapper";

// DELETE tags :val
deleteFromSet("tags", new Set(["old-tag"]))
```

### Combining update actions

Use `updateExpression()` to combine multiple actions:

```typescript
import { updateExpression, set, remove, increment } from "@fgiova/mini-dynamo-mapper";

const updates = updateExpression(
  set("name", "Bob"),
  set("email", "bob@example.com"),
  increment("loginCount"),
  remove("tempToken")
);

await mapper.update(UserModel, key, updates);
```

## Projection expressions

Select which attributes to return:

```typescript
import { projection } from "@fgiova/mini-dynamo-mapper";

// Return only name and age
await mapper.get(UserModel, key, {
  projection: projection("name", "age"),
});

// Works with query/scan too
mapper.query(UserModel, keyCondition, {
  projection: projection("pk", "sk", "name"),
});
```
