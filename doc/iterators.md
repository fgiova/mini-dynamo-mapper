# Iterators

Query and scan operations return async iterators that handle pagination automatically. You can iterate item by item or page by page.

## Query

Query items by key condition, with optional filtering:

```typescript
import { equals, beginsWith, and, greaterThan } from "@fgiova/mini-dynamo-mapper";

// Iterate items one by one
for await (const item of mapper.query(UserModel, equals("pk", "USER#1"))) {
  console.log(item);
}

// Composite key condition
const keyCondition = and(
  equals("pk", "TENANT#1"),
  beginsWith("sk", "ORDER#")
);

for await (const order of mapper.query(OrderModel, keyCondition)) {
  console.log(order);
}
```

### QueryOptions

| Option | Type | Description |
|--------|------|-------------|
| `filter` | `ConditionExpression` | Filter applied after key condition |
| `projection` | `ProjectionExpression` | Attributes to return |
| `indexName` | `string` | Query a secondary index |
| `scanIndexForward` | `boolean` | `true` = ascending (default), `false` = descending |
| `consistentRead` | `boolean` | Override default consistency |
| `limit` | `number` | Max items per page (DynamoDB Limit) |
| `startKey` | `Record<string, any>` | Resume from a specific key |
| `select` | `string` | Attribute selection mode |
| `returnConsumedCapacity` | `"INDEXES" \| "TOTAL" \| "NONE"` | Track capacity usage |

```typescript
import { projection, greaterThan } from "@fgiova/mini-dynamo-mapper";

const iterator = mapper.query(
  UserModel,
  equals("pk", "TENANT#1"),
  {
    filter: greaterThan("age", 18),
    projection: projection("pk", "sk", "name", "age"),
    indexName: "byAge",
    scanIndexForward: false, // descending
    limit: 50,
  }
);

for await (const item of iterator) {
  console.log(item);
}
```

## Scan

Full table scan with optional filtering:

```typescript
// Simple scan
for await (const item of mapper.scan(UserModel)) {
  console.log(item);
}

// Scan with filter
for await (const item of mapper.scan(UserModel, {
  filter: equals("status", "active"),
  limit: 100,
})) {
  console.log(item);
}
```

### ScanOptions

| Option | Type | Description |
|--------|------|-------------|
| `filter` | `ConditionExpression` | Filter expression |
| `projection` | `ProjectionExpression` | Attributes to return |
| `indexName` | `string` | Scan a secondary index |
| `consistentRead` | `boolean` | Override default consistency |
| `limit` | `number` | Max items per page |
| `startKey` | `Record<string, any>` | Resume from a specific key |
| `segment` | `number` | Segment number (for manual parallel scan) |
| `totalSegments` | `number` | Total segments (for manual parallel scan) |
| `select` | `string` | Attribute selection mode |
| `returnConsumedCapacity` | `"INDEXES" \| "TOTAL" \| "NONE"` | Track capacity usage |

## Parallel Scan

Scan across multiple segments in parallel for higher throughput on large tables:

```typescript
const iterator = mapper.parallelScan(UserModel, 4); // 4 segments

for await (const item of iterator) {
  console.log(item);
}
```

Items are fetched in round-robin across all segments. The `ParallelScanOptions` are the same as `ScanOptions` without `segment`/`totalSegments`.

## Page-based iteration

Instead of iterating item by item, you can iterate page by page to access metadata like `lastEvaluatedKey`, `count`, and `consumedCapacity`:

```typescript
const queryIterator = mapper.query(UserModel, equals("pk", "USER#1"));

for await (const page of queryIterator.pages()) {
  console.log(`Items in page: ${page.count}`);
  console.log(`Scanned: ${page.scannedCount}`);
  console.log(`Last key: ${page.lastEvaluatedKey}`);

  for (const item of page.items) {
    console.log(item);
  }
}
```

### PageResult\<T\>

| Field | Type | Description |
|-------|------|-------------|
| `items` | `T[]` | Items in the page |
| `lastEvaluatedKey` | `Record<string, AttributeValue>?` | Key for the next page (`undefined` if last page) |
| `count` | `number` | Number of items returned |
| `scannedCount` | `number` | Number of items evaluated before filtering |
| `consumedCapacity` | `any?` | Consumed capacity (if requested) |

## Aggregate counters

After iteration completes, access aggregate counters on the iterator:

```typescript
const iterator = mapper.query(UserModel, equals("pk", "USER#1"));

for await (const item of iterator) {
  // process items
}

console.log(`Total items: ${iterator.count}`);
console.log(`Total scanned: ${iterator.scannedCount}`);
```
