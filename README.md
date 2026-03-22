# @fgiova/mini-dynamo-mapper

A type-safe DynamoDB mapper for Node.js built on top of [@fgiova/mini-dynamo-client](https://github.com/fgiova/mini-dynamo-client).

[![NPM version](https://img.shields.io/npm/v/@fgiova/mini-dynamo-mapper.svg?style=flat)](https://www.npmjs.com/package/@fgiova/mini-dynamo-mapper)
![CI workflow](https://github.com/fgiova/mini-dynamo-mapper/actions/workflows/node.js.yml/badge.svg)
[![TypeScript](https://img.shields.io/badge/%3C%2F%3E-TypeScript-%230074c1.svg)](http://www.typescriptlang.org/)
[![Linted with Biome](https://img.shields.io/badge/Linted_with-Biome-60a5fa?style=flat&logo=biome)](https://biomejs.dev)
[![Maintainability](https://qlty.sh/gh/fgiova/projects/mini-dynamo-mapper/maintainability.svg)](https://qlty.sh/gh/fgiova/projects/mini-dynamo-mapper)
[![Code Coverage](https://qlty.sh/gh/fgiova/projects/mini-dynamo-mapper/coverage.svg)](https://qlty.sh/gh/fgiova/projects/mini-dynamo-mapper)

Define your table schema once and get full TypeScript inference for all operations: CRUD, queries, scans, batch, and transactions.

## Installation

```bash
npm install @fgiova/mini-dynamo-mapper @fgiova/mini-dynamo-client
```

**Requires** Node.js `^22.14.0 || >= 24.10.0`

## Quick Start

```typescript
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import {
  defineModel,
  DataMapper,
  equals,
  set,
  updateExpression,
} from "@fgiova/mini-dynamo-mapper";

// 1. Define your model
const UserModel = defineModel({
  tableName: "Users",
  schema: {
    pk: { type: "String", keyType: "HASH" },
    sk: { type: "String", keyType: "RANGE" },
    name: { type: "String" },
    age: { type: "Number" },
    version: { type: "Number", versionAttribute: true },
  },
  indexes: {
    byAge: { type: "global", hashKey: "age" },
  },
});

// 2. Create the mapper
const client = new MiniDynamoClient({ region: "eu-west-1" });
const mapper = new DataMapper({ client });

// 3. Put an item
const user = await mapper.put(UserModel, {
  pk: "USER#1",
  sk: "PROFILE",
  name: "Alice",
  age: 30,
  version: 0,
});

// 4. Get an item
const result = await mapper.get(UserModel, { pk: "USER#1", sk: "PROFILE" });

// 5. Update an item
const updated = await mapper.update(
  UserModel,
  { pk: "USER#1", sk: "PROFILE" },
  updateExpression(set("name", "Bob"), set("age", 31))
);

// 6. Query items
for await (const item of mapper.query(UserModel, equals("pk", "USER#1"))) {
  console.log(item.name);
}

// 7. Delete an item
await mapper.delete(UserModel, { pk: "USER#1", sk: "PROFILE" });
```

## Features

- **Type-safe schema** - Define your schema once, get full TypeScript inference on all operations
- **Optimistic locking** - Built-in version attribute support with automatic condition checks
- **Expression builders** - Fluent API for conditions, updates, and projections
- **Async iterators** - Paginated query/scan results via `for await...of`
- **Batch operations** - Batch get/put/delete with automatic retry and exponential backoff
- **Transactions** - Transactional read and write across multiple tables
- **Parallel scan** - Multi-segment scan for large tables
- **Attribute mapping** - Custom DynamoDB attribute names via `attributeName`
- **Custom types** - User-defined marshall/unmarshall functions
- **Dual format** - ESM and CommonJS support

## Documentation

Detailed documentation is available per functional module:

| Module | Description |
|--------|-------------|
| [Model & Schema](doc/model.md) | Define tables, schemas, indexes, and type inference |
| [DataMapper](doc/mapper.md) | CRUD operations: put, get, update, delete |
| [Expressions](doc/expressions.md) | Condition, update, and projection expression builders |
| [Iterators](doc/iterators.md) | Query, Scan, and ParallelScan with async iteration |
| [Batch & Transactions](doc/batch-transactions.md) | Batch get/put/delete and transactional operations |
| [Marshaller](doc/marshaller.md) | Data serialization between JS objects and DynamoDB format |
| [Types](doc/types.md) | Schema type system and TypeScript type inference |

## License

MIT - Francesco Giovannini
