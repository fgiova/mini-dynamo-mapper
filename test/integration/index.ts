import "../helpers/localtest";
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { before, teardown, test } from "tap";
import {
	DataMapper,
	defineModel,
	equals,
	set,
	updateExpression,
} from "../../src";
import { cleanTable } from "../helpers/dynamoTable";

const endpoint = process.env.LOCALSTACK_ENDPOINT as string;

const UserModel = defineModel({
	tableName: "TestTable",
	schema: {
		pk: { type: "String", keyType: "HASH" },
		sk: { type: "String", keyType: "RANGE" },
		name: { type: "String" },
		age: { type: "Number" },
		email: { type: "String" },
		tags: { type: "Set", memberType: "String" },
		metadata: {
			type: "Document",
			members: {
				key: { type: "String" },
				value: { type: "String" },
			},
		},
		version: { type: "Number", versionAttribute: true },
	},
} as const);

let client: MiniDynamoClient;
let mapper: DataMapper;

before(async () => {
	client = new MiniDynamoClient("eu-central-1", endpoint);
	mapper = new DataMapper({ client });
});

teardown(async () => {
	await client?.destroy();
});

test("CRUD lifecycle", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// Put
	const putResult = await mapper.put(UserModel, {
		pk: "USER#1",
		sk: "PROFILE",
		name: "John",
		age: 30,
		email: "john@example.com",
		tags: new Set(["admin", "active"]),
		metadata: { key: "role", value: "admin" },
	} as any);

	t.equal(putResult.pk, "USER#1");
	t.equal(putResult.version, 0);

	// Get
	const getResult = await mapper.get(UserModel, {
		pk: "USER#1",
		sk: "PROFILE",
	} as any);
	t.ok(getResult);
	t.equal(getResult!.name, "John");
	t.equal(getResult!.age, 30);
	t.equal(getResult!.version, 0);
	t.same([...getResult!.tags].sort(), ["active", "admin"]);
	t.same(getResult!.metadata, { key: "role", value: "admin" });

	// Update
	const updateResult = await mapper.update(
		UserModel,
		{ pk: "USER#1", sk: "PROFILE" } as any,
		updateExpression(set("name", "Jane"), set("age", 25)),
		{ skipVersionCheck: true },
	);
	t.equal(updateResult.name, "Jane");
	t.equal(updateResult.age, 25);

	// Get after update
	const getAfterUpdate = await mapper.get(UserModel, {
		pk: "USER#1",
		sk: "PROFILE",
	} as any);
	t.equal(getAfterUpdate!.name, "Jane");

	// Delete
	const deleteResult = await mapper.delete(
		UserModel,
		{ pk: "USER#1", sk: "PROFILE" } as any,
		{ skipVersionCheck: true },
	);
	t.ok(deleteResult);

	// Get after delete
	const getAfterDelete = await mapper.get(UserModel, {
		pk: "USER#1",
		sk: "PROFILE",
	} as any);
	t.equal(getAfterDelete, undefined);
});

test("version checking", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// First put - version 0
	const item1 = await mapper.put(UserModel, {
		pk: "USER#V",
		sk: "PROFILE",
		name: "John",
		age: 30,
	} as any);
	t.equal(item1.version, 0);

	// Second put - version 1
	const item2 = await mapper.put(UserModel, {
		...item1,
	} as any);
	t.equal(item2.version, 1);

	// Put with old version should fail
	try {
		await mapper.put(UserModel, {
			...item1,
		} as any);
		t.fail("Should have thrown");
	} catch (e: any) {
		t.ok(e.message);
	}
});

test("query", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// Insert items
	for (let i = 0; i < 5; i++) {
		await mapper.put(
			UserModel,
			{
				pk: "QUERY#1",
				sk: `ITEM#${String(i).padStart(3, "0")}`,
				name: `User${i}`,
				age: 20 + i,
			} as any,
			{ skipVersionCheck: true },
		);
	}

	// Query all
	const items: any[] = [];
	for await (const item of mapper.query(UserModel, equals("pk", "QUERY#1"))) {
		items.push(item);
	}
	t.equal(items.length, 5);

	// Query with limit and pages
	const pages: any[] = [];
	for await (const page of mapper
		.query(UserModel, equals("pk", "QUERY#1"), { limit: 2 })
		.pages()) {
		pages.push(page);
	}
	t.ok(pages.length >= 2);
});

test("scan", async (t) => {
	// Scan should find items from previous test
	const items: any[] = [];
	for await (const item of mapper.scan(UserModel)) {
		items.push(item);
	}
	t.ok(items.length >= 5);
});

test("batch operations", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// Batch put
	const putItems: any[] = [];
	for (let i = 0; i < 10; i++) {
		putItems.push({
			pk: "BATCH#1",
			sk: `ITEM#${String(i).padStart(3, "0")}`,
			name: `BatchUser${i}`,
			age: 20 + i,
		});
	}

	const putResults: any[] = [];
	for await (const item of mapper.batchPut(UserModel, putItems as any)) {
		putResults.push(item);
	}
	t.equal(putResults.length, 10);

	// Batch get
	const getKeys = putItems.map((i) => ({ pk: i.pk, sk: i.sk }));
	const getResults: any[] = [];
	for await (const item of mapper.batchGet(UserModel, getKeys as any)) {
		getResults.push(item);
	}
	t.equal(getResults.length, 10);

	// Batch delete
	const deleteResults: any[] = [];
	for await (const key of mapper.batchDelete(UserModel, getKeys as any)) {
		deleteResults.push(key);
	}
	t.equal(deleteResults.length, 10);
});

test("transactions", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// TransactWrite: put + condition check
	await mapper.transactWrite([
		{
			type: "Put",
			model: UserModel,
			item: {
				pk: "TX#1",
				sk: "PROFILE",
				name: "TxUser1",
				age: 30,
			} as any,
		},
		{
			type: "Put",
			model: UserModel,
			item: {
				pk: "TX#2",
				sk: "PROFILE",
				name: "TxUser2",
				age: 25,
			} as any,
		},
	]);

	// TransactGet
	const results = await mapper.transactGet([
		{ model: UserModel, key: { pk: "TX#1", sk: "PROFILE" } as any },
		{ model: UserModel, key: { pk: "TX#2", sk: "PROFILE" } as any },
	]);
	t.equal(results.length, 2);
	t.equal(results[0].name, "TxUser1");
	t.equal(results[1].name, "TxUser2");
});

test("parallel scan", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// Insert items for parallel scan
	for (let i = 0; i < 10; i++) {
		await mapper.put(
			UserModel,
			{
				pk: `PSCAN#${i}`,
				sk: "ITEM",
				name: `User${i}`,
				age: 20 + i,
			} as any,
			{ skipVersionCheck: true },
		);
	}

	const items: any[] = [];
	for await (const item of mapper.parallelScan(UserModel, 2)) {
		items.push(item);
	}
	t.equal(items.length, 10);
});

test("batchWrite mixed put and delete", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// First put some items
	for (let i = 0; i < 3; i++) {
		await mapper.put(
			UserModel,
			{
				pk: `BW#${i}`,
				sk: "ITEM",
				name: `User${i}`,
				age: 20 + i,
			} as any,
			{ skipVersionCheck: true },
		);
	}

	// batchWrite: put new + delete existing
	const ops: any[] = [];
	for await (const op of mapper.batchWrite(UserModel, [
		{
			type: "put",
			item: { pk: "BW#NEW1", sk: "ITEM", name: "New1", age: 30 } as any,
		},
		{
			type: "put",
			item: { pk: "BW#NEW2", sk: "ITEM", name: "New2", age: 31 } as any,
		},
		{ type: "delete", key: { pk: "BW#0", sk: "ITEM" } as any },
		{ type: "delete", key: { pk: "BW#1", sk: "ITEM" } as any },
	])) {
		ops.push(op);
	}
	t.equal(ops.length, 4);

	// Verify: BW#0 and BW#1 deleted, BW#2 still exists, NEW1 and NEW2 created
	const deleted0 = await mapper.get(UserModel, {
		pk: "BW#0",
		sk: "ITEM",
	} as any);
	t.equal(deleted0, undefined);

	const kept2 = await mapper.get(UserModel, { pk: "BW#2", sk: "ITEM" } as any);
	t.ok(kept2);

	const new1 = await mapper.get(UserModel, {
		pk: "BW#NEW1",
		sk: "ITEM",
	} as any);
	t.ok(new1);
	t.equal(new1!.name, "New1");
});

test("transactWrite with Update", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// First insert an item
	await mapper.put(
		UserModel,
		{
			pk: "TXU#1",
			sk: "PROFILE",
			name: "Original",
			age: 30,
		} as any,
		{ skipVersionCheck: true },
	);

	// TransactWrite with Update
	await mapper.transactWrite([
		{
			type: "Update",
			model: UserModel,
			key: { pk: "TXU#1", sk: "PROFILE" } as any,
			updates: updateExpression(set("name", "Updated"), set("age", 99)),
		},
	]);

	const result = await mapper.get(UserModel, {
		pk: "TXU#1",
		sk: "PROFILE",
	} as any);
	t.ok(result);
	t.equal(result!.name, "Updated");
	t.equal(result!.age, 99);
});

test("transactWrite with Delete + condition", async (t) => {
	await cleanTable(endpoint, "TestTable");

	// Insert item
	await mapper.put(
		UserModel,
		{
			pk: "TXD#1",
			sk: "PROFILE",
			name: "ToDelete",
			age: 40,
		} as any,
		{ skipVersionCheck: true },
	);

	// TransactWrite Delete with condition
	await mapper.transactWrite([
		{
			type: "Delete",
			model: UserModel,
			key: { pk: "TXD#1", sk: "PROFILE" } as any,
			condition: equals("name", "ToDelete"),
		},
	]);

	const result = await mapper.get(UserModel, {
		pk: "TXD#1",
		sk: "PROFILE",
	} as any);
	t.equal(result, undefined);
});

test("query with filter", async (t) => {
	await cleanTable(endpoint, "TestTable");

	for (let i = 0; i < 5; i++) {
		await mapper.put(
			UserModel,
			{
				pk: "QF#1",
				sk: `ITEM#${String(i).padStart(3, "0")}`,
				name: `User${i}`,
				age: 20 + i,
			} as any,
			{ skipVersionCheck: true },
		);
	}

	// Query with filter: age > 22
	const items: any[] = [];
	for await (const item of mapper.query(UserModel, equals("pk", "QF#1"), {
		filter: {
			type: "Simple",
			subject: "age",
			predicate: { type: "GreaterThan", value: 22 },
		},
	})) {
		items.push(item);
	}
	t.equal(items.length, 2); // age 23 and 24
});

test("scan with filter", async (t) => {
	// Uses items from previous test if table not cleaned
	await cleanTable(endpoint, "TestTable");

	for (let i = 0; i < 5; i++) {
		await mapper.put(
			UserModel,
			{
				pk: `SF#${i}`,
				sk: "ITEM",
				name: `User${i}`,
				age: 20 + i,
			} as any,
			{ skipVersionCheck: true },
		);
	}

	const items: any[] = [];
	for await (const item of mapper.scan(UserModel, {
		filter: {
			type: "Simple",
			subject: "age",
			predicate: { type: "GreaterThanOrEqual", value: 23 },
		},
	})) {
		items.push(item);
	}
	t.equal(items.length, 2); // age 23 and 24
});

test("get with projection", async (t) => {
	await cleanTable(endpoint, "TestTable");

	await mapper.put(
		UserModel,
		{
			pk: "PROJ#1",
			sk: "PROFILE",
			name: "John",
			age: 30,
			email: "john@example.com",
		} as any,
		{ skipVersionCheck: true },
	);

	const result = await mapper.get(
		UserModel,
		{ pk: "PROJ#1", sk: "PROFILE" } as any,
		{ projection: ["name", "age"] },
	);
	t.ok(result);
	t.equal(result!.name, "John");
	t.equal(result!.age, 30);
	// email should not be returned (or at least projection was applied)
});

test("transactWrite with ConditionCheck failure", async (t) => {
	await cleanTable(endpoint, "TestTable");

	await mapper.put(
		UserModel,
		{
			pk: "CC#1",
			sk: "PROFILE",
			name: "John",
			age: 30,
		} as any,
		{ skipVersionCheck: true },
	);

	try {
		await mapper.transactWrite([
			{
				type: "ConditionCheck",
				model: UserModel,
				key: { pk: "CC#1", sk: "PROFILE" } as any,
				condition: equals("name", "WRONG"),
			},
			{
				type: "Put",
				model: UserModel,
				item: {
					pk: "CC#2",
					sk: "PROFILE",
					name: "Should not be created",
					age: 25,
				} as any,
			},
		]);
		t.fail("Should have thrown");
	} catch (e: any) {
		t.ok(e.message);
	}

	// Verify CC#2 was not created
	const result = await mapper.get(UserModel, {
		pk: "CC#2",
		sk: "PROFILE",
	} as any);
	t.equal(result, undefined);
});
