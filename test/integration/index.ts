import { Signer } from "@fgiova/aws-signature";
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { test } from "tap";
import { equals } from "../../src/expressions/condition";
import { set, updateExpression } from "../../src/expressions/update";
import { DataMapper } from "../../src/mapper/data-mapper";
import { defineModel } from "../../src/model";
import { cleanTable } from "../helpers/dynamoTable";
import { getEndpoint } from "../helpers/localtest";

const endpoint = getEndpoint();

if (!process.env.LOCALSTACK_ENDPOINT && !process.env.TEST_LOCAL) {
	console.log("Skipping integration tests - no LOCALSTACK_ENDPOINT set");
	process.exit(0);
}

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

let signer: Signer;
let client: MiniDynamoClient;
let mapper: DataMapper;

test("setup", async (_t) => {
	signer = new Signer({
		accessKeyId: "test",
		secretAccessKey: "test",
	});
	client = new MiniDynamoClient("us-east-1", endpoint, undefined, signer);
	mapper = new DataMapper({ client });
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

test("teardown", async (_t) => {
	await client.destroy();
	await signer.destroy();
});
