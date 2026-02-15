import { Signer } from "@fgiova/aws-signature";
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { test } from "tap";
import { Agent, MockAgent, setGlobalDispatcher } from "undici";
import { equals } from "../../src/expressions/condition";
import { set, updateExpression } from "../../src/expressions/update";
import { DataMapper } from "../../src/mapper/data-mapper";
import { defineModel } from "../../src/model";

const TestModel = defineModel({
	tableName: "TestTable",
	schema: {
		pk: { type: "String", keyType: "HASH" },
		sk: { type: "String", keyType: "RANGE" },
		name: { type: "String" },
		age: { type: "Number" },
		version: { type: "Number", versionAttribute: true },
	},
} as const);

const originalAgent = new Agent();

test("DataMapper", async (t) => {
	let mockAgent: MockAgent;
	let mockPool: ReturnType<MockAgent["get"]>;
	let client: MiniDynamoClient;
	let mapper: DataMapper;
	let signer: Signer;

	t.beforeEach(async () => {
		mockAgent = new MockAgent();
		mockAgent.disableNetConnect();
		setGlobalDispatcher(mockAgent);
		mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");
		signer = new Signer({
			credentials: {
				accessKeyId: "test",
				secretAccessKey: "test",
			},
		});
		client = new MiniDynamoClient("us-east-1", undefined, undefined, signer);
		mapper = new DataMapper({ client });
	});

	t.afterEach(async () => {
		setGlobalDispatcher(originalAgent);
		await signer.destroy();
		await mockAgent.close();
	});

	// === PUT ===

	await t.test("put - basic item with version increment", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const result = await mapper.put(TestModel, {
			pk: "PK1",
			sk: "SK1",
			name: "John",
			age: 30,
			version: 0,
		} as any);

		t.equal(result.pk, "PK1");
		t.equal(result.name, "John");
		t.equal(result.version, 1);
	});

	await t.test("put - new item version starts at 0", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const result = await mapper.put(TestModel, {
			pk: "PK1",
			sk: "SK1",
			name: "John",
			age: 30,
		} as any);

		t.equal(result.version, 0);
	});

	await t.test("put - skipVersionCheck", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const result = await mapper.put(
			TestModel,
			{ pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
			{ skipVersionCheck: true },
		);

		t.equal(result.version, undefined);
	});

	// === GET ===

	await t.test("get - item found", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Item: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "John" },
					age: { N: "30" },
					version: { N: "1" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.get(TestModel, {
			pk: "PK1",
			sk: "SK1",
		} as any);
		t.ok(result);
		t.equal(result!.pk, "PK1");
		t.equal(result!.name, "John");
		t.equal(result!.age, 30);
		t.equal(result!.version, 1);
	});

	await t.test("get - item not found", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const result = await mapper.get(TestModel, {
			pk: "NOTFOUND",
			sk: "SK1",
		} as any);
		t.equal(result, undefined);
	});

	// === DELETE ===

	await t.test("delete - returns old item", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Attributes: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "John" },
					age: { N: "30" },
					version: { N: "1" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.delete(TestModel, {
			pk: "PK1",
			sk: "SK1",
		} as any);
		t.ok(result);
		t.equal(result!.pk, "PK1");
	});

	// === UPDATE ===

	await t.test("update - basic SET", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Attributes: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "Jane" },
					age: { N: "25" },
					version: { N: "2" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.update(
			TestModel,
			{ pk: "PK1", sk: "SK1" } as any,
			updateExpression(set("name", "Jane"), set("age", 25)),
		);

		t.equal(result.name, "Jane");
		t.equal(result.age, 25);
	});

	// === QUERY ===

	await t.test("query - iterates items across pages", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
						version: { N: "0" },
					},
					{
						pk: { S: "PK1" },
						sk: { S: "SK2" },
						name: { S: "Jane" },
						age: { N: "25" },
						version: { N: "0" },
					},
				],
				LastEvaluatedKey: { pk: { S: "PK1" }, sk: { S: "SK2" } },
				Count: 2,
				ScannedCount: 2,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK3" },
						name: { S: "Bob" },
						age: { N: "35" },
						version: { N: "0" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.query(TestModel, equals("pk", "PK1"))) {
			items.push(item);
		}

		t.equal(items.length, 3);
		t.equal(items[0].name, "John");
		t.equal(items[1].name, "Jane");
		t.equal(items[2].name, "Bob");
	});

	await t.test("query - pages() returns page metadata", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
						version: { N: "0" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const pages: any[] = [];
		for await (const page of mapper
			.query(TestModel, equals("pk", "PK1"))
			.pages()) {
			pages.push(page);
		}

		t.equal(pages.length, 1);
		t.equal(pages[0].items.length, 1);
		t.equal(pages[0].count, 1);
		t.equal(pages[0].scannedCount, 1);
	});

	// === SCAN ===

	await t.test("scan - basic", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
						version: { N: "0" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.scan(TestModel)) {
			items.push(item);
		}

		t.equal(items.length, 1);
		t.equal(items[0].name, "John");
	});

	// === BATCH ===

	await t.test("batchGet - basic", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: {
					TestTable: [
						{
							pk: { S: "PK1" },
							sk: { S: "SK1" },
							name: { S: "John" },
							age: { N: "30" },
							version: { N: "0" },
						},
						{
							pk: { S: "PK2" },
							sk: { S: "SK2" },
							name: { S: "Jane" },
							age: { N: "25" },
							version: { N: "0" },
						},
					],
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.batchGet(TestModel, [
			{ pk: "PK1", sk: "SK1" } as any,
			{ pk: "PK2", sk: "SK2" } as any,
		])) {
			items.push(item);
		}

		t.equal(items.length, 2);
		t.equal(items[0].name, "John");
		t.equal(items[1].name, "Jane");
	});

	await t.test("batchPut - basic", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const items: any[] = [];
		for await (const item of mapper.batchPut(TestModel, [
			{ pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
			{ pk: "PK2", sk: "SK2", name: "Jane", age: 25 } as any,
		])) {
			items.push(item);
		}

		t.equal(items.length, 2);
	});

	await t.test("batchDelete - basic", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		const keys: any[] = [];
		for await (const key of mapper.batchDelete(TestModel, [
			{ pk: "PK1", sk: "SK1" } as any,
			{ pk: "PK2", sk: "SK2" } as any,
		])) {
			keys.push(key);
		}

		t.equal(keys.length, 2);
	});

	// === TRANSACTIONS ===

	await t.test("transactGet - basic", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: [
					{
						Item: {
							pk: { S: "PK1" },
							sk: { S: "SK1" },
							name: { S: "John" },
							age: { N: "30" },
							version: { N: "1" },
						},
					},
				],
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const results = await mapper.transactGet([
			{
				model: TestModel,
				key: { pk: "PK1", sk: "SK1" } as any,
			},
		]);

		t.equal(results.length, 1);
		t.equal(results[0].pk, "PK1");
		t.equal(results[0].name, "John");
	});

	await t.test("transactWrite - put and delete", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		await mapper.transactWrite([
			{
				type: "Put",
				model: TestModel,
				item: {
					pk: "PK1",
					sk: "SK1",
					name: "John",
					age: 30,
				} as any,
			},
			{
				type: "Delete",
				model: TestModel,
				key: { pk: "PK2", sk: "SK2" } as any,
			},
		]);

		t.pass("transactWrite completed successfully");
	});

	await t.test("transactWrite - with condition check", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{},
			{
				headers: { "content-type": "application/x-amz-json-1.0" },
			},
		);

		await mapper.transactWrite([
			{
				type: "ConditionCheck",
				model: TestModel,
				key: { pk: "PK1", sk: "SK1" } as any,
				condition: equals("name", "John"),
			},
		]);

		t.pass("transactWrite with condition check completed");
	});
});

test("cleanup", async () => {
	setGlobalDispatcher(originalAgent);
});
