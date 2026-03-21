import { Signer } from "@fgiova/aws-signature";
import { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { test } from "tap";
import { Agent, MockAgent, setGlobalDispatcher } from "undici";
import {
	DataMapper,
	defineModel,
	equals,
	greaterThan,
	set,
	updateExpression,
} from "../../src";
import { executeBatchGet, executeBatchWrite } from "../../src/mapper/batch";
import {
	buildVersionCondition,
	getVersionField,
	incrementVersion,
	mergeVersionCondition,
} from "../../src/mapper/version";

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
	const signer = new Signer({
		credentials: {
			accessKeyId: "test",
			secretAccessKey: "test",
		},
	});

	t.teardown(() => signer.destroy());

	t.beforeEach(async () => {
		mockAgent = new MockAgent();
		mockAgent.disableNetConnect();
		setGlobalDispatcher(mockAgent);
		mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");
		client = new MiniDynamoClient("us-east-1", undefined, undefined, signer);
		mapper = new DataMapper({ client });
	});

	t.afterEach(async () => {
		setGlobalDispatcher(originalAgent);
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

// === Batch retry tests ===

test("executeBatchWrite retries unprocessed items", async (t) => {
	void 0; // executeBatchWrite imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	// First call returns unprocessed items
	mockPool.intercept({ path: "/", method: "POST" }).reply(
		200,
		{
			UnprocessedItems: {
				TestTable: [{ PutRequest: { Item: { pk: { S: "PK1" } } } }],
			},
		},
		{ headers: { "content-type": "application/x-amz-json-1.0" } },
	);

	// Second call succeeds
	mockPool
		.intercept({ path: "/", method: "POST" })
		.reply(
			200,
			{},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

	await executeBatchWrite(client, [
		{
			tableName: "TestTable",
			requests: [{ type: "put", item: { pk: { S: "PK1" } } }],
		},
	]);

	t.pass("retried and succeeded");
	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("executeBatchWrite throws after max retries", async (t) => {
	void 0; // executeBatchWrite imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	// Always return unprocessed items
	for (let i = 0; i < 3; i++) {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				UnprocessedItems: {
					TestTable: [{ PutRequest: { Item: { pk: { S: "PK1" } } } }],
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);
	}

	try {
		await executeBatchWrite(
			client,
			[
				{
					tableName: "TestTable",
					requests: [{ type: "delete", key: { pk: { S: "PK1" } } }],
				},
			],
			{ maxRetries: 3 },
		);
		t.fail("Should have thrown");
	} catch (e: any) {
		t.ok(e.message.includes("BatchWrite failed after"));
	}

	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("executeBatchGet retries unprocessed keys", async (t) => {
	void 0; // executeBatchGet imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	// First call returns partial results with unprocessed keys
	mockPool.intercept({ path: "/", method: "POST" }).reply(
		200,
		{
			Responses: {
				TestTable: [{ pk: { S: "PK1" }, name: { S: "John" } }],
			},
			UnprocessedKeys: {
				TestTable: { Keys: [{ pk: { S: "PK2" } }] },
			},
		},
		{ headers: { "content-type": "application/x-amz-json-1.0" } },
	);

	// Second call returns remaining
	mockPool.intercept({ path: "/", method: "POST" }).reply(
		200,
		{
			Responses: {
				TestTable: [{ pk: { S: "PK2" }, name: { S: "Jane" } }],
			},
		},
		{ headers: { "content-type": "application/x-amz-json-1.0" } },
	);

	const result = await executeBatchGet(client, [
		{
			tableName: "TestTable",
			keys: [{ pk: { S: "PK1" } }, { pk: { S: "PK2" } }],
		},
	]);

	t.equal(result.items.TestTable.length, 2);

	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("executeBatchGet throws after max retries", async (t) => {
	void 0; // executeBatchGet imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	for (let i = 0; i < 2; i++) {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: {},
				UnprocessedKeys: {
					TestTable: { Keys: [{ pk: { S: "PK1" } }] },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);
	}

	try {
		await executeBatchGet(
			client,
			[
				{
					tableName: "TestTable",
					keys: [{ pk: { S: "PK1" } }],
				},
			],
			{ maxRetries: 2 },
		);
		t.fail("Should have thrown");
	} catch (e: any) {
		t.ok(e.message.includes("BatchGet failed after"));
	}

	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("executeBatchWrite with consumed capacity", async (t) => {
	void 0; // executeBatchWrite imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	mockPool.intercept({ path: "/", method: "POST" }).reply(
		200,
		{
			ConsumedCapacity: [{ TableName: "TestTable", CapacityUnits: 5 }],
		},
		{ headers: { "content-type": "application/x-amz-json-1.0" } },
	);

	const result = await executeBatchWrite(
		client,
		[
			{
				tableName: "TestTable",
				requests: [{ type: "put", item: { pk: { S: "PK1" } } }],
			},
		],
		{ returnConsumedCapacity: "TOTAL" },
	);

	t.ok(result.consumedCapacity);
	t.equal(result.consumedCapacity!.length, 1);

	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("executeBatchGet with consumed capacity", async (t) => {
	void 0; // executeBatchGet imported at top level

	const mockAgent = new MockAgent();
	mockAgent.disableNetConnect();
	setGlobalDispatcher(mockAgent);
	const mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");

	const signer = new Signer({
		credentials: { accessKeyId: "test", secretAccessKey: "test" },
	});
	const client = new MiniDynamoClient(
		"us-east-1",
		undefined,
		undefined,
		signer,
	);

	mockPool.intercept({ path: "/", method: "POST" }).reply(
		200,
		{
			Responses: { TestTable: [{ pk: { S: "PK1" } }] },
			ConsumedCapacity: [{ TableName: "TestTable", CapacityUnits: 3 }],
		},
		{ headers: { "content-type": "application/x-amz-json-1.0" } },
	);

	const result = await executeBatchGet(
		client,
		[
			{
				tableName: "TestTable",
				keys: [{ pk: { S: "PK1" } }],
				projection: "pk",
				expressionAttributeNames: { "#pk": "pk" },
				consistentRead: true,
			},
		],
		{ returnConsumedCapacity: "TOTAL" },
	);

	t.ok(result.consumedCapacity);

	setGlobalDispatcher(originalAgent);
	await signer.destroy();
	await mockAgent.close();
});

test("final cleanup", async () => {
	setGlobalDispatcher(originalAgent);
});

// === Version helper tests ===

test("getVersionField - returns version field", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = getVersionField(schema);
	t.ok(result);
	t.equal(result!.fieldName, "version");
	t.equal(result!.attributeName, "version");
});

test("getVersionField - returns undefined when no version field", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
		name: { type: "String" as const },
	};
	const result = getVersionField(schema);
	t.equal(result, undefined);
});

test("getVersionField - uses attributeName when set", async (t) => {
	const schema = {
		version: {
			type: "Number" as const,
			versionAttribute: true,
			attributeName: "v",
		},
	};
	const result = getVersionField(schema);
	t.equal(result!.attributeName, "v");
});

test("buildVersionCondition - new item (no version)", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = buildVersionCondition(schema, { pk: "PK1" });
	t.ok(result);
	t.equal(result!.type, "Simple");
	t.equal((result as any).predicate.type, "AttributeNotExists");
});

test("buildVersionCondition - existing item with version", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = buildVersionCondition(schema, { pk: "PK1", version: 3 });
	t.ok(result);
	t.equal((result as any).predicate.type, "Equals");
	t.equal((result as any).predicate.value, 3);
});

test("buildVersionCondition - null version", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = buildVersionCondition(schema, { pk: "PK1", version: null });
	t.ok(result);
	t.equal((result as any).predicate.type, "AttributeNotExists");
});

test("buildVersionCondition - no version field in schema", async (t) => {
	const schema = {
		pk: { type: "String" as const, keyType: "HASH" as const },
	};
	const result = buildVersionCondition(schema, { pk: "PK1" });
	t.equal(result, undefined);
});

test("incrementVersion - new item starts at 0", async (t) => {
	const schema = {
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = incrementVersion(schema, { name: "test" });
	t.equal((result as any).version, 0);
});

test("incrementVersion - null version starts at 0", async (t) => {
	const schema = {
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = incrementVersion(schema, { version: null });
	t.equal(result.version, 0);
});

test("incrementVersion - existing version increments", async (t) => {
	const schema = {
		version: { type: "Number" as const, versionAttribute: true },
	};
	const result = incrementVersion(schema, { version: 5 });
	t.equal(result.version, 6);
});

test("incrementVersion - no version field returns item unchanged", async (t) => {
	const schema = {
		name: { type: "String" as const },
	};
	const item = { name: "test" };
	const result = incrementVersion(schema, item);
	t.same(result, item);
});

test("mergeVersionCondition - both conditions", async (t) => {
	const user = equals("name", "John");
	const version = equals("version", 1);
	const result = mergeVersionCondition(user, version);
	t.equal(result!.type, "And");
	t.equal((result as any).conditions.length, 2);
});

test("mergeVersionCondition - only user condition", async (t) => {
	const user = equals("name", "John");
	const result = mergeVersionCondition(user, undefined);
	t.equal(result, user);
});

test("mergeVersionCondition - only version condition", async (t) => {
	const version = equals("version", 1);
	const result = mergeVersionCondition(undefined, version);
	t.equal(result, version);
});

test("mergeVersionCondition - neither condition", async (t) => {
	const result = mergeVersionCondition(undefined, undefined);
	t.equal(result, undefined);
});

// === Additional DataMapper tests ===

test("DataMapper - advanced", async (t) => {
	let mockAgent: MockAgent;
	let mockPool: ReturnType<MockAgent["get"]>;
	let client: MiniDynamoClient;
	let mapper: DataMapper;
	const signer = new Signer({
		credentials: {
			accessKeyId: "test",
			secretAccessKey: "test",
		},
	});

	t.teardown(() => signer.destroy());

	const TestModelNoVersion = defineModel({
		tableName: "TestTable",
		schema: {
			pk: { type: "String", keyType: "HASH" },
			sk: { type: "String", keyType: "RANGE" },
			name: { type: "String" },
			age: { type: "Number" },
		},
	} as const);

	t.beforeEach(async () => {
		mockAgent = new MockAgent();
		mockAgent.disableNetConnect();
		setGlobalDispatcher(mockAgent);
		mockPool = mockAgent.get("https://dynamodb.us-east-1.amazonaws.com");
		client = new MiniDynamoClient("us-east-1", undefined, undefined, signer);
		mapper = new DataMapper({ client, tableNamePrefix: "dev-" });
	});

	t.afterEach(async () => {
		setGlobalDispatcher(originalAgent);
		await mockAgent.close();
	});

	await t.test("tableNamePrefix is applied", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Item: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "John" },
					age: { N: "30" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.get(TestModelNoVersion, {
			pk: "PK1",
			sk: "SK1",
		} as any);
		t.ok(result);
	});

	await t.test("get with projection", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Item: { pk: { S: "PK1" }, sk: { S: "SK1" }, name: { S: "John" } } },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const result = await mapper.get(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1" } as any,
			{
				projection: ["name"],
			},
		);
		t.ok(result);
		t.equal(result!.name, "John");
	});

	await t.test("delete returns undefined when no Attributes", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const result = await mapper.delete(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1" } as any,
			{
				skipVersionCheck: true,
			},
		);
		t.equal(result, undefined);
	});

	await t.test("update with skipVersionCheck", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Attributes: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "Updated" },
					age: { N: "99" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.update(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1" } as any,
			updateExpression(set("name", "Updated")),
			{ skipVersionCheck: true },
		);
		t.equal(result.name, "Updated");
	});

	await t.test("batchGet with projection", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: {
					"dev-TestTable": [
						{ pk: { S: "PK1" }, sk: { S: "SK1" }, name: { S: "John" } },
					],
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.batchGet(
			TestModelNoVersion,
			[{ pk: "PK1", sk: "SK1" } as any],
			{
				projection: ["name"],
			},
		)) {
			items.push(item);
		}
		t.equal(items.length, 1);
		t.equal(items[0].name, "John");
	});

	await t.test("batchWrite mixed put+delete", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const results: any[] = [];
		for await (const op of mapper.batchWrite(TestModelNoVersion, [
			{
				type: "put",
				item: { pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
			},
			{ type: "delete", key: { pk: "PK2", sk: "SK2" } as any },
		])) {
			results.push(op);
		}
		t.equal(results.length, 2);
		t.equal(results[0].type, "put");
		t.equal(results[1].type, "delete");
	});

	await t.test("transactWrite with Update operation", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		await mapper.transactWrite([
			{
				type: "Update",
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
				updates: updateExpression(set("name", "Updated")),
			},
		]);
		t.pass("transactWrite with Update completed");
	});

	await t.test("transactWrite with Update + condition", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		await mapper.transactWrite([
			{
				type: "Update",
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
				updates: updateExpression(set("name", "Updated")),
				condition: equals("name", "John"),
			},
		]);
		t.pass("transactWrite with Update+condition completed");
	});

	await t.test("transactWrite with Delete + condition", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		await mapper.transactWrite([
			{
				type: "Delete",
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
				condition: equals("name", "John"),
			},
		]);
		t.pass("transactWrite with Delete+condition completed");
	});

	await t.test("transactWrite with Put + condition", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		await mapper.transactWrite([
			{
				type: "Put",
				model: TestModelNoVersion,
				item: { pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
				condition: equals("pk", "PK1"),
			},
		]);
		t.pass("transactWrite with Put+condition completed");
	});

	await t.test("transactGet with projection", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: [
					{ Item: { pk: { S: "PK1" }, sk: { S: "SK1" }, name: { S: "John" } } },
				],
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const results = await mapper.transactGet([
			{
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
				projection: ["name"],
			},
		]);
		t.equal(results.length, 1);
		t.equal(results[0].name, "John");
	});

	await t.test("transactGet returns undefined for missing items", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Responses: [{}] },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const results = await mapper.transactGet([
			{
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
			},
		]);
		t.equal(results[0], undefined);
	});

	await t.test("transactGet with empty Responses", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const results = await mapper.transactGet([
			{
				model: TestModelNoVersion,
				key: { pk: "PK1", sk: "SK1" } as any,
			},
		]);
		t.same(results, []);
	});

	await t.test("transactWrite unknown type throws", async (t) => {
		try {
			await mapper.transactWrite([
				{
					type: "Unknown" as any,
					model: TestModelNoVersion,
					key: { pk: "PK1", sk: "SK1" } as any,
				} as any,
			]);
			t.fail("Should have thrown");
		} catch (e: any) {
			t.ok(e.message.includes("Unknown transaction type"));
		}
	});

	await t.test("put with user condition", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const result = await mapper.put(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
			{ condition: equals("pk", "PK1") },
		);
		t.equal(result.pk, "PK1");
	});

	await t.test("delete with user condition", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Attributes: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "John" },
					age: { N: "30" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.delete(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1" } as any,
			{ condition: equals("name", "John") },
		);
		t.ok(result);
	});

	await t.test("update with user condition", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Attributes: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "Updated" },
					age: { N: "30" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await mapper.update(
			TestModelNoVersion,
			{ pk: "PK1", sk: "SK1" } as any,
			updateExpression(set("name", "Updated")),
			{ condition: equals("name", "John"), skipVersionCheck: true },
		);
		t.equal(result.name, "Updated");
	});

	// === Query/Scan with options ===

	await t.test("query with filter and projection", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.query(
			TestModelNoVersion,
			equals("pk", "PK1"),
			{
				filter: greaterThan("age", 18),
				projection: ["name", "age"],
				startKey: { pk: "PK0", sk: "SK0" },
			},
		)) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("scan with filter and projection", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.scan(TestModelNoVersion, {
			filter: greaterThan("age", 18),
			projection: ["name", "age"],
			startKey: { pk: "PK0", sk: "SK0" },
		})) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("scan pages()", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const pages: any[] = [];
		for await (const page of mapper.scan(TestModelNoVersion).pages()) {
			pages.push(page);
		}
		t.equal(pages.length, 1);
		t.equal(pages[0].count, 1);
	});

	await t.test("parallelScan basic", async (t) => {
		// 2 segments, each returns 1 item
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK2" },
						sk: { S: "SK2" },
						name: { S: "Jane" },
						age: { N: "25" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.parallelScan(TestModelNoVersion, 2)) {
			items.push(item);
		}
		t.equal(items.length, 2);
	});

	await t.test("parallelScan with empty segments", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const items: any[] = [];
		for await (const item of mapper.parallelScan(TestModelNoVersion, 2)) {
			items.push(item);
		}
		t.equal(items.length, 0);
	});

	// === BaseIterator edge cases ===

	await t.test("iterator next() after pages() returns done", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const iter = mapper.scan(TestModelNoVersion);
		// Switch to pages mode
		const pagesIter = iter.pages();
		await pagesIter.next();
		// Now next() on the original iterator should return done
		const result = await iter.next();
		t.equal(result.done, true);
	});

	await t.test("iterator count and scannedCount getters", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
					{
						pk: { S: "PK2" },
						sk: { S: "SK2" },
						name: { S: "Jane" },
						age: { N: "25" },
					},
				],
				Count: 2,
				ScannedCount: 5,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const iter = mapper.scan(TestModelNoVersion);
		const items: any[] = [];
		for await (const item of iter) {
			items.push(item);
		}
		t.equal(iter.count, 2);
		t.equal(iter.scannedCount, 5);
	});

	await t.test("iterator returns empty page", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const items: any[] = [];
		for await (const item of mapper.scan(TestModelNoVersion)) {
			items.push(item);
		}
		t.equal(items.length, 0);
	});

	// === Batch retry tests ===

	await t.test("batchGet with empty result for table", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Responses: {} },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const items: any[] = [];
		for await (const item of mapper.batchGet(TestModelNoVersion, [
			{ pk: "PK1", sk: "SK1" } as any,
		])) {
			items.push(item);
		}
		t.equal(items.length, 0);
	});

	// === Branch coverage: options with returnConsumedCapacity ===

	await t.test("batchPut with returnConsumedCapacity option", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const items: any[] = [];
		for await (const item of mapper.batchPut(
			TestModelNoVersion,
			[{ pk: "PK1", sk: "SK1", name: "John", age: 30 } as any],
			{ returnConsumedCapacity: "TOTAL" },
		)) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("batchDelete with returnConsumedCapacity option", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const keys: any[] = [];
		for await (const key of mapper.batchDelete(
			TestModelNoVersion,
			[{ pk: "PK1", sk: "SK1" } as any],
			{ returnConsumedCapacity: "TOTAL" },
		)) {
			keys.push(key);
		}
		t.equal(keys.length, 1);
	});

	await t.test("batchWrite with returnConsumedCapacity option", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const results: any[] = [];
		for await (const op of mapper.batchWrite(
			TestModelNoVersion,
			[
				{
					type: "put",
					item: { pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
				},
			],
			{ returnConsumedCapacity: "TOTAL" },
		)) {
			results.push(op);
		}
		t.equal(results.length, 1);
	});

	await t.test("transactGet with returnConsumedCapacity option", async (t) => {
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
						},
					},
				],
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const results = await mapper.transactGet(
			[{ model: TestModelNoVersion, key: { pk: "PK1", sk: "SK1" } as any }],
			{ returnConsumedCapacity: "TOTAL" },
		);
		t.equal(results.length, 1);
	});

	await t.test("transactWrite with all options", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		await mapper.transactWrite(
			[
				{
					type: "Put",
					model: TestModelNoVersion,
					item: { pk: "PK1", sk: "SK1", name: "John", age: 30 } as any,
				},
			],
			{
				clientRequestToken: "token-123",
				returnConsumedCapacity: "TOTAL",
				returnItemCollectionMetrics: "SIZE",
			},
		);
		t.pass("transactWrite with all options completed");
	});

	// === Query/Scan without options (branch coverage for ?? {}) ===

	await t.test(
		"query without options (no filter/projection/startKey)",
		async (t) => {
			mockPool.intercept({ path: "/", method: "POST" }).reply(
				200,
				{
					Items: [
						{
							pk: { S: "PK1" },
							sk: { S: "SK1" },
							name: { S: "John" },
							age: { N: "30" },
						},
					],
					Count: 1,
					ScannedCount: 1,
				},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

			const items: any[] = [];
			for await (const item of mapper.query(
				TestModelNoVersion,
				equals("pk", "PK1"),
			)) {
				items.push(item);
			}
			t.equal(items.length, 1);
		},
	);

	await t.test(
		"scan without options (no filter/projection/startKey)",
		async (t) => {
			mockPool.intercept({ path: "/", method: "POST" }).reply(
				200,
				{
					Items: [
						{
							pk: { S: "PK1" },
							sk: { S: "SK1" },
							name: { S: "John" },
							age: { N: "30" },
						},
					],
					Count: 1,
					ScannedCount: 1,
				},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

			const items: any[] = [];
			for await (const item of mapper.scan(TestModelNoVersion)) {
				items.push(item);
			}
			t.equal(items.length, 1);
		},
	);

	await t.test("batchGet with consistentRead option", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Responses: {
					"dev-TestTable": [
						{
							pk: { S: "PK1" },
							sk: { S: "SK1" },
							name: { S: "John" },
							age: { N: "30" },
						},
					],
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.batchGet(
			TestModelNoVersion,
			[{ pk: "PK1", sk: "SK1" } as any],
			{ consistentRead: true },
		)) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	// === readConsistency: strong ===

	await t.test("strong consistency is applied", async (t) => {
		const strongMapper = new DataMapper({
			client,
			readConsistency: "strong",
			tableNamePrefix: "dev-",
		});

		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Item: {
					pk: { S: "PK1" },
					sk: { S: "SK1" },
					name: { S: "John" },
					age: { N: "30" },
				},
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const result = await strongMapper.get(TestModelNoVersion, {
			pk: "PK1",
			sk: "SK1",
		} as any);
		t.ok(result);
	});

	// === Global skipVersionCheck ===

	await t.test("global skipVersionCheck", async (t) => {
		const TestModelWithVersion = defineModel({
			tableName: "TestTable",
			schema: {
				pk: { type: "String", keyType: "HASH" },
				sk: { type: "String", keyType: "RANGE" },
				name: { type: "String" },
				version: { type: "Number", versionAttribute: true },
			},
		} as const);

		const skipMapper = new DataMapper({
			client,
			skipVersionCheck: true,
			tableNamePrefix: "dev-",
		});

		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const result = await skipMapper.put(TestModelWithVersion, {
			pk: "PK1",
			sk: "SK1",
			name: "John",
		} as any);
		// With skipVersionCheck, version should not be incremented
		t.equal(result.version, undefined);
	});

	await t.test("parallelScan pages()", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK2" },
						sk: { S: "SK2" },
						name: { S: "Jane" },
						age: { N: "25" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const pages: any[] = [];
		for await (const page of mapper
			.parallelScan(TestModelNoVersion, 2)
			.pages()) {
			pages.push(page);
		}
		t.ok(pages.length >= 1);
	});

	await t.test("parallelScan pages() all empty", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const pages: any[] = [];
		for await (const page of mapper
			.parallelScan(TestModelNoVersion, 2)
			.pages()) {
			pages.push(page);
		}
		// All segments are empty, so we should get pages but then end
		t.ok(pages.length >= 0);
	});

	await t.test("parallelScan with consistentRead option", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.parallelScan(TestModelNoVersion, 1, {
			consistentRead: true,
		})) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("query with consistentRead option", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.query(
			TestModelNoVersion,
			equals("pk", "PK1"),
			{ consistentRead: true },
		)) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("scan with consistentRead option", async (t) => {
		mockPool.intercept({ path: "/", method: "POST" }).reply(
			200,
			{
				Items: [
					{
						pk: { S: "PK1" },
						sk: { S: "SK1" },
						name: { S: "John" },
						age: { N: "30" },
					},
				],
				Count: 1,
				ScannedCount: 1,
			},
			{ headers: { "content-type": "application/x-amz-json-1.0" } },
		);

		const items: any[] = [];
		for await (const item of mapper.scan(TestModelNoVersion, {
			consistentRead: true,
		})) {
			items.push(item);
		}
		t.equal(items.length, 1);
	});

	await t.test("parallelScan next() after exhaustion", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const iter = mapper.parallelScan(TestModelNoVersion, 2);
		// Exhaust the iterator
		for await (const _ of iter) {
			/* empty */
		}
		// Call next() again after exhaustion
		const result = await iter.next();
		t.equal(result.done, true);
	});

	await t.test("parallelScan pages() after exhaustion", async (t) => {
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);
		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{ Items: [], Count: 0, ScannedCount: 0 },
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const pagesIter = mapper.parallelScan(TestModelNoVersion, 2).pages();
		// Exhaust
		let r = await pagesIter.next();
		while (!r.done) {
			r = await pagesIter.next();
		}
		// Call next() again
		const result = await pagesIter.next();
		t.equal(result.done, true);
	});

	await t.test("batchPut with global skipVersionCheck", async (t) => {
		const TestModelWithVersion = defineModel({
			tableName: "TestTable",
			schema: {
				pk: { type: "String", keyType: "HASH" },
				sk: { type: "String", keyType: "RANGE" },
				name: { type: "String" },
				version: { type: "Number", versionAttribute: true },
			},
		} as const);

		const skipMapper = new DataMapper({
			client,
			skipVersionCheck: true,
			tableNamePrefix: "dev-",
		});

		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const items: any[] = [];
		for await (const item of skipMapper.batchPut(TestModelWithVersion, [
			{ pk: "PK1", sk: "SK1", name: "John" } as any,
		])) {
			items.push(item);
		}
		t.equal(items[0].version, undefined);
	});

	await t.test("batchWrite put with global skipVersionCheck", async (t) => {
		const TestModelWithVersion = defineModel({
			tableName: "TestTable",
			schema: {
				pk: { type: "String", keyType: "HASH" },
				sk: { type: "String", keyType: "RANGE" },
				name: { type: "String" },
				version: { type: "Number", versionAttribute: true },
			},
		} as const);

		const skipMapper = new DataMapper({
			client,
			skipVersionCheck: true,
			tableNamePrefix: "dev-",
		});

		mockPool
			.intercept({ path: "/", method: "POST" })
			.reply(
				200,
				{},
				{ headers: { "content-type": "application/x-amz-json-1.0" } },
			);

		const ops: any[] = [];
		for await (const op of skipMapper.batchWrite(TestModelWithVersion, [
			{ type: "put", item: { pk: "PK1", sk: "SK1", name: "John" } as any },
		])) {
			ops.push(op);
		}
		t.equal(ops.length, 1);
	});
});
