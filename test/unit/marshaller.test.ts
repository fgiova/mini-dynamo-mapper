import { test } from "tap";
import {
	autoMarshallValue,
	marshallItem,
	marshallKey,
	marshallValue,
} from "../../src/marshaller/marshall";
import {
	unmarshallItem,
	unmarshallValue,
} from "../../src/marshaller/unmarshall";
import type { Schema, SchemaType } from "../../src/types/schema-type";

test("marshallValue - String", async (t) => {
	const result = marshallValue({ type: "String" }, "hello");
	t.same(result, { S: "hello" });
});

test("marshallValue - Number", async (t) => {
	t.same(marshallValue({ type: "Number" }, 42), { N: "42" });
	t.same(marshallValue({ type: "Number" }, 3.14), { N: "3.14" });
});

test("marshallValue - Boolean", async (t) => {
	t.same(marshallValue({ type: "Boolean" }, true), { BOOL: true });
	t.same(marshallValue({ type: "Boolean" }, false), { BOOL: false });
});

test("marshallValue - Binary", async (t) => {
	const buf = new Uint8Array([1, 2, 3]);
	const result = marshallValue({ type: "Binary" }, buf);
	t.ok(result?.B);
	t.equal(typeof result?.B, "string");
	// Verify round-trip
	const decoded = Buffer.from(result!.B!, "base64");
	t.same(new Uint8Array(decoded), buf);
});

test("marshallValue - Date", async (t) => {
	const date = new Date("2024-01-01T00:00:00Z");
	const result = marshallValue({ type: "Date" }, date);
	t.same(result, { N: String(Math.floor(date.getTime() / 1000)) });
});

test("marshallValue - Null", async (t) => {
	t.same(marshallValue({ type: "Null" }, null), { NULL: true });
	t.same(marshallValue({ type: "String" }, null), { NULL: true });
});

test("marshallValue - List", async (t) => {
	const schemaType: SchemaType = {
		type: "List",
		memberType: { type: "Number" },
	};
	const result = marshallValue(schemaType, [1, 2, 3]);
	t.same(result, { L: [{ N: "1" }, { N: "2" }, { N: "3" }] });
});

test("marshallValue - Map", async (t) => {
	const schemaType: SchemaType = {
		type: "Map",
		memberType: { type: "Number" },
	};
	const map = new Map([
		["a", 1],
		["b", 2],
	]);
	const result = marshallValue(schemaType, map);
	t.same(result, { M: { a: { N: "1" }, b: { N: "2" } } });
});

test("marshallValue - Set String", async (t) => {
	const schemaType: SchemaType = { type: "Set", memberType: "String" };
	const result = marshallValue(schemaType, new Set(["a", "b"]));
	t.ok(result?.SS);
	t.same(new Set(result!.SS), new Set(["a", "b"]));
});

test("marshallValue - Set Number", async (t) => {
	const schemaType: SchemaType = { type: "Set", memberType: "Number" };
	const result = marshallValue(schemaType, new Set([1, 2]));
	t.ok(result?.NS);
	t.same(new Set(result!.NS), new Set(["1", "2"]));
});

test("marshallValue - empty Set returns NULL", async (t) => {
	const schemaType: SchemaType = { type: "Set", memberType: "String" };
	const result = marshallValue(schemaType, new Set());
	t.same(result, { NULL: true });
});

test("marshallValue - Document", async (t) => {
	const schemaType: SchemaType = {
		type: "Document",
		members: {
			key: { type: "String" },
			value: { type: "Number" },
		},
	};
	const result = marshallValue(schemaType, { key: "test", value: 42 });
	t.same(result, { M: { key: { S: "test" }, value: { N: "42" } } });
});

test("marshallValue - Tuple", async (t) => {
	const schemaType: SchemaType = {
		type: "Tuple",
		members: [{ type: "String" }, { type: "Number" }],
	};
	const result = marshallValue(schemaType, ["hello", 42]);
	t.same(result, { L: [{ S: "hello" }, { N: "42" }] });
});

test("marshallValue - Collection", async (t) => {
	const schemaType: SchemaType = { type: "Collection" };
	const result = marshallValue(schemaType, ["hello", 42, true]);
	t.same(result, { L: [{ S: "hello" }, { N: "42" }, { BOOL: true }] });
});

test("marshallValue - Hash", async (t) => {
	const schemaType: SchemaType = { type: "Hash" };
	const result = marshallValue(schemaType, { name: "John", age: 30 });
	t.same(result, { M: { name: { S: "John" }, age: { N: "30" } } });
});

test("marshallValue - Any", async (t) => {
	t.same(marshallValue({ type: "Any" }, "hello"), { S: "hello" });
	t.same(marshallValue({ type: "Any" }, 42), { N: "42" });
	t.same(marshallValue({ type: "Any" }, true), { BOOL: true });
	t.same(marshallValue({ type: "Any" }, null), { NULL: true });
});

test("marshallValue - Custom", async (t) => {
	const schemaType: SchemaType = {
		type: "Custom",
		marshall: (value: string) => ({ S: value.toUpperCase() }),
		unmarshall: (av) => av.S!.toLowerCase(),
	};
	const result = marshallValue(schemaType, "hello");
	t.same(result, { S: "HELLO" });
});

test("marshallItem - basic", async (t) => {
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		name: { type: "String" },
		age: { type: "Number" },
	};
	const result = marshallItem(schema, { pk: "PK1", name: "John", age: 30 });
	t.same(result, {
		pk: { S: "PK1" },
		name: { S: "John" },
		age: { N: "30" },
	});
});

test("marshallItem respects attributeName", async (t) => {
	const schema: Schema = {
		myField: { type: "String", attributeName: "my_field" },
	};
	const result = marshallItem(schema, { myField: "hello" });
	t.same(result, { my_field: { S: "hello" } });
});

test("marshallItem uses defaultProvider for undefined", async (t) => {
	const schema: Schema = {
		id: { type: "String", defaultProvider: () => "default-id" },
	};
	const result = marshallItem(schema, {});
	t.same(result, { id: { S: "default-id" } });
});

test("marshallItem skips undefined without defaultProvider", async (t) => {
	const schema: Schema = {
		name: { type: "String" },
		age: { type: "Number" },
	};
	const result = marshallItem(schema, { name: "John" });
	t.same(result, { name: { S: "John" } });
	t.notOk(result.age);
});

test("marshallKey marshalls only key fields", async (t) => {
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		sk: { type: "String", keyType: "RANGE" },
		name: { type: "String" },
	};
	const result = marshallKey(schema, { pk: "PK1", sk: "SK1" });
	t.same(result, { pk: { S: "PK1" }, sk: { S: "SK1" } });
	t.notOk(result.name);
});

test("unmarshallValue - String", async (t) => {
	t.equal(unmarshallValue({ type: "String" }, { S: "hello" }), "hello");
});

test("unmarshallValue - Number", async (t) => {
	t.equal(unmarshallValue({ type: "Number" }, { N: "42" }), 42);
	t.equal(unmarshallValue({ type: "Number" }, { N: "3.14" }), 3.14);
});

test("unmarshallValue - Boolean", async (t) => {
	t.equal(unmarshallValue({ type: "Boolean" }, { BOOL: true }), true);
});

test("unmarshallValue - Date", async (t) => {
	const date = new Date("2024-01-01T00:00:00Z");
	const epochSeconds = Math.floor(date.getTime() / 1000);
	const result = unmarshallValue({ type: "Date" }, { N: String(epochSeconds) });
	t.equal(result.getTime(), date.getTime());
});

test("unmarshallValue - Null", async (t) => {
	t.equal(unmarshallValue({ type: "String" }, { NULL: true }), null);
	t.equal(unmarshallValue({ type: "Null" }, { NULL: true }), null);
});

test("unmarshallValue - Set String", async (t) => {
	const result = unmarshallValue(
		{ type: "Set", memberType: "String" },
		{ SS: ["a", "b"] },
	);
	t.type(result, Set);
	t.same([...result], ["a", "b"]);
});

test("unmarshallValue - Set Number", async (t) => {
	const result = unmarshallValue(
		{ type: "Set", memberType: "Number" },
		{ NS: ["1", "2"] },
	);
	t.type(result, Set);
	t.same([...result], [1, 2]);
});

test("unmarshallValue - Document", async (t) => {
	const schemaType: SchemaType = {
		type: "Document",
		members: {
			key: { type: "String" },
			value: { type: "Number" },
		},
	};
	const result = unmarshallValue(schemaType, {
		M: { key: { S: "test" }, value: { N: "42" } },
	});
	t.same(result, { key: "test", value: 42 });
});

test("unmarshallValue - Custom", async (t) => {
	const schemaType: SchemaType = {
		type: "Custom",
		marshall: (value: string) => ({ S: value.toUpperCase() }),
		unmarshall: (av) => av.S!.toLowerCase(),
	};
	const result = unmarshallValue(schemaType, { S: "HELLO" });
	t.equal(result, "hello");
});

test("unmarshallItem - basic", async (t) => {
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		name: { type: "String" },
		age: { type: "Number" },
	};
	const result = unmarshallItem(schema, {
		pk: { S: "PK1" },
		name: { S: "John" },
		age: { N: "30" },
	});
	t.same(result, { pk: "PK1", name: "John", age: 30 });
});

test("unmarshallItem respects attributeName", async (t) => {
	const schema: Schema = {
		myField: { type: "String", attributeName: "my_field" },
	};
	const result = unmarshallItem(schema, { my_field: { S: "hello" } });
	t.same(result, { myField: "hello" });
});

test("round-trip: marshall -> unmarshall", async (t) => {
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		name: { type: "String" },
		age: { type: "Number" },
		active: { type: "Boolean" },
		tags: { type: "Set", memberType: "String" },
		metadata: {
			type: "Document",
			members: {
				key: { type: "String" },
				value: { type: "Number" },
			},
		},
	};
	const original = {
		pk: "PK1",
		name: "John",
		age: 30,
		active: true,
		tags: new Set(["tag1", "tag2"]),
		metadata: { key: "test", value: 42 },
	};
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);

	t.equal(unmarshalled.pk, original.pk);
	t.equal(unmarshalled.name, original.name);
	t.equal(unmarshalled.age, original.age);
	t.equal(unmarshalled.active, original.active);
	t.same([...unmarshalled.tags], [...original.tags]);
	t.same(unmarshalled.metadata, original.metadata);
});

test("round-trip: nested documents", async (t) => {
	const schema: Schema = {
		data: {
			type: "Document",
			members: {
				level1: {
					type: "Document",
					members: {
						level2: { type: "String" },
						count: { type: "Number" },
					},
				},
			},
		},
	};
	const original = { data: { level1: { level2: "deep", count: 99 } } };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled, original);
});

test("autoMarshallValue - various types", async (t) => {
	t.same(autoMarshallValue("hello"), { S: "hello" });
	t.same(autoMarshallValue(42), { N: "42" });
	t.same(autoMarshallValue(true), { BOOL: true });
	t.same(autoMarshallValue(null), { NULL: true });
	t.same(autoMarshallValue([1, 2]), { L: [{ N: "1" }, { N: "2" }] });
	t.same(autoMarshallValue({ a: "b" }), { M: { a: { S: "b" } } });
});
