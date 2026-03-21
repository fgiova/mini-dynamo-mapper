import { test } from "tap";
import type { Schema, SchemaType } from "../../src";
import {
	autoMarshallValue,
	marshallItem,
	marshallKey,
	marshallValue,
	unmarshallItem,
	unmarshallValue,
} from "../../src";

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

// === Missing coverage: unmarshall types ===

test("unmarshallValue - List", async (t) => {
	const schemaType: SchemaType = {
		type: "List",
		memberType: { type: "Number" },
	};
	const result = unmarshallValue(schemaType, {
		L: [{ N: "1" }, { N: "2" }, { N: "3" }],
	});
	t.same(result, [1, 2, 3]);
});

test("unmarshallValue - List empty (no L)", async (t) => {
	const schemaType: SchemaType = {
		type: "List",
		memberType: { type: "String" },
	};
	const result = unmarshallValue(schemaType, {});
	t.same(result, []);
});

test("unmarshallValue - Map", async (t) => {
	const schemaType: SchemaType = {
		type: "Map",
		memberType: { type: "Number" },
	};
	const result = unmarshallValue(schemaType, {
		M: { a: { N: "1" }, b: { N: "2" } },
	});
	t.type(result, Map);
	t.equal(result.get("a"), 1);
	t.equal(result.get("b"), 2);
});

test("unmarshallValue - Map empty (no M)", async (t) => {
	const schemaType: SchemaType = {
		type: "Map",
		memberType: { type: "String" },
	};
	const result = unmarshallValue(schemaType, {});
	t.type(result, Map);
	t.equal(result.size, 0);
});

test("unmarshallValue - Set Binary", async (t) => {
	const b64_1 = Buffer.from([1, 2]).toString("base64");
	const b64_2 = Buffer.from([3, 4]).toString("base64");
	const result = unmarshallValue(
		{ type: "Set", memberType: "Binary" },
		{ BS: [b64_1, b64_2] },
	);
	t.type(result, Set);
	const items = [...result];
	t.equal(items.length, 2);
	t.same(items[0], new Uint8Array([1, 2]));
	t.same(items[1], new Uint8Array([3, 4]));
});

test("unmarshallValue - Binary", async (t) => {
	const b64 = Buffer.from([1, 2, 3]).toString("base64");
	const result = unmarshallValue({ type: "Binary" }, { B: b64 });
	t.same(result, new Uint8Array([1, 2, 3]));
});

test("unmarshallValue - Binary null", async (t) => {
	const result = unmarshallValue({ type: "Binary" }, {});
	t.equal(result, null);
});

test("unmarshallValue - Tuple", async (t) => {
	const schemaType: SchemaType = {
		type: "Tuple",
		members: [{ type: "String" }, { type: "Number" }, { type: "Boolean" }],
	};
	const result = unmarshallValue(schemaType, {
		L: [{ S: "hello" }, { N: "42" }, { BOOL: true }],
	});
	t.same(result, ["hello", 42, true]);
});

test("unmarshallValue - Tuple empty (no L)", async (t) => {
	const schemaType: SchemaType = {
		type: "Tuple",
		members: [{ type: "String" }],
	};
	const result = unmarshallValue(schemaType, {});
	t.same(result, []);
});

test("unmarshallValue - Tuple with missing member", async (t) => {
	const schemaType: SchemaType = {
		type: "Tuple",
		members: [{ type: "String" }, { type: "Number" }],
	};
	const result = unmarshallValue(schemaType, {
		L: [{ S: "hello" }],
	});
	t.equal(result[0], "hello");
	t.equal(result[1], undefined);
});

test("unmarshallValue - Collection", async (t) => {
	const schemaType: SchemaType = { type: "Collection" };
	const result = unmarshallValue(schemaType, {
		L: [{ S: "hello" }, { N: "42" }, { BOOL: true }],
	});
	t.same(result, ["hello", 42, true]);
});

test("unmarshallValue - Collection empty (no L)", async (t) => {
	const schemaType: SchemaType = { type: "Collection" };
	const result = unmarshallValue(schemaType, {});
	t.same(result, []);
});

test("unmarshallValue - Hash", async (t) => {
	const schemaType: SchemaType = { type: "Hash" };
	const result = unmarshallValue(schemaType, {
		M: { name: { S: "John" }, age: { N: "30" } },
	});
	t.same(result, { name: "John", age: 30 });
});

test("unmarshallValue - Hash empty (no M)", async (t) => {
	const schemaType: SchemaType = { type: "Hash" };
	const result = unmarshallValue(schemaType, {});
	t.same(result, {});
});

test("unmarshallValue - Any", async (t) => {
	t.equal(unmarshallValue({ type: "Any" }, { S: "hello" }), "hello");
	t.equal(unmarshallValue({ type: "Any" }, { N: "42" }), 42);
	t.equal(unmarshallValue({ type: "Any" }, { BOOL: true }), true);
	t.equal(unmarshallValue({ type: "Any" }, { NULL: true }), null);
});

test("unmarshallValue - Document with null M", async (t) => {
	const schemaType: SchemaType = {
		type: "Document",
		members: { key: { type: "String" } },
	};
	const result = unmarshallValue(schemaType, {});
	t.equal(result, null);
});

test("unmarshallValue - Document with valueConstructor", async (t) => {
	class MyClass {
		key = "";
		value = 0;
	}
	const schemaType: SchemaType = {
		type: "Document",
		members: {
			key: { type: "String" },
			value: { type: "Number" },
		},
		valueConstructor: MyClass,
	};
	const result = unmarshallValue(schemaType, {
		M: { key: { S: "test" }, value: { N: "42" } },
	});
	t.type(result, MyClass);
	t.equal(result.key, "test");
	t.equal(result.value, 42);
});

test("unmarshallValue - String null", async (t) => {
	t.equal(unmarshallValue({ type: "String" }, {}), null);
});

test("unmarshallValue - Number null", async (t) => {
	t.equal(unmarshallValue({ type: "Number" }, {}), null);
});

test("unmarshallValue - Boolean null", async (t) => {
	t.equal(unmarshallValue({ type: "Boolean" }, {}), null);
});

test("unmarshallValue - Date null", async (t) => {
	t.equal(unmarshallValue({ type: "Date" }, {}), null);
});

test("unmarshallItem - with constructor", async (t) => {
	class User {
		pk = "";
		name = "";
	}
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		name: { type: "String" },
	};
	const result = unmarshallItem(
		schema,
		{ pk: { S: "PK1" }, name: { S: "John" } },
		User,
	);
	t.type(result, User);
	t.equal(result.pk, "PK1");
});

test("unmarshallItem - skips missing attributes", async (t) => {
	const schema: Schema = {
		pk: { type: "String", keyType: "HASH" },
		name: { type: "String" },
		age: { type: "Number" },
	};
	const result = unmarshallItem(schema, { pk: { S: "PK1" } });
	t.equal(result.pk, "PK1");
	t.equal(result.name, undefined);
	t.equal(result.age, undefined);
});

// === Missing coverage: autoUnmarshallValue edge cases ===

test("autoMarshallValue - Date", async (t) => {
	const date = new Date("2024-06-15T00:00:00Z");
	const result = autoMarshallValue(date);
	t.same(result, { N: String(Math.floor(date.getTime() / 1000)) });
});

test("autoMarshallValue - Uint8Array", async (t) => {
	const buf = new Uint8Array([1, 2, 3]);
	const result = autoMarshallValue(buf);
	t.ok(result?.B);
	t.equal(typeof result?.B, "string");
});

test("autoMarshallValue - Buffer", async (t) => {
	const buf = Buffer.from([1, 2, 3]);
	const result = autoMarshallValue(buf);
	t.ok(result?.B);
});

test("autoMarshallValue - Set String", async (t) => {
	const result = autoMarshallValue(new Set(["a", "b"]));
	t.same(result, { SS: ["a", "b"] });
});

test("autoMarshallValue - Set Number", async (t) => {
	const result = autoMarshallValue(new Set([1, 2]));
	t.ok(result?.NS);
});

test("autoMarshallValue - Set Binary", async (t) => {
	const result = autoMarshallValue(
		new Set([new Uint8Array([1]), new Uint8Array([2])]),
	);
	t.ok(result?.BS);
});

test("autoMarshallValue - empty Set", async (t) => {
	const result = autoMarshallValue(new Set());
	t.same(result, { NULL: true });
});

test("autoMarshallValue - Set of objects (fallback to L)", async (t) => {
	const result = autoMarshallValue(new Set([{ a: 1 }]));
	t.ok(result?.L);
});

test("autoMarshallValue - Map", async (t) => {
	const map = new Map<string, any>([["key", "value"]]);
	const result = autoMarshallValue(map);
	t.same(result, { M: { key: { S: "value" } } });
});

test("autoMarshallValue - undefined", async (t) => {
	t.same(autoMarshallValue(undefined), { NULL: true });
});

test("autoMarshallValue - undefined returns NULL", async (t) => {
	const result = autoMarshallValue(undefined);
	t.same(result, { NULL: true });
});

// === Missing coverage: marshall edge cases ===

test("marshallValue - Collection non-array", async (t) => {
	const result = marshallValue({ type: "Collection" }, "not-array");
	t.same(result, { NULL: true });
});

test("marshallValue - Hash null value", async (t) => {
	const result = marshallValue({ type: "Hash" }, null);
	t.same(result, { NULL: true });
});

test("marshallValue - Hash non-object", async (t) => {
	const result = marshallValue({ type: "Hash" }, "not-object");
	t.same(result, { NULL: true });
});

test("marshallValue - Set Binary", async (t) => {
	const b1 = new Uint8Array([1, 2]);
	const b2 = new Uint8Array([3, 4]);
	const result = marshallValue(
		{ type: "Set", memberType: "Binary" },
		new Set([b1, b2]),
	);
	t.ok(result?.BS);
	t.equal(result!.BS!.length, 2);
});

test("marshallValue - Binary with Buffer", async (t) => {
	const buf = Buffer.from([1, 2, 3]);
	const result = marshallValue({ type: "Binary" }, buf);
	t.ok(result?.B);
});

test("marshallValue - Binary with ArrayBuffer", async (t) => {
	const ab = new ArrayBuffer(3);
	const view = new Uint8Array(ab);
	view[0] = 1;
	view[1] = 2;
	view[2] = 3;
	const result = marshallValue({ type: "Binary" }, ab);
	t.ok(result?.B);
});

// === round-trip: advanced types ===

test("round-trip: Map type", async (t) => {
	const schema: Schema = {
		data: { type: "Map", memberType: { type: "String" } },
	};
	const original = {
		data: new Map([
			["k1", "v1"],
			["k2", "v2"],
		]),
	};
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.type(unmarshalled.data, Map);
	t.equal(unmarshalled.data.get("k1"), "v1");
	t.equal(unmarshalled.data.get("k2"), "v2");
});

test("round-trip: Tuple type", async (t) => {
	const schema: Schema = {
		data: {
			type: "Tuple",
			members: [{ type: "String" }, { type: "Number" }, { type: "Boolean" }],
		},
	};
	const original = { data: ["hello", 42, true] };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled.data, original.data);
});

test("round-trip: Collection type", async (t) => {
	const schema: Schema = {
		data: { type: "Collection" },
	};
	const original = { data: ["hello", 42, true] };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled.data, original.data);
});

test("round-trip: Hash type", async (t) => {
	const schema: Schema = {
		data: { type: "Hash" },
	};
	const original = { data: { name: "John", age: 30 } };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled.data, original.data);
});

test("round-trip: Any type", async (t) => {
	const schema: Schema = {
		data: { type: "Any" },
	};
	const original = { data: { nested: [1, "two", true] } };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled.data, original.data);
});

test("marshallValue - Null type explicit", async (t) => {
	t.same(marshallValue({ type: "Null" }, "anything"), { NULL: true });
});

test("unmarshallValue - Null type explicit", async (t) => {
	t.equal(unmarshallValue({ type: "Null" }, {}), null);
	t.equal(unmarshallValue({ type: "Null" }, { NULL: true }), null);
});

test("round-trip: Custom type", async (t) => {
	const schema: Schema = {
		data: {
			type: "Custom",
			marshall: (value: string) => ({ S: JSON.stringify(value) }),
			unmarshall: (av) => JSON.parse(av.S!),
		},
	};
	const original = { data: { key: "value" } };
	const marshalled = marshallItem(schema, original);
	const unmarshalled = unmarshallItem(schema, marshalled);
	t.same(unmarshalled.data, original.data);
});

// === autoUnmarshallValue branch coverage ===

test("unmarshallValue - Collection with nested types via autoUnmarshall", async (t) => {
	const schemaType: SchemaType = { type: "Collection" };
	const result = unmarshallValue(schemaType, {
		L: [
			{ NULL: true },
			{ S: "text" },
			{ N: "42" },
			{ BOOL: false },
			{ B: Buffer.from([1, 2]).toString("base64") },
			{ SS: ["a", "b"] },
			{ NS: ["1", "2"] },
			{ BS: [Buffer.from([3]).toString("base64")] },
			{ L: [{ S: "nested" }] },
			{ M: { k: { S: "v" } } },
		],
	});
	t.equal(result[0], null);
	t.equal(result[1], "text");
	t.equal(result[2], 42);
	t.equal(result[3], false);
	t.same(result[4], new Uint8Array([1, 2]));
	t.type(result[5], Set);
	t.same([...result[5]], ["a", "b"]);
	t.type(result[6], Set);
	t.same([...result[6]], [1, 2]);
	t.type(result[7], Set);
	t.same(result[8], ["nested"]);
	t.same(result[9], { k: "v" });
});

test("unmarshallValue - Hash with nested auto types", async (t) => {
	const schemaType: SchemaType = { type: "Hash" };
	const result = unmarshallValue(schemaType, {
		M: {
			binary: { B: Buffer.from([5]).toString("base64") },
			ss: { SS: ["x"] },
			ns: { NS: ["9"] },
			bs: { BS: [Buffer.from([7]).toString("base64")] },
		},
	});
	t.same(result.binary, new Uint8Array([5]));
	t.type(result.ss, Set);
	t.type(result.ns, Set);
	t.type(result.bs, Set);
});

// === Set unmarshall ?? branch (when SS/NS/BS are undefined) ===

test("unmarshallValue - Set String with missing SS", async (t) => {
	const result = unmarshallValue({ type: "Set", memberType: "String" }, {});
	t.type(result, Set);
	t.equal(result.size, 0);
});

test("unmarshallValue - Set Number with missing NS", async (t) => {
	const result = unmarshallValue({ type: "Set", memberType: "Number" }, {});
	t.type(result, Set);
	t.equal(result.size, 0);
});

test("unmarshallValue - Set Binary with missing BS", async (t) => {
	const result = unmarshallValue({ type: "Set", memberType: "Binary" }, {});
	t.type(result, Set);
	t.equal(result.size, 0);
});
