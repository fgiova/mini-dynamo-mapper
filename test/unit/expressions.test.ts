import { test } from "tap";
import { ExpressionAttributes } from "../../src/expressions/attributes";
import {
	and,
	attributeExists,
	attributeNotExists,
	attributeType,
	beginsWith,
	between,
	contains,
	equals,
	greaterThan,
	greaterThanOrEqual,
	inList,
	lessThan,
	lessThanOrEqual,
	not,
	notEquals,
	or,
} from "../../src/expressions/condition";
import { projection } from "../../src/expressions/projection";
import {
	serializeConditionExpression,
	serializeProjectionExpression,
	serializeUpdateExpression,
} from "../../src/expressions/serialize";
import {
	add,
	decrement,
	deleteFromSet,
	ifNotExists,
	increment,
	listAppend,
	listPrepend,
	remove,
	set,
	updateExpression,
} from "../../src/expressions/update";

// === Condition Factory Tests ===

test("equals creates correct expression", async (t) => {
	const expr = equals("name", "John");
	t.same(expr, {
		type: "Simple",
		subject: "name",
		predicate: { type: "Equals", value: "John" },
	});
});

test("notEquals creates correct expression", async (t) => {
	const expr = notEquals("name", "John");
	t.equal(expr.predicate.type, "NotEquals");
});

test("lessThan creates correct expression", async (t) => {
	const expr = lessThan("age", 18);
	t.equal(expr.predicate.type, "LessThan");
	t.equal((expr.predicate as any).value, 18);
});

test("lessThanOrEqual creates correct expression", async (t) => {
	const expr = lessThanOrEqual("age", 18);
	t.equal(expr.predicate.type, "LessThanOrEqual");
});

test("greaterThan creates correct expression", async (t) => {
	const expr = greaterThan("age", 18);
	t.equal(expr.predicate.type, "GreaterThan");
});

test("greaterThanOrEqual creates correct expression", async (t) => {
	const expr = greaterThanOrEqual("age", 18);
	t.equal(expr.predicate.type, "GreaterThanOrEqual");
});

test("between creates correct expression", async (t) => {
	const expr = between("age", 18, 65);
	t.equal(expr.predicate.type, "Between");
	t.equal((expr.predicate as any).lowerBound, 18);
	t.equal((expr.predicate as any).upperBound, 65);
});

test("inList creates correct expression", async (t) => {
	const expr = inList("status", ["active", "pending"]);
	t.equal(expr.predicate.type, "InList");
	t.same((expr.predicate as any).values, ["active", "pending"]);
});

test("attributeExists creates correct expression", async (t) => {
	const expr = attributeExists("email");
	t.equal(expr.predicate.type, "AttributeExists");
});

test("attributeNotExists creates correct expression", async (t) => {
	const expr = attributeNotExists("email");
	t.equal(expr.predicate.type, "AttributeNotExists");
});

test("attributeType creates correct expression", async (t) => {
	const expr = attributeType("data", "S");
	t.equal(expr.predicate.type, "AttributeType");
	t.equal((expr.predicate as any).expected, "S");
});

test("beginsWith creates correct expression", async (t) => {
	const expr = beginsWith("pk", "USER#");
	t.equal(expr.predicate.type, "BeginsWith");
	t.equal((expr.predicate as any).expected, "USER#");
});

test("contains creates correct expression", async (t) => {
	const expr = contains("name", "John");
	t.equal(expr.predicate.type, "Contains");
});

test("and combines conditions", async (t) => {
	const expr = and(equals("a", 1), greaterThan("b", 2));
	t.equal(expr.type, "And");
	t.equal(expr.conditions.length, 2);
});

test("or combines conditions", async (t) => {
	const expr = or(equals("a", 1), equals("b", 2));
	t.equal(expr.type, "Or");
	t.equal(expr.conditions.length, 2);
});

test("not wraps condition", async (t) => {
	const expr = not(equals("a", 1));
	t.equal(expr.type, "Not");
	t.equal(expr.condition.type, "Simple");
});

test("nested and/or/not", async (t) => {
	const expr = or(and(equals("a", 1), equals("b", 2)), not(equals("c", 3)));
	t.equal(expr.type, "Or");
	t.equal(expr.conditions.length, 2);
	t.equal(expr.conditions[0].type, "And");
	t.equal(expr.conditions[1].type, "Not");
});

// === Update Expression Tests ===

test("set creates SetAction", async (t) => {
	const action = set("name", "John");
	t.same(action, { type: "Set", path: "name", value: "John" });
});

test("remove creates RemoveAction", async (t) => {
	const action = remove("oldField");
	t.same(action, { type: "Remove", path: "oldField" });
});

test("add creates AddAction", async (t) => {
	const action = add("count", 1);
	t.same(action, { type: "Add", path: "count", value: 1 });
});

test("deleteFromSet creates DeleteAction", async (t) => {
	const action = deleteFromSet("tags", new Set(["old"]));
	t.equal(action.type, "Delete");
	t.equal(action.path, "tags");
});

test("increment creates mathematical expression", async (t) => {
	const action = increment("count", 5);
	t.equal(action.type, "Set");
	t.equal(action.path, "count");
	t.equal(action.value.type, "MathematicalExpression");
	t.equal(action.value.operation, "+");
	t.equal(action.value.operand2, 5);
});

test("decrement creates mathematical expression", async (t) => {
	const action = decrement("count", 3);
	t.equal(action.type, "Set");
	t.equal(action.value.operation, "-");
	t.equal(action.value.operand2, 3);
});

test("updateExpression combines actions", async (t) => {
	const expr = updateExpression(
		set("name", "John"),
		remove("oldField"),
		add("count", 1),
	);
	t.equal(expr.actions.length, 3);
});

test("ifNotExists creates function expression", async (t) => {
	const expr = ifNotExists("count", 0);
	t.equal(expr.type, "FunctionExpression");
	t.equal(expr.name, "if_not_exists");
	t.same(expr.args, ["count", 0]);
});

test("listAppend creates function expression", async (t) => {
	const expr = listAppend("items", ["new"]);
	t.equal(expr.name, "list_append");
	t.same(expr.args, ["items", ["new"]]);
});

test("listPrepend creates function expression with reversed args", async (t) => {
	const expr = listPrepend("items", ["new"]);
	t.equal(expr.name, "list_append");
	t.same(expr.args, [["new"], "items"]);
});

// === Projection Tests ===

test("projection creates path list", async (t) => {
	const result = projection("name", "age", "address.city");
	t.same(result, ["name", "age", "address.city"]);
});

// === ExpressionAttributes Tests ===

test("addName generates unique placeholders", async (t) => {
	const attrs = new ExpressionAttributes();
	const p1 = attrs.addName("name");
	const p2 = attrs.addName("age");
	t.match(p1, /^#attr\d+$/);
	t.match(p2, /^#attr\d+$/);
	t.not(p1, p2);
	t.same(attrs.names, { [p1]: "name", [p2]: "age" });
});

test("addName deduplicates same attribute", async (t) => {
	const attrs = new ExpressionAttributes();
	const p1 = attrs.addName("name");
	const p2 = attrs.addName("name");
	t.equal(p1, p2);
});

test("addName handles nested paths", async (t) => {
	const attrs = new ExpressionAttributes();
	const path = attrs.addName("address.city");
	t.ok(path.includes("."));
	const parts = path.split(".");
	t.equal(parts.length, 2);
	t.match(parts[0], /^#attr\d+$/);
	t.match(parts[1], /^#attr\d+$/);
});

test("addName handles array index in path", async (t) => {
	const attrs = new ExpressionAttributes();
	const path = attrs.addName("items[0].name");
	t.ok(path.includes("[0]"));
});

test("addValue generates unique placeholders", async (t) => {
	const attrs = new ExpressionAttributes();
	const p1 = attrs.addValue({ S: "hello" });
	const p2 = attrs.addValue({ N: "42" });
	t.match(p1, /^:val\d+$/);
	t.match(p2, /^:val\d+$/);
	t.not(p1, p2);
});

// === Serialization Tests ===

test("serialize equals", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(equals("name", "John"), attrs);
	t.ok(result.includes("="));
	t.match(result, /#attr\d+ = :val\d+/);
});

test("serialize notEquals", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(notEquals("name", "John"), attrs);
	t.ok(result.includes("<>"));
});

test("serialize lessThan", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(lessThan("age", 18), attrs);
	t.ok(result.includes("<"));
	t.notOk(result.includes("<>"));
});

test("serialize between", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(between("age", 18, 65), attrs);
	t.ok(result.includes("BETWEEN"));
	t.ok(result.includes("AND"));
});

test("serialize inList", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(
		inList("status", ["active", "pending"]),
		attrs,
	);
	t.ok(result.includes("IN"));
});

test("serialize attribute_exists", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(attributeExists("email"), attrs);
	t.ok(result.includes("attribute_exists"));
});

test("serialize attribute_not_exists", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(
		attributeNotExists("email"),
		attrs,
	);
	t.ok(result.includes("attribute_not_exists"));
});

test("serialize begins_with", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(beginsWith("pk", "USER#"), attrs);
	t.ok(result.includes("begins_with"));
});

test("serialize contains", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(contains("name", "John"), attrs);
	t.ok(result.includes("contains"));
});

test("serialize and", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(
		and(equals("a", 1), equals("b", 2)),
		attrs,
	);
	t.ok(result.includes("AND"));
});

test("serialize or", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(
		or(equals("a", 1), equals("b", 2)),
		attrs,
	);
	t.ok(result.includes("OR"));
});

test("serialize not", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeConditionExpression(not(equals("a", 1)), attrs);
	t.ok(result.includes("NOT"));
});

test("serialize SET + REMOVE update", async (t) => {
	const attrs = new ExpressionAttributes();
	const expr = updateExpression(set("name", "John"), remove("old"));
	const result = serializeUpdateExpression(expr, attrs);
	t.ok(result.includes("SET"));
	t.ok(result.includes("REMOVE"));
});

test("serialize ADD + DELETE update", async (t) => {
	const attrs = new ExpressionAttributes();
	const expr = updateExpression(
		add("count", 1),
		deleteFromSet("tags", new Set(["old"])),
	);
	const result = serializeUpdateExpression(expr, attrs);
	t.ok(result.includes("ADD"));
	t.ok(result.includes("DELETE"));
});

test("serialize increment update", async (t) => {
	const attrs = new ExpressionAttributes();
	const expr = updateExpression(increment("count", 5));
	const result = serializeUpdateExpression(expr, attrs);
	t.ok(result.includes("SET"));
	t.ok(result.includes("+"));
});

test("serialize projection", async (t) => {
	const attrs = new ExpressionAttributes();
	const result = serializeProjectionExpression(
		["name", "age", "address.city"],
		attrs,
	);
	t.ok(result.includes(","));
	const parts = result.split(", ");
	t.equal(parts.length, 3);
});
