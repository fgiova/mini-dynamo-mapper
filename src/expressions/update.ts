import type {
	AddAction,
	DeleteAction,
	FunctionExpression,
	MathematicalExpression,
	RemoveAction,
	SetAction,
	UpdateAction,
	UpdateExpression,
} from "../types/expressions";

export function updateExpression(...actions: UpdateAction[]): UpdateExpression {
	return { actions };
}

export function set(path: string, value: any): SetAction {
	return { type: "Set", path, value };
}

export function remove(path: string): RemoveAction {
	return { type: "Remove", path };
}

export function add(path: string, value: any): AddAction {
	return { type: "Add", path, value };
}

export function deleteFromSet(path: string, value: any): DeleteAction {
	return { type: "Delete", path, value };
}

export function increment(path: string, by = 1): SetAction {
	const mathExpr: MathematicalExpression = {
		type: "MathematicalExpression",
		operand1: path,
		operation: "+",
		operand2: by,
	};
	return { type: "Set", path, value: mathExpr };
}

export function decrement(path: string, by = 1): SetAction {
	const mathExpr: MathematicalExpression = {
		type: "MathematicalExpression",
		operand1: path,
		operation: "-",
		operand2: by,
	};
	return { type: "Set", path, value: mathExpr };
}

export function ifNotExists(
	path: string,
	defaultValue: any,
): FunctionExpression {
	return {
		type: "FunctionExpression",
		name: "if_not_exists",
		args: [path, defaultValue],
	};
}

export function listAppend(path: string, values: any[]): FunctionExpression {
	return {
		type: "FunctionExpression",
		name: "list_append",
		args: [path, values],
	};
}

export function listPrepend(path: string, values: any[]): FunctionExpression {
	return {
		type: "FunctionExpression",
		name: "list_append",
		args: [values, path],
	};
}
