import type { AttributeValue } from "@fgiova/mini-dynamo-client";

export type AttributePath = string;

// Condition predicates
export interface EqualsPredicate {
	type: "Equals";
	value: any;
}
export interface NotEqualsPredicate {
	type: "NotEquals";
	value: any;
}
export interface LessThanPredicate {
	type: "LessThan";
	value: any;
}
export interface LessThanOrEqualPredicate {
	type: "LessThanOrEqual";
	value: any;
}
export interface GreaterThanPredicate {
	type: "GreaterThan";
	value: any;
}
export interface GreaterThanOrEqualPredicate {
	type: "GreaterThanOrEqual";
	value: any;
}
export interface BetweenPredicate {
	type: "Between";
	lowerBound: any;
	upperBound: any;
}
export interface InListPredicate {
	type: "InList";
	values: any[];
}
export interface AttributeExistsPredicate {
	type: "AttributeExists";
}
export interface AttributeNotExistsPredicate {
	type: "AttributeNotExists";
}
export interface AttributeTypePredicate {
	type: "AttributeType";
	expected: string;
}
export interface BeginsWithPredicate {
	type: "BeginsWith";
	expected: string;
}
export interface ContainsPredicate {
	type: "Contains";
	expected: any;
}

export type ConditionPredicate =
	| EqualsPredicate
	| NotEqualsPredicate
	| LessThanPredicate
	| LessThanOrEqualPredicate
	| GreaterThanPredicate
	| GreaterThanOrEqualPredicate
	| BetweenPredicate
	| InListPredicate
	| AttributeExistsPredicate
	| AttributeNotExistsPredicate
	| AttributeTypePredicate
	| BeginsWithPredicate
	| ContainsPredicate;

// Condition expressions
export interface SimpleConditionExpression {
	type: "Simple";
	subject: AttributePath;
	predicate: ConditionPredicate;
}

export interface AndExpression {
	type: "And";
	conditions: ConditionExpression[];
}

export interface OrExpression {
	type: "Or";
	conditions: ConditionExpression[];
}

export interface NotExpression {
	type: "Not";
	condition: ConditionExpression;
}

export type ConditionExpression =
	| SimpleConditionExpression
	| AndExpression
	| OrExpression
	| NotExpression;

// Mathematical expressions
export interface MathematicalExpression {
	type: "MathematicalExpression";
	operand1: AttributePath | number;
	operation: "+" | "-";
	operand2: AttributePath | number;
}

// Function expressions
export interface FunctionExpression {
	type: "FunctionExpression";
	name: "if_not_exists" | "list_append";
	args: any[];
}

// Update expression types
export interface SetAction {
	type: "Set";
	path: AttributePath;
	value: any | MathematicalExpression | FunctionExpression;
}

export interface RemoveAction {
	type: "Remove";
	path: AttributePath;
}

export interface AddAction {
	type: "Add";
	path: AttributePath;
	value: any;
}

export interface DeleteAction {
	type: "Delete";
	path: AttributePath;
	value: any;
}

export type UpdateAction = SetAction | RemoveAction | AddAction | DeleteAction;

export interface UpdateExpression {
	actions: UpdateAction[];
}

// Projection
export type ProjectionExpression = AttributePath[];

// Expression attributes
export interface ExpressionAttributeNames {
	[placeholder: string]: string;
}

export interface ExpressionAttributeValues {
	[placeholder: string]: AttributeValue;
}

export interface SerializedExpression {
	expression: string;
	names: ExpressionAttributeNames;
	values: ExpressionAttributeValues;
}
