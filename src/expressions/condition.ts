import type {
	AndExpression,
	ConditionExpression,
	NotExpression,
	OrExpression,
	SimpleConditionExpression,
} from "../types/expressions";

export function equals(subject: string, value: any): SimpleConditionExpression {
	return { type: "Simple", subject, predicate: { type: "Equals", value } };
}

export function notEquals(
	subject: string,
	value: any,
): SimpleConditionExpression {
	return { type: "Simple", subject, predicate: { type: "NotEquals", value } };
}

export function lessThan(
	subject: string,
	value: any,
): SimpleConditionExpression {
	return { type: "Simple", subject, predicate: { type: "LessThan", value } };
}

export function lessThanOrEqual(
	subject: string,
	value: any,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "LessThanOrEqual", value },
	};
}

export function greaterThan(
	subject: string,
	value: any,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "GreaterThan", value },
	};
}

export function greaterThanOrEqual(
	subject: string,
	value: any,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "GreaterThanOrEqual", value },
	};
}

export function between(
	subject: string,
	lowerBound: any,
	upperBound: any,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "Between", lowerBound, upperBound },
	};
}

export function inList(
	subject: string,
	values: any[],
): SimpleConditionExpression {
	return { type: "Simple", subject, predicate: { type: "InList", values } };
}

export function attributeExists(subject: string): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "AttributeExists" },
	};
}

export function attributeNotExists(subject: string): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "AttributeNotExists" },
	};
}

export function attributeType(
	subject: string,
	expected: string,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "AttributeType", expected },
	};
}

export function beginsWith(
	subject: string,
	expected: string,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "BeginsWith", expected },
	};
}

export function contains(
	subject: string,
	expected: any,
): SimpleConditionExpression {
	return {
		type: "Simple",
		subject,
		predicate: { type: "Contains", expected },
	};
}

export function and(...conditions: ConditionExpression[]): AndExpression {
	return { type: "And", conditions };
}

export function or(...conditions: ConditionExpression[]): OrExpression {
	return { type: "Or", conditions };
}

export function not(condition: ConditionExpression): NotExpression {
	return { type: "Not", condition };
}
