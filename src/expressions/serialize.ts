import { autoMarshallValue } from "../marshaller/marshall";
import type {
	ConditionExpression,
	ProjectionExpression,
	UpdateExpression,
} from "../types/expressions";
import type { ExpressionAttributes } from "./attributes";

export function serializeConditionExpression(
	expr: ConditionExpression,
	attributes: ExpressionAttributes,
): string {
	switch (expr.type) {
		case "Simple": {
			const path = attributes.addName(expr.subject);
			const predicate = expr.predicate;

			switch (predicate.type) {
				case "Equals": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} = ${val}`;
				}
				case "NotEquals": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} <> ${val}`;
				}
				case "LessThan": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} < ${val}`;
				}
				case "LessThanOrEqual": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} <= ${val}`;
				}
				case "GreaterThan": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} > ${val}`;
				}
				case "GreaterThanOrEqual": {
					const val = attributes.addValue(autoMarshallValue(predicate.value)!);
					return `${path} >= ${val}`;
				}
				case "Between": {
					const lower = attributes.addValue(
						autoMarshallValue(predicate.lowerBound)!,
					);
					const upper = attributes.addValue(
						autoMarshallValue(predicate.upperBound)!,
					);
					return `${path} BETWEEN ${lower} AND ${upper}`;
				}
				case "InList": {
					const vals = predicate.values.map((v) =>
						attributes.addValue(autoMarshallValue(v)!),
					);
					return `${path} IN (${vals.join(", ")})`;
				}
				case "AttributeExists":
					return `attribute_exists(${path})`;
				case "AttributeNotExists":
					return `attribute_not_exists(${path})`;
				case "AttributeType": {
					const val = attributes.addValue({ S: predicate.expected });
					return `attribute_type(${path}, ${val})`;
				}
				case "BeginsWith": {
					const val = attributes.addValue({ S: predicate.expected });
					return `begins_with(${path}, ${val})`;
				}
				case "Contains": {
					const val = attributes.addValue(
						autoMarshallValue(predicate.expected)!,
					);
					return `contains(${path}, ${val})`;
				}
			}
			break;
		}
		case "And": {
			const parts = expr.conditions.map(
				(c) => `(${serializeConditionExpression(c, attributes)})`,
			);
			return parts.join(" AND ");
		}
		case "Or": {
			const parts = expr.conditions.map(
				(c) => `(${serializeConditionExpression(c, attributes)})`,
			);
			return parts.join(" OR ");
		}
		case "Not":
			return `NOT (${serializeConditionExpression(expr.condition, attributes)})`;
	}

	return "";
}

export function serializeUpdateExpression(
	expr: UpdateExpression,
	attributes: ExpressionAttributes,
): string {
	const setClauses: string[] = [];
	const removeClauses: string[] = [];
	const addClauses: string[] = [];
	const deleteClauses: string[] = [];

	for (const action of expr.actions) {
		switch (action.type) {
			case "Set": {
				const path = attributes.addName(action.path);
				const valueStr = serializeSetValue(
					action.value,
					action.path,
					attributes,
				);
				setClauses.push(`${path} = ${valueStr}`);
				break;
			}
			case "Remove": {
				const path = attributes.addName(action.path);
				removeClauses.push(path);
				break;
			}
			case "Add": {
				const path = attributes.addName(action.path);
				const val = attributes.addValue(autoMarshallValue(action.value)!);
				addClauses.push(`${path} ${val}`);
				break;
			}
			case "Delete": {
				const path = attributes.addName(action.path);
				const val = attributes.addValue(autoMarshallValue(action.value)!);
				deleteClauses.push(`${path} ${val}`);
				break;
			}
		}
	}

	const parts: string[] = [];
	if (setClauses.length > 0) parts.push(`SET ${setClauses.join(", ")}`);
	if (removeClauses.length > 0)
		parts.push(`REMOVE ${removeClauses.join(", ")}`);
	if (addClauses.length > 0) parts.push(`ADD ${addClauses.join(", ")}`);
	if (deleteClauses.length > 0)
		parts.push(`DELETE ${deleteClauses.join(", ")}`);

	return parts.join(" ");
}

export function serializeProjectionExpression(
	paths: ProjectionExpression,
	attributes: ExpressionAttributes,
): string {
	return paths.map((p) => attributes.addName(p)).join(", ");
}

function serializeSetValue(
	value: any,
	_actionPath: string,
	attributes: ExpressionAttributes,
): string {
	if (
		value &&
		typeof value === "object" &&
		value.type === "MathematicalExpression"
	) {
		const op1 =
			typeof value.operand1 === "string"
				? attributes.addName(value.operand1)
				: attributes.addValue({ N: String(value.operand1) });
		const op2 =
			typeof value.operand2 === "string"
				? attributes.addName(value.operand2)
				: attributes.addValue({ N: String(value.operand2) });
		return `${op1} ${value.operation} ${op2}`;
	}

	if (
		value &&
		typeof value === "object" &&
		value.type === "FunctionExpression"
	) {
		if (value.name === "if_not_exists") {
			const pathArg =
				typeof value.args[0] === "string"
					? attributes.addName(value.args[0])
					: attributes.addValue(autoMarshallValue(value.args[0])!);
			const defaultVal = attributes.addValue(autoMarshallValue(value.args[1])!);
			return `if_not_exists(${pathArg}, ${defaultVal})`;
		}
		if (value.name === "list_append") {
			const arg0 = Array.isArray(value.args[0])
				? attributes.addValue(autoMarshallValue(value.args[0])!)
				: attributes.addName(value.args[0]);
			const arg1 = Array.isArray(value.args[1])
				? attributes.addValue(autoMarshallValue(value.args[1])!)
				: attributes.addName(value.args[1]);
			return `list_append(${arg0}, ${arg1})`;
		}
	}

	return attributes.addValue(autoMarshallValue(value)!);
}
