import type { ConditionExpression } from "../types/expressions";
import type { Schema } from "../types/schema-type";

export function getVersionField(
	schema: Schema,
): { fieldName: string; attributeName: string } | undefined {
	for (const key of Object.keys(schema)) {
		const schemaType = schema[key];
		if (schemaType.type === "Number" && schemaType.versionAttribute) {
			return {
				fieldName: key,
				attributeName: schemaType.attributeName ?? key,
			};
		}
	}
	return undefined;
}

export function buildVersionCondition(
	schema: Schema,
	item: Record<string, any>,
): ConditionExpression | undefined {
	const versionField = getVersionField(schema);
	if (!versionField) return undefined;

	const currentVersion = item[versionField.fieldName];

	if (currentVersion === undefined || currentVersion === null) {
		return {
			type: "Simple",
			subject: versionField.attributeName,
			predicate: { type: "AttributeNotExists" },
		};
	}

	return {
		type: "Simple",
		subject: versionField.attributeName,
		predicate: { type: "Equals", value: currentVersion },
	};
}

export function incrementVersion<T extends Record<string, any>>(
	schema: Schema,
	item: T,
): T {
	const versionField = getVersionField(schema);
	if (!versionField) return item;

	const clone = { ...item };
	const currentVersion = clone[versionField.fieldName];

	if (currentVersion === undefined || currentVersion === null) {
		(clone as any)[versionField.fieldName] = 0;
	} else {
		(clone as any)[versionField.fieldName] = currentVersion + 1;
	}

	return clone;
}

export function mergeVersionCondition(
	userCondition: ConditionExpression | undefined,
	versionCondition: ConditionExpression | undefined,
): ConditionExpression | undefined {
	if (userCondition && versionCondition) {
		return {
			type: "And",
			conditions: [userCondition, versionCondition],
		};
	}
	return userCondition ?? versionCondition;
}
