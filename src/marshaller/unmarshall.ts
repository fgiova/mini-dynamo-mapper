import type { AttributeValue } from "@fgiova/mini-dynamo-client";
import type { Schema, SchemaType } from "../types/schema-type";

export function unmarshallItem<T = Record<string, any>>(
	schema: Schema,
	attributeMap: Record<string, AttributeValue>,
	ctor?: new () => T,
): T {
	const result: Record<string, any> = ctor ? new ctor() : ({} as any);

	for (const key of Object.keys(schema)) {
		const schemaType = schema[key];
		const dynamoKey = schemaType.attributeName ?? key;
		const attributeValue = attributeMap[dynamoKey];

		if (attributeValue === undefined) {
			continue;
		}

		result[key] = unmarshallValue(schemaType, attributeValue);
	}

	return result as T;
}

export function unmarshallValue(
	schemaType: SchemaType,
	attributeValue: AttributeValue,
): any {
	if (attributeValue.NULL) {
		return null;
	}

	switch (schemaType.type) {
		case "String":
			return attributeValue.S ?? null;

		case "Number":
			return attributeValue.N !== undefined ? Number(attributeValue.N) : null;

		case "Boolean":
			return attributeValue.BOOL ?? null;

		case "Binary":
			return attributeValue.B !== undefined
				? base64ToBuffer(attributeValue.B)
				: null;

		case "Date":
			return attributeValue.N !== undefined
				? new Date(Number(attributeValue.N) * 1000)
				: null;

		case "Null":
			return null;

		case "List": {
			if (!attributeValue.L) return [];
			return attributeValue.L.map((item) =>
				unmarshallValue(schemaType.memberType, item),
			);
		}

		case "Map": {
			if (!attributeValue.M) return new Map();
			const map = new Map<string, any>();
			for (const [k, v] of Object.entries(attributeValue.M)) {
				map.set(k, unmarshallValue(schemaType.memberType, v));
			}
			return map;
		}

		case "Set": {
			switch (schemaType.memberType) {
				case "String":
					return new Set(attributeValue.SS ?? []);
				case "Number":
					return new Set((attributeValue.NS ?? []).map(Number));
				case "Binary":
					return new Set((attributeValue.BS ?? []).map(base64ToBuffer));
			}
			/* c8 ignore next 2 */
			break;
		}

		case "Document": {
			if (!attributeValue.M) return null;
			const unmarshalled = unmarshallItem(schemaType.members, attributeValue.M);
			if (schemaType.valueConstructor) {
				return Object.assign(new schemaType.valueConstructor(), unmarshalled);
			}
			return unmarshalled;
		}

		case "Tuple": {
			if (!attributeValue.L) return [];
			return schemaType.members.map((member, i) =>
				attributeValue.L![i]
					? unmarshallValue(member, attributeValue.L![i])
					: undefined,
			);
		}

		case "Collection": {
			if (!attributeValue.L) return [];
			return attributeValue.L.map(autoUnmarshallValue);
		}

		case "Hash": {
			if (!attributeValue.M) return {};
			const result: Record<string, any> = {};
			for (const [k, v] of Object.entries(attributeValue.M)) {
				result[k] = autoUnmarshallValue(v);
			}
			return result;
		}

		case "Any":
			return autoUnmarshallValue(attributeValue);

		case "Custom":
			return schemaType.unmarshall(attributeValue);
	}
}

function autoUnmarshallValue(attributeValue: AttributeValue): any {
	if (attributeValue.NULL) return null;
	if (attributeValue.S !== undefined) return attributeValue.S;
	if (attributeValue.N !== undefined) return Number(attributeValue.N);
	if (attributeValue.BOOL !== undefined) return attributeValue.BOOL;
	if (attributeValue.B !== undefined) return base64ToBuffer(attributeValue.B);
	if (attributeValue.SS) return new Set(attributeValue.SS);
	if (attributeValue.NS) return new Set(attributeValue.NS.map(Number));
	if (attributeValue.BS) return new Set(attributeValue.BS.map(base64ToBuffer));
	if (attributeValue.L) return attributeValue.L.map(autoUnmarshallValue);
	if (attributeValue.M) {
		const result: Record<string, any> = {};
		for (const [k, v] of Object.entries(attributeValue.M)) {
			result[k] = autoUnmarshallValue(v);
		}
		return result;
	}
	/* c8 ignore start */
	return null;
}
/* c8 ignore stop */

function base64ToBuffer(b64: string): Uint8Array {
	return new Uint8Array(Buffer.from(b64, "base64"));
}
