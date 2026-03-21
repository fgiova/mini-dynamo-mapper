import type { AttributeValue } from "@fgiova/mini-dynamo-client";
import type { Schema, SchemaType } from "../types/schema-type";

export function marshallItem(
	schema: Schema,
	item: Record<string, any>,
): Record<string, AttributeValue> {
	const result: Record<string, AttributeValue> = {};

	for (const key of Object.keys(schema)) {
		const schemaType = schema[key];
		const dynamoKey = schemaType.attributeName ?? key;
		let value = item[key];

		if (value === undefined && schemaType.defaultProvider) {
			value = schemaType.defaultProvider();
		}

		if (value === undefined) {
			continue;
		}

		const marshalled = marshallValue(schemaType, value);
		if (marshalled !== undefined) {
			result[dynamoKey] = marshalled;
		}
	}

	return result;
}

export function marshallKey(
	schema: Schema,
	key: Record<string, any>,
): Record<string, AttributeValue> {
	const result: Record<string, AttributeValue> = {};

	for (const k of Object.keys(schema)) {
		const schemaType = schema[k];
		if ("keyType" in schemaType && schemaType.keyType) {
			const dynamoKey = schemaType.attributeName ?? k;
			const value = key[k];
			if (value !== undefined) {
				const marshalled = marshallValue(schemaType, value);
				if (marshalled !== undefined) {
					result[dynamoKey] = marshalled;
				}
			}
		}
	}

	return result;
}

export function marshallValue(
	schemaType: SchemaType,
	value: any,
): AttributeValue | undefined {
	if (value === null || value === undefined) {
		return { NULL: true };
	}

	switch (schemaType.type) {
		case "String":
			return { S: String(value) };

		case "Number":
			return { N: String(value) };

		case "Boolean":
			return { BOOL: Boolean(value) };

		case "Binary": {
			const b64 = bufferToBase64(value);
			return { B: b64 };
		}

		case "Date":
			return { N: String(Math.floor(value.getTime() / 1000)) };

		case "Null":
			return { NULL: true };

		case "List": {
			const items: AttributeValue[] = [];
			for (const item of value) {
				const m = marshallValue(schemaType.memberType, item);
				if (m !== undefined) items.push(m);
			}
			return { L: items };
		}

		case "Map": {
			const map: Record<string, AttributeValue> = {};
			for (const [k, v] of value) {
				const m = marshallValue(schemaType.memberType, v);
				if (m !== undefined) map[k] = m;
			}
			return { M: map };
		}

		case "Set": {
			if (value.size === 0) {
				return { NULL: true };
			}
			const values = [...value];
			switch (schemaType.memberType) {
				case "String":
					return { SS: values.map(String) };
				case "Number":
					return { NS: values.map(String) };
				case "Binary":
					return { BS: values.map(bufferToBase64) };
			}
			/* c8 ignore next 2 */
			break;
		}

		case "Document":
			return { M: marshallItem(schemaType.members, value) };

		case "Tuple": {
			const items: AttributeValue[] = [];
			for (let i = 0; i < schemaType.members.length; i++) {
				const m = marshallValue(schemaType.members[i], value[i]);
				if (m !== undefined) items.push(m);
			}
			return { L: items };
		}

		case "Collection": {
			if (!Array.isArray(value)) return { NULL: true };
			const items: AttributeValue[] = [];
			for (const item of value) {
				const m = autoMarshallValue(item);
				if (m !== undefined) items.push(m);
			}
			return { L: items };
		}

		case "Hash": {
			if (typeof value !== "object" || value === null) return { NULL: true };
			const map: Record<string, AttributeValue> = {};
			for (const [k, v] of Object.entries(value)) {
				const m = autoMarshallValue(v);
				if (m !== undefined) map[k] = m;
			}
			return { M: map };
		}

		case "Any":
			return autoMarshallValue(value);

		case "Custom":
			return schemaType.marshall(value);
	}
}

export function autoMarshallValue(value: any): AttributeValue | undefined {
	if (value === null || value === undefined) {
		return { NULL: true };
	}

	if (typeof value === "string") {
		return { S: value };
	}

	if (typeof value === "number") {
		return { N: String(value) };
	}

	if (typeof value === "boolean") {
		return { BOOL: value };
	}

	if (value instanceof Date) {
		return { N: String(Math.floor(value.getTime() / 1000)) };
	}

	if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
		return { B: bufferToBase64(value) };
	}

	if (value instanceof Set) {
		if (value.size === 0) return { NULL: true };
		const first = [...value][0];
		if (typeof first === "string") return { SS: [...value] as string[] };
		if (typeof first === "number")
			return { NS: [...value].map(String) as string[] };
		if (first instanceof Uint8Array || Buffer.isBuffer(first))
			return { BS: [...value].map(bufferToBase64) };
		// Fallback: convert to list
		return { L: [...value].map((v) => autoMarshallValue(v)!).filter(Boolean) };
	}

	if (Array.isArray(value)) {
		return {
			L: value.map((v) => autoMarshallValue(v)!).filter(Boolean),
		};
	}

	if (value instanceof Map) {
		const map: Record<string, AttributeValue> = {};
		for (const [k, v] of value) {
			const m = autoMarshallValue(v);
			if (m !== undefined) map[String(k)] = m;
		}
		return { M: map };
	}

	if (typeof value === "object") {
		const map: Record<string, AttributeValue> = {};
		for (const [k, v] of Object.entries(value)) {
			const m = autoMarshallValue(v);
			if (m !== undefined) map[k] = m;
		}
		return { M: map };
	}
	/* c8 ignore start */
	return undefined;
}
/* c8 ignore stop */

function bufferToBase64(value: Uint8Array | Buffer | ArrayBuffer): string {
	if (Buffer.isBuffer(value)) {
		return value.toString("base64");
	}
	if (value instanceof ArrayBuffer) {
		return Buffer.from(value).toString("base64");
	}
	return Buffer.from(value).toString("base64");
}
