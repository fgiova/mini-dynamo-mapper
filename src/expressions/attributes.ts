import type { AttributeValue } from "@fgiova/mini-dynamo-client";

export class ExpressionAttributes {
	private nameCounter = 0;
	private valueCounter = 0;
	private namesMap = new Map<string, string>();
	private reverseNamesMap = new Map<string, string>();
	private valuesMap = new Map<string, AttributeValue>();

	addName(path: string): string {
		const segments = parsePath(path);
		const placeholderSegments: string[] = [];

		for (const segment of segments) {
			if (segment.startsWith("[")) {
				placeholderSegments.push(segment);
			} else {
				if (this.reverseNamesMap.has(segment)) {
					placeholderSegments.push(this.reverseNamesMap.get(segment) as string);
				} else {
					const placeholder = `#attr${this.nameCounter++}`;
					this.namesMap.set(placeholder, segment);
					this.reverseNamesMap.set(segment, placeholder);
					placeholderSegments.push(placeholder);
				}
			}
		}

		return placeholderSegments.join(".");
	}

	addValue(value: AttributeValue): string {
		const placeholder = `:val${this.valueCounter++}`;
		this.valuesMap.set(placeholder, value);
		return placeholder;
	}

	get names(): Record<string, string> {
		const result: Record<string, string> = {};
		for (const [k, v] of this.namesMap) {
			result[k] = v;
		}
		return result;
	}

	get values(): Record<string, AttributeValue> {
		const result: Record<string, AttributeValue> = {};
		for (const [k, v] of this.valuesMap) {
			result[k] = v;
		}
		return result;
	}
}

function parsePath(path: string): string[] {
	const segments: string[] = [];
	let current = "";

	for (let i = 0; i < path.length; i++) {
		const ch = path[i];
		if (ch === ".") {
			if (current) segments.push(current);
			current = "";
		} else if (ch === "[") {
			if (current) segments.push(current);
			current = "";
			let bracket = "[";
			i++;
			while (i < path.length && path[i] !== "]") {
				bracket += path[i];
				i++;
			}
			bracket += "]";
			segments.push(bracket);
		} else {
			current += ch;
		}
	}

	if (current) segments.push(current);
	return segments;
}
