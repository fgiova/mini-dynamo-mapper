import type { ProjectionExpression } from "../types/expressions";

export function projection(...paths: string[]): ProjectionExpression {
	return paths;
}
