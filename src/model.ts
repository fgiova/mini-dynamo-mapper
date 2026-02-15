import type { InferKey, InferSchema, Schema } from "./types/schema";

export interface IndexDefinition {
	type: "global" | "local";
	hashKey: string;
	rangeKey?: string;
}

export interface ModelDefinition<S extends Schema> {
	tableName: string;
	schema: S;
	indexes?: Record<string, IndexDefinition>;
}

export function defineModel<S extends Schema>(
	definition: ModelDefinition<S>,
): ModelDefinition<S> {
	return definition;
}

export type InferModel<M> = M extends ModelDefinition<infer S>
	? InferSchema<S>
	: never;
export type InferModelKey<M> = M extends ModelDefinition<infer S>
	? InferKey<S>
	: never;
