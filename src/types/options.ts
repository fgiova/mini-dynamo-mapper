import type { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import type { InferModel, InferModelKey, ModelDefinition } from "../model";
import type {
	ConditionExpression,
	ProjectionExpression,
	UpdateExpression,
} from "./expressions";

export interface DataMapperConfiguration {
	client: MiniDynamoClient;
	readConsistency?: "eventual" | "strong";
	skipVersionCheck?: boolean;
	tableNamePrefix?: string;
}

export interface GetOptions {
	projection?: ProjectionExpression;
	consistentRead?: boolean;
}

export interface PutOptions {
	condition?: ConditionExpression;
	returnValues?: "NONE" | "ALL_OLD";
	skipVersionCheck?: boolean;
}

export interface DeleteOptions {
	condition?: ConditionExpression;
	returnValues?: "NONE" | "ALL_OLD";
	skipVersionCheck?: boolean;
}

export interface UpdateOptions {
	condition?: ConditionExpression;
	returnValues?: "NONE" | "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW";
	skipVersionCheck?: boolean;
	onMissing?: "remove" | "skip";
}

export interface QueryOptions {
	filter?: ConditionExpression;
	projection?: ProjectionExpression;
	indexName?: string;
	scanIndexForward?: boolean;
	consistentRead?: boolean;
	limit?: number;
	startKey?: Record<string, any>;
	select?:
		| "ALL_ATTRIBUTES"
		| "ALL_PROJECTED_ATTRIBUTES"
		| "COUNT"
		| "SPECIFIC_ATTRIBUTES";
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
}

export interface ScanOptions {
	filter?: ConditionExpression;
	projection?: ProjectionExpression;
	indexName?: string;
	consistentRead?: boolean;
	limit?: number;
	startKey?: Record<string, any>;
	segment?: number;
	totalSegments?: number;
	select?:
		| "ALL_ATTRIBUTES"
		| "ALL_PROJECTED_ATTRIBUTES"
		| "COUNT"
		| "SPECIFIC_ATTRIBUTES";
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
}

export interface ParallelScanOptions
	extends Omit<ScanOptions, "segment" | "totalSegments"> {}

export interface BatchGetOptions {
	consistentRead?: boolean;
	projection?: ProjectionExpression;
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
}

export interface BatchWriteOptions {
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
	returnItemCollectionMetrics?: "SIZE" | "NONE";
}

export interface TransactGetOptions {
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
}

export interface TransactWriteOptions {
	clientRequestToken?: string;
	returnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
	returnItemCollectionMetrics?: "SIZE" | "NONE";
}

export interface TransactGetItem<M extends ModelDefinition<any>> {
	model: M;
	key: InferModelKey<M>;
	projection?: ProjectionExpression;
}

export interface TransactPutItem<M extends ModelDefinition<any>> {
	type: "Put";
	model: M;
	item: InferModel<M>;
	condition?: ConditionExpression;
}

export interface TransactDeleteItem<M extends ModelDefinition<any>> {
	type: "Delete";
	model: M;
	key: InferModelKey<M>;
	condition?: ConditionExpression;
}

export interface TransactUpdateItem<M extends ModelDefinition<any>> {
	type: "Update";
	model: M;
	key: InferModelKey<M>;
	updates: UpdateExpression;
	condition?: ConditionExpression;
}

export interface TransactConditionCheck<M extends ModelDefinition<any>> {
	type: "ConditionCheck";
	model: M;
	key: InferModelKey<M>;
	condition: ConditionExpression;
}

export type TransactWriteItem =
	| TransactPutItem<any>
	| TransactDeleteItem<any>
	| TransactUpdateItem<any>
	| TransactConditionCheck<any>;
