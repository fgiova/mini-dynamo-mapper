import type {
	AttributeValue,
	MiniDynamoClient,
} from "@fgiova/mini-dynamo-client";
import { ExpressionAttributes } from "../expressions/attributes";
import {
	serializeConditionExpression,
	serializeProjectionExpression,
	serializeUpdateExpression,
} from "../expressions/serialize";
import { ParallelScanIterator } from "../iterators/parallel-scan-iterator";
import { QueryIterator } from "../iterators/query-iterator";
import { ScanIterator } from "../iterators/scan-iterator";
import { marshallItem, marshallKey } from "../marshaller/marshall";
import { unmarshallItem } from "../marshaller/unmarshall";
import type { ModelDefinition } from "../model";
import type {
	ConditionExpression,
	UpdateExpression,
} from "../types/expressions";
import type {
	BatchGetOptions,
	BatchWriteOptions,
	DataMapperConfiguration,
	DeleteOptions,
	GetOptions,
	ParallelScanOptions,
	PutOptions,
	QueryOptions,
	ScanOptions,
	TransactGetItem,
	TransactGetOptions,
	TransactWriteItem,
	TransactWriteOptions,
	UpdateOptions,
} from "../types/options";
import type { InferKey, InferSchema } from "../types/schema";
import type { Schema } from "../types/schema-type";
import {
	type BatchWriteRequest,
	executeBatchGet,
	executeBatchWrite,
} from "./batch";
import {
	buildVersionCondition,
	getVersionField,
	incrementVersion,
	mergeVersionCondition,
} from "./version";

export type BatchWriteOperation<S extends Schema> =
	| { type: "put"; item: InferSchema<S> }
	| { type: "delete"; key: InferKey<S> };

export class DataMapper {
	private client: MiniDynamoClient;
	private readConsistency: "eventual" | "strong";
	private skipVersionCheck: boolean;
	private tableNamePrefix: string;

	constructor(config: DataMapperConfiguration) {
		this.client = config.client;
		this.readConsistency = config.readConsistency ?? "eventual";
		this.skipVersionCheck = config.skipVersionCheck ?? false;
		this.tableNamePrefix = config.tableNamePrefix ?? "";
	}

	private getTableName<S extends Schema>(model: ModelDefinition<S>): string {
		return this.tableNamePrefix + model.tableName;
	}

	// === CRUD ===

	async put<S extends Schema>(
		model: ModelDefinition<S>,
		item: InferSchema<S>,
		options?: PutOptions,
	): Promise<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);
		const skipVersion = options?.skipVersionCheck ?? this.skipVersionCheck;

		const processedItem = skipVersion
			? (item as any)
			: incrementVersion(schema, item as any);

		const marshalledItem = marshallItem(schema, processedItem);

		const versionCondition = skipVersion
			? undefined
			: buildVersionCondition(schema, item as any);
		const mergedCondition = mergeVersionCondition(
			options?.condition,
			versionCondition,
		);

		let conditionExpr: string | undefined;
		let exprAttrNames: Record<string, string> | undefined;
		let exprAttrValues: Record<string, AttributeValue> | undefined;

		if (mergedCondition) {
			const attributes = new ExpressionAttributes();
			conditionExpr = serializeConditionExpression(mergedCondition, attributes);
			const names = attributes.names;
			const values = attributes.values;
			exprAttrNames = Object.keys(names).length > 0 ? names : undefined;
			exprAttrValues = Object.keys(values).length > 0 ? values : undefined;
		}

		await this.client.putItem({
			TableName: tableName,
			Item: marshalledItem,
			ConditionExpression: conditionExpr,
			ExpressionAttributeNames: exprAttrNames,
			ExpressionAttributeValues: exprAttrValues,
			ReturnValues: options?.returnValues,
		});

		return processedItem as InferSchema<S>;
	}

	async get<S extends Schema>(
		model: ModelDefinition<S>,
		key: InferKey<S>,
		options?: GetOptions,
	): Promise<InferSchema<S> | undefined> {
		const schema = model.schema;
		const tableName = this.getTableName(model);
		const marshalledKey = marshallKey(schema, key as any);

		let projectionExpr: string | undefined;
		let exprAttrNames: Record<string, string> | undefined;

		if (options?.projection) {
			const attributes = new ExpressionAttributes();
			projectionExpr = serializeProjectionExpression(
				options.projection,
				attributes,
			);
			const names = attributes.names;
			exprAttrNames = Object.keys(names).length > 0 ? names : undefined;
		}

		const result = await this.client.getItem({
			TableName: tableName,
			Key: marshalledKey,
			ConsistentRead:
				options?.consistentRead ?? this.readConsistency === "strong",
			ProjectionExpression: projectionExpr,
			ExpressionAttributeNames: exprAttrNames,
		});

		if (!result.Item) return undefined;
		return unmarshallItem(schema, result.Item) as InferSchema<S>;
	}

	async delete<S extends Schema>(
		model: ModelDefinition<S>,
		key: InferKey<S>,
		options?: DeleteOptions,
	): Promise<InferSchema<S> | undefined> {
		const schema = model.schema;
		const tableName = this.getTableName(model);
		const marshalledKey = marshallKey(schema, key as any);
		const skipVersion = options?.skipVersionCheck ?? this.skipVersionCheck;

		const versionCondition = skipVersion
			? undefined
			: buildVersionCondition(schema, key as any);
		const mergedCondition = mergeVersionCondition(
			options?.condition,
			versionCondition,
		);

		let conditionExpr: string | undefined;
		let exprAttrNames: Record<string, string> | undefined;
		let exprAttrValues: Record<string, AttributeValue> | undefined;

		if (mergedCondition) {
			const attributes = new ExpressionAttributes();
			conditionExpr = serializeConditionExpression(mergedCondition, attributes);
			const names = attributes.names;
			const values = attributes.values;
			exprAttrNames = Object.keys(names).length > 0 ? names : undefined;
			exprAttrValues = Object.keys(values).length > 0 ? values : undefined;
		}

		const result = await this.client.deleteItem({
			TableName: tableName,
			Key: marshalledKey,
			ConditionExpression: conditionExpr,
			ExpressionAttributeNames: exprAttrNames,
			ExpressionAttributeValues: exprAttrValues,
			ReturnValues: options?.returnValues ?? "ALL_OLD",
		});

		if (result.Attributes) {
			return unmarshallItem(schema, result.Attributes) as InferSchema<S>;
		}
		return undefined;
	}

	async update<S extends Schema>(
		model: ModelDefinition<S>,
		key: InferKey<S>,
		updates: UpdateExpression,
		options?: UpdateOptions,
	): Promise<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);
		const marshalledKey = marshallKey(schema, key as any);
		const skipVersion = options?.skipVersionCheck ?? this.skipVersionCheck;

		const attributes = new ExpressionAttributes();

		// Add version increment to the update if needed
		const finalUpdates = { ...updates, actions: [...updates.actions] };
		if (!skipVersion) {
			const versionField = getVersionField(schema);
			if (versionField) {
				const versionPath = versionField.attributeName;
				finalUpdates.actions.push({
					type: "Set",
					path: versionPath,
					value: {
						type: "MathematicalExpression",
						operand1: versionPath,
						operation: "+",
						operand2: 1,
					},
				});
			}
		}

		const updateExpr = serializeUpdateExpression(finalUpdates, attributes);

		const versionCondition = skipVersion
			? undefined
			: buildVersionCondition(schema, key as any);
		const mergedCondition = mergeVersionCondition(
			options?.condition,
			versionCondition,
		);

		let conditionExpr: string | undefined;
		if (mergedCondition) {
			conditionExpr = serializeConditionExpression(mergedCondition, attributes);
		}

		const names = attributes.names;
		const values = attributes.values;

		const result = await this.client.updateItem({
			TableName: tableName,
			Key: marshalledKey,
			UpdateExpression: updateExpr,
			ConditionExpression: conditionExpr,
			ExpressionAttributeNames:
				Object.keys(names).length > 0 ? names : undefined,
			ExpressionAttributeValues:
				Object.keys(values).length > 0 ? values : undefined,
			ReturnValues: options?.returnValues ?? "ALL_NEW",
		});

		return unmarshallItem(schema, result.Attributes!) as InferSchema<S>;
	}

	// === Query/Scan ===

	query<S extends Schema>(
		model: ModelDefinition<S>,
		keyCondition: ConditionExpression,
		options?: QueryOptions,
	): QueryIterator<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		return new QueryIterator<InferSchema<S>>(
			this.client,
			schema,
			tableName,
			keyCondition,
			{
				...options,
				consistentRead:
					options?.consistentRead ?? this.readConsistency === "strong",
			},
		);
	}

	scan<S extends Schema>(
		model: ModelDefinition<S>,
		options?: ScanOptions,
	): ScanIterator<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		return new ScanIterator<InferSchema<S>>(this.client, schema, tableName, {
			...options,
			consistentRead:
				options?.consistentRead ?? this.readConsistency === "strong",
		});
	}

	parallelScan<S extends Schema>(
		model: ModelDefinition<S>,
		segments: number,
		options?: ParallelScanOptions,
	): ParallelScanIterator<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		return new ParallelScanIterator<InferSchema<S>>(
			this.client,
			schema,
			tableName,
			segments,
			{
				...options,
				consistentRead:
					options?.consistentRead ?? this.readConsistency === "strong",
			},
		);
	}

	// === Batch ===

	async *batchGet<S extends Schema>(
		model: ModelDefinition<S>,
		keys: InferKey<S>[],
		options?: BatchGetOptions,
	): AsyncIterableIterator<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		const marshalledKeys = keys.map((key) => marshallKey(schema, key as any));

		let projectionExpr: string | undefined;
		let exprAttrNames: Record<string, string> | undefined;

		if (options?.projection) {
			const attributes = new ExpressionAttributes();
			projectionExpr = serializeProjectionExpression(
				options.projection,
				attributes,
			);
			const names = attributes.names;
			exprAttrNames = Object.keys(names).length > 0 ? names : undefined;
		}

		const result = await executeBatchGet(
			this.client,
			[
				{
					tableName,
					keys: marshalledKeys,
					projection: projectionExpr,
					expressionAttributeNames: exprAttrNames,
					consistentRead:
						options?.consistentRead ?? this.readConsistency === "strong",
				},
			],
			{ returnConsumedCapacity: options?.returnConsumedCapacity },
		);

		const items = result.items[tableName] ?? [];
		for (const rawItem of items) {
			yield unmarshallItem(schema, rawItem) as InferSchema<S>;
		}
	}

	async *batchPut<S extends Schema>(
		model: ModelDefinition<S>,
		items: InferSchema<S>[],
		options?: BatchWriteOptions,
	): AsyncIterableIterator<InferSchema<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		const processedItems = items.map((item) =>
			this.skipVersionCheck
				? (item as any)
				: incrementVersion(schema, item as any),
		);

		const requests: BatchWriteRequest[] = [
			{
				tableName,
				requests: processedItems.map((item) => ({
					type: "put" as const,
					item: marshallItem(schema, item),
				})),
			},
		];

		await executeBatchWrite(this.client, requests, {
			returnConsumedCapacity: options?.returnConsumedCapacity,
		});

		for (const item of processedItems) {
			yield item as InferSchema<S>;
		}
	}

	async *batchDelete<S extends Schema>(
		model: ModelDefinition<S>,
		keys: InferKey<S>[],
		options?: BatchWriteOptions,
	): AsyncIterableIterator<InferKey<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		const requests: BatchWriteRequest[] = [
			{
				tableName,
				requests: keys.map((key) => ({
					type: "delete" as const,
					key: marshallKey(schema, key as any),
				})),
			},
		];

		await executeBatchWrite(this.client, requests, {
			returnConsumedCapacity: options?.returnConsumedCapacity,
		});

		for (const key of keys) {
			yield key;
		}
	}

	async *batchWrite<S extends Schema>(
		model: ModelDefinition<S>,
		operations: BatchWriteOperation<S>[],
		options?: BatchWriteOptions,
	): AsyncIterableIterator<BatchWriteOperation<S>> {
		const schema = model.schema;
		const tableName = this.getTableName(model);

		const requests: BatchWriteRequest[] = [
			{
				tableName,
				requests: operations.map((op) => {
					if (op.type === "put") {
						const processedItem = this.skipVersionCheck
							? (op.item as any)
							: incrementVersion(schema, op.item as any);
						return {
							type: "put" as const,
							item: marshallItem(schema, processedItem),
						};
					}
					return {
						type: "delete" as const,
						key: marshallKey(schema, op.key as any),
					};
				}),
			},
		];

		await executeBatchWrite(this.client, requests, {
			returnConsumedCapacity: options?.returnConsumedCapacity,
		});

		for (const op of operations) {
			yield op;
		}
	}

	// === Transactions ===

	async transactGet(
		items: TransactGetItem<any>[],
		options?: TransactGetOptions,
	): Promise<any[]> {
		const transactItems = items.map((item) => {
			const schema = item.model.schema;
			const tableName = this.getTableName(item.model);
			const marshalledKey = marshallKey(schema, item.key as any);

			const getItem: any = {
				TableName: tableName,
				Key: marshalledKey,
			};

			if (item.projection) {
				const attributes = new ExpressionAttributes();
				getItem.ProjectionExpression = serializeProjectionExpression(
					item.projection,
					attributes,
				);
				const names = attributes.names;
				if (Object.keys(names).length > 0) {
					getItem.ExpressionAttributeNames = names;
				}
			}

			return { Get: getItem };
		});

		const result = await this.client.transactGetItems({
			TransactItems: transactItems,
			ReturnConsumedCapacity: options?.returnConsumedCapacity,
		});

		return (result.Responses ?? []).map((response, index) => {
			if (!response.Item) return undefined;
			const schema = items[index].model.schema;
			return unmarshallItem(schema, response.Item);
		});
	}

	async transactWrite(
		items: TransactWriteItem[],
		options?: TransactWriteOptions,
	): Promise<void> {
		const transactItems = items.map((item) => {
			const schema = item.model.schema;
			const tableName = this.getTableName(item.model);

			switch (item.type) {
				case "Put": {
					const marshalledItem = marshallItem(schema, item.item as any);
					const result: any = {
						Put: {
							TableName: tableName,
							Item: marshalledItem,
						},
					};
					if (item.condition) {
						const attributes = new ExpressionAttributes();
						result.Put.ConditionExpression = serializeConditionExpression(
							item.condition,
							attributes,
						);
						const names = attributes.names;
						const values = attributes.values;
						if (Object.keys(names).length > 0)
							result.Put.ExpressionAttributeNames = names;
						if (Object.keys(values).length > 0)
							result.Put.ExpressionAttributeValues = values;
					}
					return result;
				}

				case "Delete": {
					const marshalledKey = marshallKey(schema, item.key as any);
					const result: any = {
						Delete: {
							TableName: tableName,
							Key: marshalledKey,
						},
					};
					if (item.condition) {
						const attributes = new ExpressionAttributes();
						result.Delete.ConditionExpression = serializeConditionExpression(
							item.condition,
							attributes,
						);
						const names = attributes.names;
						const values = attributes.values;
						if (Object.keys(names).length > 0)
							result.Delete.ExpressionAttributeNames = names;
						if (Object.keys(values).length > 0)
							result.Delete.ExpressionAttributeValues = values;
					}
					return result;
				}

				case "Update": {
					const marshalledKey = marshallKey(schema, item.key as any);
					const attributes = new ExpressionAttributes();
					const updateExpr = serializeUpdateExpression(
						item.updates,
						attributes,
					);
					const result: any = {
						Update: {
							TableName: tableName,
							Key: marshalledKey,
							UpdateExpression: updateExpr,
						},
					};
					if (item.condition) {
						result.Update.ConditionExpression = serializeConditionExpression(
							item.condition,
							attributes,
						);
					}
					const names = attributes.names;
					const values = attributes.values;
					if (Object.keys(names).length > 0)
						result.Update.ExpressionAttributeNames = names;
					if (Object.keys(values).length > 0)
						result.Update.ExpressionAttributeValues = values;
					return result;
				}

				case "ConditionCheck": {
					const marshalledKey = marshallKey(schema, item.key as any);
					const attributes = new ExpressionAttributes();
					const condExpr = serializeConditionExpression(
						item.condition,
						attributes,
					);
					const names = attributes.names;
					const values = attributes.values;
					return {
						ConditionCheck: {
							TableName: tableName,
							Key: marshalledKey,
							ConditionExpression: condExpr,
							...(Object.keys(names).length > 0 && {
								ExpressionAttributeNames: names,
							}),
							...(Object.keys(values).length > 0 && {
								ExpressionAttributeValues: values,
							}),
						},
					};
				}
				default:
					throw new Error(`Unknown transaction type: ${(item as any).type}`);
			}
		});

		await this.client.transactWriteItems({
			TransactItems: transactItems,
			ClientRequestToken: options?.clientRequestToken,
			ReturnConsumedCapacity: options?.returnConsumedCapacity,
			ReturnItemCollectionMetrics: options?.returnItemCollectionMetrics,
		});
	}
}
