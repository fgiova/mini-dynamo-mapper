import type { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { ExpressionAttributes } from "../expressions/attributes";
import {
	serializeConditionExpression,
	serializeProjectionExpression,
} from "../expressions/serialize";
import { marshallKey } from "../marshaller/marshall";
import type { ConditionExpression } from "../types/expressions";
import type { QueryOptions } from "../types/options";
import type { Schema } from "../types/schema-type";
import { BaseIterator, type RawPageResult } from "./base-iterator";

export class QueryIterator<T> extends BaseIterator<T> {
	private tableName: string;
	private keyConditionExpression: string;
	private filterExpression?: string;
	private projectionExpression?: string;
	private expressionAttributes: ExpressionAttributes;
	private options: QueryOptions;

	constructor(
		client: MiniDynamoClient,
		schema: Schema,
		tableName: string,
		keyCondition: ConditionExpression,
		options?: QueryOptions,
	) {
		super(client, schema);
		this.tableName = tableName;
		this.options = options ?? {};
		this.expressionAttributes = new ExpressionAttributes();

		this.keyConditionExpression = serializeConditionExpression(
			keyCondition,
			this.expressionAttributes,
		);

		if (options?.filter) {
			this.filterExpression = serializeConditionExpression(
				options.filter,
				this.expressionAttributes,
			);
		}

		if (options?.projection) {
			this.projectionExpression = serializeProjectionExpression(
				options.projection,
				this.expressionAttributes,
			);
		}

		if (options?.startKey) {
			this.lastEvaluatedKey = marshallKey(schema, options.startKey);
		}
	}

	protected async fetchNextPage(): Promise<RawPageResult> {
		const names = this.expressionAttributes.names;
		const values = this.expressionAttributes.values;

		const result = await this.client.query({
			TableName: this.tableName,
			KeyConditionExpression: this.keyConditionExpression,
			FilterExpression: this.filterExpression,
			ProjectionExpression: this.projectionExpression,
			ExpressionAttributeNames:
				Object.keys(names).length > 0 ? names : undefined,
			ExpressionAttributeValues:
				Object.keys(values).length > 0 ? values : undefined,
			ExclusiveStartKey: this.lastEvaluatedKey,
			IndexName: this.options.indexName,
			ScanIndexForward: this.options.scanIndexForward,
			ConsistentRead: this.options.consistentRead,
			Limit: this.options.limit,
			Select: this.options.select,
			ReturnConsumedCapacity: this.options.returnConsumedCapacity,
		});

		return {
			items: result.Items ?? [],
			lastEvaluatedKey: result.LastEvaluatedKey,
			count: result.Count ?? 0,
			scannedCount: result.ScannedCount ?? 0,
			consumedCapacity: result.ConsumedCapacity,
		};
	}
}
