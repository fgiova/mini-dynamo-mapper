import type { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import { ExpressionAttributes } from "../expressions/attributes";
import {
	serializeConditionExpression,
	serializeProjectionExpression,
} from "../expressions/serialize";
import { marshallKey } from "../marshaller/marshall";
import type { ScanOptions } from "../types/options";
import type { Schema } from "../types/schema-type";
import { BaseIterator, type RawPageResult } from "./base-iterator";

export class ScanIterator<T> extends BaseIterator<T> {
	private tableName: string;
	private filterExpression?: string;
	private projectionExpression?: string;
	private expressionAttributes: ExpressionAttributes;
	private options: ScanOptions;

	constructor(
		client: MiniDynamoClient,
		schema: Schema,
		tableName: string,
		options?: ScanOptions,
	) {
		super(client, schema);
		this.tableName = tableName;
		/* c8 ignore next -- options is always provided by DataMapper */
		this.options = options ?? {};
		this.expressionAttributes = new ExpressionAttributes();

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

		const result = await this.client.scan({
			TableName: this.tableName,
			FilterExpression: this.filterExpression,
			ProjectionExpression: this.projectionExpression,
			/* c8 ignore next 4 -- names/values may be empty when no filter/projection is set */
			ExpressionAttributeNames:
				Object.keys(names).length > 0 ? names : undefined,
			ExpressionAttributeValues:
				Object.keys(values).length > 0 ? values : undefined,
			ExclusiveStartKey: this.lastEvaluatedKey,
			IndexName: this.options.indexName,
			ConsistentRead: this.options.consistentRead,
			Limit: this.options.limit,
			Segment: this.options.segment,
			TotalSegments: this.options.totalSegments,
			Select: this.options.select,
			ReturnConsumedCapacity: this.options.returnConsumedCapacity,
		});

		/* c8 ignore next 7 -- DynamoDB always returns Items/Count/ScannedCount */
		return {
			items: result.Items ?? [],
			lastEvaluatedKey: result.LastEvaluatedKey,
			count: result.Count ?? 0,
			scannedCount: result.ScannedCount ?? 0,
			consumedCapacity: result.ConsumedCapacity,
		};
	}
}
