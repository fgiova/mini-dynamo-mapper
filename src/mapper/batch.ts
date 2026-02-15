import type {
	AttributeValue,
	MiniDynamoClient,
} from "@fgiova/mini-dynamo-client";

export interface BatchWriteRequest {
	tableName: string;
	requests: Array<
		| { type: "put"; item: Record<string, AttributeValue> }
		| { type: "delete"; key: Record<string, AttributeValue> }
	>;
}

export interface BatchGetRequest {
	tableName: string;
	keys: Array<Record<string, AttributeValue>>;
	projection?: string;
	expressionAttributeNames?: Record<string, string>;
	consistentRead?: boolean;
}

export async function executeBatchWrite(
	client: MiniDynamoClient,
	requests: BatchWriteRequest[],
	options?: { maxRetries?: number; returnConsumedCapacity?: string },
): Promise<{ consumedCapacity?: any[] }> {
	const maxRetries = options?.maxRetries ?? 10;
	const allCapacity: any[] = [];

	const requestItems: Record<string, any[]> = {};
	for (const req of requests) {
		if (!requestItems[req.tableName]) requestItems[req.tableName] = [];
		for (const r of req.requests) {
			if (r.type === "put") {
				requestItems[req.tableName].push({ PutRequest: { Item: r.item } });
			} else {
				requestItems[req.tableName].push({ DeleteRequest: { Key: r.key } });
			}
		}
	}

	let unprocessed: Record<string, any[]> | undefined = requestItems;
	let attempt = 0;

	while (unprocessed && Object.keys(unprocessed).length > 0) {
		if (attempt >= maxRetries) {
			throw new Error(
				`BatchWrite failed after ${maxRetries} retries with unprocessed items`,
			);
		}

		if (attempt > 0) {
			await sleep(getBackoffDelay(attempt));
		}

		const result = await client.batchWriteItem({
			RequestItems: unprocessed,
			ReturnConsumedCapacity: options?.returnConsumedCapacity as any,
		});

		if (result.ConsumedCapacity) {
			allCapacity.push(...result.ConsumedCapacity);
		}

		unprocessed = result.UnprocessedItems;
		attempt++;
	}

	return { consumedCapacity: allCapacity.length > 0 ? allCapacity : undefined };
}

export async function executeBatchGet(
	client: MiniDynamoClient,
	requests: BatchGetRequest[],
	options?: { maxRetries?: number; returnConsumedCapacity?: string },
): Promise<{
	items: Record<string, Record<string, AttributeValue>[]>;
	consumedCapacity?: any[];
}> {
	const maxRetries = options?.maxRetries ?? 10;
	const allItems: Record<string, Record<string, AttributeValue>[]> = {};
	const allCapacity: any[] = [];

	const requestItems: Record<string, any> = {};
	for (const req of requests) {
		requestItems[req.tableName] = {
			Keys: req.keys,
			...(req.projection && { ProjectionExpression: req.projection }),
			...(req.expressionAttributeNames && {
				ExpressionAttributeNames: req.expressionAttributeNames,
			}),
			...(req.consistentRead !== undefined && {
				ConsistentRead: req.consistentRead,
			}),
		};
	}

	let unprocessed: Record<string, any> | undefined = requestItems;
	let attempt = 0;

	while (unprocessed && Object.keys(unprocessed).length > 0) {
		if (attempt >= maxRetries) {
			throw new Error(
				`BatchGet failed after ${maxRetries} retries with unprocessed keys`,
			);
		}

		if (attempt > 0) {
			await sleep(getBackoffDelay(attempt));
		}

		const result = await client.batchGetItem({
			RequestItems: unprocessed,
			ReturnConsumedCapacity: options?.returnConsumedCapacity as any,
		});

		if (result.Responses) {
			for (const [table, items] of Object.entries(result.Responses)) {
				if (!allItems[table]) allItems[table] = [];
				allItems[table].push(...items);
			}
		}

		if (result.ConsumedCapacity) {
			allCapacity.push(...result.ConsumedCapacity);
		}

		unprocessed = result.UnprocessedKeys;
		attempt++;
	}

	return {
		items: allItems,
		consumedCapacity: allCapacity.length > 0 ? allCapacity : undefined,
	};
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBackoffDelay(attempt: number, baseDelay = 50): number {
	return Math.min(2 ** attempt * baseDelay, 30000);
}
