import type {
	AttributeValue,
	MiniDynamoClient,
} from "@fgiova/mini-dynamo-client";
import { unmarshallItem } from "../marshaller/unmarshall";
import type { Schema } from "../types/schema-type";

export interface PageResult<T> {
	items: T[];
	lastEvaluatedKey?: Record<string, AttributeValue>;
	count: number;
	scannedCount: number;
	consumedCapacity?: any;
}

export interface RawPageResult {
	items: Record<string, AttributeValue>[];
	lastEvaluatedKey?: Record<string, AttributeValue>;
	count: number;
	scannedCount: number;
	consumedCapacity?: any;
}

export abstract class BaseIterator<T> implements AsyncIterableIterator<T> {
	protected client: MiniDynamoClient;
	protected schema: Schema;
	protected buffer: T[] = [];
	protected lastEvaluatedKey?: Record<string, AttributeValue>;
	protected done = false;
	private _count = 0;
	private _scannedCount = 0;
	private pagesMode = false;

	constructor(client: MiniDynamoClient, schema: Schema) {
		this.client = client;
		this.schema = schema;
	}

	async next(): Promise<IteratorResult<T>> {
		if (this.pagesMode) {
			return { value: undefined as any, done: true };
		}

		if (this.buffer.length > 0) {
			return { value: this.buffer.shift()!, done: false };
		}

		if (this.done) {
			return { value: undefined as any, done: true };
		}

		const rawPage = await this.fetchNextPage();
		const items = rawPage.items.map(
			(item) => unmarshallItem(this.schema, item) as T,
		);
		this._count += rawPage.count;
		this._scannedCount += rawPage.scannedCount;

		if (!rawPage.lastEvaluatedKey) {
			this.done = true;
		} else {
			this.lastEvaluatedKey = rawPage.lastEvaluatedKey;
		}

		this.buffer = items;

		if (this.buffer.length > 0) {
			return { value: this.buffer.shift()!, done: false };
		}

		return { value: undefined as any, done: true };
	}

	[Symbol.asyncIterator](): AsyncIterableIterator<T> {
		return this;
	}

	pages(): AsyncIterableIterator<PageResult<T>> {
		this.pagesMode = true;
		const self = this;

		return {
			async next(): Promise<IteratorResult<PageResult<T>>> {
				if (self.done) {
					return { value: undefined as any, done: true };
				}

				const rawPage = await self.fetchNextPage();
				const items = rawPage.items.map(
					(item) => unmarshallItem(self.schema, item) as T,
				);
				self._count += rawPage.count;
				self._scannedCount += rawPage.scannedCount;

				if (!rawPage.lastEvaluatedKey) {
					self.done = true;
				/* c8 ignore next 3 */
			} else {
					self.lastEvaluatedKey = rawPage.lastEvaluatedKey;
				}

				return {
					value: {
						items,
						lastEvaluatedKey: rawPage.lastEvaluatedKey,
						count: rawPage.count,
						scannedCount: rawPage.scannedCount,
						consumedCapacity: rawPage.consumedCapacity,
					},
					done: false,
				};
			},
			[Symbol.asyncIterator]() {
				return this;
			},
		};
	}

	get count(): number {
		return this._count;
	}

	get scannedCount(): number {
		return this._scannedCount;
	}

	protected abstract fetchNextPage(): Promise<RawPageResult>;
}
