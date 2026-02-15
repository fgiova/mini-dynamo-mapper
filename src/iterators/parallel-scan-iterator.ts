import type { MiniDynamoClient } from "@fgiova/mini-dynamo-client";
import type { ParallelScanOptions } from "../types/options";
import type { Schema } from "../types/schema-type";
import type { PageResult } from "./base-iterator";
import { ScanIterator } from "./scan-iterator";

export class ParallelScanIterator<T> implements AsyncIterableIterator<T> {
	private iterators: ScanIterator<T>[];
	private currentIndex = 0;
	private exhausted: boolean[];

	constructor(
		client: MiniDynamoClient,
		schema: Schema,
		tableName: string,
		segments: number,
		options?: ParallelScanOptions,
	) {
		this.iterators = Array.from(
			{ length: segments },
			(_, i) =>
				new ScanIterator<T>(client, schema, tableName, {
					...options,
					segment: i,
					totalSegments: segments,
				}),
		);
		this.exhausted = new Array(segments).fill(false);
	}

	async next(): Promise<IteratorResult<T>> {
		if (this.exhausted.every(Boolean)) {
			return { value: undefined as any, done: true };
		}

		const startIndex = this.currentIndex;
		do {
			if (!this.exhausted[this.currentIndex]) {
				const result = await this.iterators[this.currentIndex].next();
				if (result.done) {
					this.exhausted[this.currentIndex] = true;
				} else {
					this.currentIndex = (this.currentIndex + 1) % this.iterators.length;
					return result;
				}
			}
			this.currentIndex = (this.currentIndex + 1) % this.iterators.length;
		} while (this.currentIndex !== startIndex);

		return { value: undefined as any, done: true };
	}

	[Symbol.asyncIterator](): AsyncIterableIterator<T> {
		return this;
	}

	pages(): AsyncIterableIterator<PageResult<T>> {
		const iterators = this.iterators;
		const exhausted = new Array(iterators.length).fill(false);
		let currentIndex = 0;

		return {
			async next(): Promise<IteratorResult<PageResult<T>>> {
				if (exhausted.every(Boolean)) {
					return { value: undefined as any, done: true };
				}

				const startIndex = currentIndex;
				do {
					if (!exhausted[currentIndex]) {
						const pageIterator = iterators[currentIndex].pages();
						const result = await pageIterator.next();
						if (result.done) {
							exhausted[currentIndex] = true;
						} else {
							currentIndex = (currentIndex + 1) % iterators.length;
							return result;
						}
					}
					currentIndex = (currentIndex + 1) % iterators.length;
				} while (currentIndex !== startIndex);

				return { value: undefined as any, done: true };
			},
			[Symbol.asyncIterator]() {
				return this;
			},
		};
	}
}
