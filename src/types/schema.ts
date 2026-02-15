import type { Schema, SchemaType } from "./schema-type";

export type { Schema };

// Infer a single SchemaType to its TypeScript type
export type InferSchemaType<T extends SchemaType> = T extends {
	type: "String";
}
	? string
	: T extends { type: "Number" }
		? number
		: T extends { type: "Boolean" }
			? boolean
			: T extends { type: "Binary" }
				? Uint8Array
				: T extends { type: "Date" }
					? Date
					: T extends { type: "Null" }
						? null
						: T extends { type: "Set"; memberType: "String" }
							? Set<string>
							: T extends { type: "Set"; memberType: "Number" }
								? Set<number>
								: T extends { type: "Set"; memberType: "Binary" }
									? Set<Uint8Array>
									: T extends { type: "List"; memberType: infer M }
										? M extends SchemaType
											? Array<InferSchemaType<M>>
											: never
										: T extends { type: "Map"; memberType: infer M }
											? M extends SchemaType
												? Map<string, InferSchemaType<M>>
												: never
											: T extends {
														type: "Document";
														members: infer M;
													}
												? M extends Schema
													? InferSchema<M>
													: never
												: T extends {
															type: "Tuple";
															members: infer M;
														}
													? M extends SchemaType[]
														? InferTuple<M>
														: never
													: T extends { type: "Collection" }
														? Array<any>
														: T extends { type: "Hash" }
															? Record<string, any>
															: T extends { type: "Any" }
																? any
																: T extends {
																			type: "Custom";
																			unmarshall: (value: any) => infer R;
																		}
																	? R
																	: never;

// Infer an entire Schema to an object type
export type InferSchema<S extends Schema> = {
	[K in keyof S]: InferSchemaType<S[K]>;
};

// Infer a tuple type
export type InferTuple<T extends SchemaType[]> = {
	[K in keyof T]: T[K] extends SchemaType ? InferSchemaType<T[K]> : never;
};

// Extract only key fields from schema
export type InferKey<S extends Schema> = {
	[K in keyof S as S[K] extends { keyType: "HASH" | "RANGE" }
		? K
		: never]: InferSchemaType<S[K]>;
};
