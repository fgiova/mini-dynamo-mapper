import type { AttributeValue } from "@fgiova/mini-dynamo-client";

export interface BaseType {
	attributeName?: string;
	defaultProvider?: () => any;
}

export interface KeyableType extends BaseType {
	keyType?: "HASH" | "RANGE";
	indexKeyConfigurations?: Record<string, "HASH" | "RANGE">;
}

export interface StringType extends KeyableType {
	type: "String";
}

export interface NumberType extends KeyableType {
	type: "Number";
	versionAttribute?: boolean;
}

export interface BooleanType extends BaseType {
	type: "Boolean";
}

export interface BinaryType extends KeyableType {
	type: "Binary";
}

export interface DateType extends KeyableType {
	type: "Date";
}

export interface NullType extends BaseType {
	type: "Null";
}

export interface ListType extends BaseType {
	type: "List";
	memberType: SchemaType;
}

export interface MapType extends BaseType {
	type: "Map";
	memberType: SchemaType;
}

export interface SetType extends BaseType {
	type: "Set";
	memberType: "String" | "Number" | "Binary";
}

export interface DocumentType extends BaseType {
	type: "Document";
	members: Schema;
	valueConstructor?: new () => any;
}

export interface TupleType extends BaseType {
	type: "Tuple";
	members: SchemaType[];
}

export interface CollectionType extends BaseType {
	type: "Collection";
	onEmpty?: "nullify" | "leave";
	onInvalid?: "throw" | "omit";
}

export interface HashType extends BaseType {
	type: "Hash";
	onEmpty?: "nullify" | "leave";
	onInvalid?: "throw" | "omit";
}

export interface AnyType extends BaseType {
	type: "Any";
	onEmpty?: "nullify" | "leave";
	onInvalid?: "throw" | "omit";
	unwrapNumbers?: boolean;
}

export interface CustomType<T = any> extends BaseType {
	type: "Custom";
	marshall: (value: T) => AttributeValue;
	unmarshall: (value: AttributeValue) => T;
	attributeType?: "S" | "N" | "B";
}

export type SchemaType =
	| StringType
	| NumberType
	| BooleanType
	| BinaryType
	| DateType
	| NullType
	| ListType
	| MapType
	| SetType
	| DocumentType
	| TupleType
	| CollectionType
	| HashType
	| AnyType
	| CustomType<any>;

export interface Schema {
	[key: string]: SchemaType;
}
