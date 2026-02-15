// === Core ===

// === Expression Attributes (advanced usage) ===
export { ExpressionAttributes } from "./expressions/attributes";
// === Expression Builders ===
export {
	and,
	attributeExists,
	attributeNotExists,
	attributeType,
	beginsWith,
	between,
	contains,
	equals,
	greaterThan,
	greaterThanOrEqual,
	inList,
	lessThan,
	lessThanOrEqual,
	not,
	notEquals,
	or,
} from "./expressions/condition";
export { projection } from "./expressions/projection";
export {
	serializeConditionExpression,
	serializeProjectionExpression,
	serializeUpdateExpression,
} from "./expressions/serialize";
export {
	add,
	decrement,
	deleteFromSet,
	ifNotExists,
	increment,
	listAppend,
	listPrepend,
	remove,
	set,
	updateExpression,
} from "./expressions/update";
export type { PageResult } from "./iterators/base-iterator";
export { ParallelScanIterator } from "./iterators/parallel-scan-iterator";
// === Iterators ===
export { QueryIterator } from "./iterators/query-iterator";
export { ScanIterator } from "./iterators/scan-iterator";
export { type BatchWriteOperation, DataMapper } from "./mapper/data-mapper";

// === Marshaller (advanced usage) ===
export {
	autoMarshallValue,
	marshallItem,
	marshallKey,
	marshallValue,
} from "./marshaller/marshall";
export { unmarshallItem, unmarshallValue } from "./marshaller/unmarshall";
export type {
	IndexDefinition,
	InferModel,
	InferModelKey,
	ModelDefinition,
} from "./model";
export { defineModel } from "./model";
// === Types - Expressions ===
export type {
	AddAction,
	AndExpression,
	AttributeExistsPredicate,
	AttributeNotExistsPredicate,
	AttributePath,
	AttributeTypePredicate,
	BeginsWithPredicate,
	BetweenPredicate,
	ConditionExpression,
	ConditionPredicate,
	ContainsPredicate,
	DeleteAction,
	EqualsPredicate,
	FunctionExpression,
	GreaterThanOrEqualPredicate,
	GreaterThanPredicate,
	InListPredicate,
	LessThanOrEqualPredicate,
	LessThanPredicate,
	MathematicalExpression,
	NotEqualsPredicate,
	NotExpression,
	OrExpression,
	ProjectionExpression,
	RemoveAction,
	SerializedExpression,
	SetAction,
	SimpleConditionExpression,
	UpdateAction,
	UpdateExpression,
} from "./types/expressions";
// === Types - Options ===
export type {
	BatchGetOptions,
	BatchWriteOptions,
	DataMapperConfiguration,
	DeleteOptions,
	GetOptions,
	ParallelScanOptions,
	PutOptions,
	QueryOptions,
	ScanOptions,
	TransactConditionCheck,
	TransactDeleteItem,
	TransactGetItem,
	TransactGetOptions,
	TransactPutItem,
	TransactUpdateItem,
	TransactWriteItem,
	TransactWriteOptions,
	UpdateOptions,
} from "./types/options";
// === Types - Model ===
export type {
	InferKey,
	InferSchema,
	InferSchemaType,
	InferTuple,
} from "./types/schema";
// === Types - Schema ===
export type {
	AnyType,
	BaseType,
	BinaryType,
	BooleanType,
	CollectionType,
	CustomType,
	DateType,
	DocumentType,
	HashType,
	KeyableType,
	ListType,
	MapType,
	NullType,
	NumberType,
	Schema,
	SchemaType,
	SetType,
	StringType,
	TupleType,
} from "./types/schema-type";
