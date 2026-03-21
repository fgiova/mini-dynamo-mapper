import {
	BatchWriteItemCommand,
	CreateTableCommand,
	DynamoDBClient,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";

export async function createTestTable(endpoint: string): Promise<void> {
	const client = new DynamoDBClient({
		endpoint,
		region: "us-central-1",
		credentials: {
			accessKeyId: "test",
			secretAccessKey: "test",
		},
	});

	try {
		await client.send(
			new CreateTableCommand({
				TableName: "TestTable",
				KeySchema: [
					{ AttributeName: "pk", KeyType: "HASH" },
					{ AttributeName: "sk", KeyType: "RANGE" },
				],
				AttributeDefinitions: [
					{ AttributeName: "pk", AttributeType: "S" },
					{ AttributeName: "sk", AttributeType: "S" },
					{ AttributeName: "name", AttributeType: "S" },
				],
				GlobalSecondaryIndexes: [
					{
						IndexName: "name-index",
						KeySchema: [{ AttributeName: "name", KeyType: "HASH" }],
						Projection: { ProjectionType: "ALL" },
						ProvisionedThroughput: {
							ReadCapacityUnits: 5,
							WriteCapacityUnits: 5,
						},
					},
				],
				ProvisionedThroughput: {
					ReadCapacityUnits: 5,
					WriteCapacityUnits: 5,
				},
			}),
		);
	} catch (e: any) {
		if (e.name !== "ResourceInUseException") throw e;
	}

	client.destroy();
}

export async function cleanTable(
	endpoint: string,
	tableName: string,
): Promise<void> {
	const client = new DynamoDBClient({
		endpoint,
		region: "eu-central-1",
		credentials: {
			accessKeyId: "test",
			secretAccessKey: "test",
		},
	});

	const scanResult = await client.send(
		new ScanCommand({ TableName: tableName }),
	);

	if (scanResult.Items && scanResult.Items.length > 0) {
		const chunks = [];
		for (let i = 0; i < scanResult.Items.length; i += 25) {
			chunks.push(scanResult.Items.slice(i, i + 25));
		}

		for (const chunk of chunks) {
			await client.send(
				new BatchWriteItemCommand({
					RequestItems: {
						[tableName]: chunk.map((item) => ({
							DeleteRequest: {
								Key: { pk: item.pk, sk: item.sk },
							},
						})),
					},
				}),
			);
		}
	}

	client.destroy();
}
