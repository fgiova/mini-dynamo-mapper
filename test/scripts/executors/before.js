const { GenericContainer, Wait } = require("testcontainers");

module.exports = async () => {
	if (process.env.TEST_LOCAL) {
		return;
	}

	const container = await new GenericContainer("localstack/localstack:latest")
		.withExposedPorts(4566)
		.withEnvironment({
			SERVICES: "dynamodb",
			DEFAULT_REGION: "us-east-1",
		})
		.withWaitStrategy(Wait.forLogMessage("Ready."))
		.start();

	const port = container.getMappedPort(4566);
	const host = container.getHost();

	process.env.LOCALSTACK_ENDPOINT = `http://${host}:${port}`;
	process.env.CONTAINER_ID = container.getId();

	// Create test table
	const {
		DynamoDBClient,
		CreateTableCommand,
	} = require("@aws-sdk/client-dynamodb");
	const client = new DynamoDBClient({
		endpoint: process.env.LOCALSTACK_ENDPOINT,
		region: "us-east-1",
		credentials: {
			accessKeyId: "test",
			secretAccessKey: "test",
		},
	});

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

	client.destroy();
};
