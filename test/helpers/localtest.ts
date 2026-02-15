export function getEndpoint(): string {
	return process.env.LOCALSTACK_ENDPOINT ?? "http://localhost:4566";
}

export function isLocalTest(): boolean {
	return !!process.env.TEST_LOCAL || !!process.env.LOCALSTACK_ENDPOINT;
}
