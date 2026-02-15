// LocalStack runner helper
module.exports = {
	getEndpoint() {
		return process.env.LOCALSTACK_ENDPOINT ?? "http://localhost:4566";
	},
};
