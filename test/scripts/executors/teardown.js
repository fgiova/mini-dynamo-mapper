module.exports = async () => {
	if (process.env.TEST_LOCAL) {
		return;
	}

	const containerId = process.env.CONTAINER_ID;
	if (containerId) {
		try {
			// Container will be cleaned up by testcontainers
			require("testcontainers");
		} catch {
			// Ignore cleanup errors
		}
	}
};
