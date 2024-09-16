// Cube.js configuration options: https://cube.dev/docs/config

module.exports = {
    processSubscriptionsInterval: 1000,
    orchestratorOptions: {
      queryCacheOptions: {
        refreshKeyRenewalThreshold: 1,
      }
    }
  };