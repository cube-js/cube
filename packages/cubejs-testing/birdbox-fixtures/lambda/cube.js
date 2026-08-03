module.exports = {
  orchestratorOptions: {
    preAggregationsOptions: {
      externalRefresh: false,
    },
  },
  contextToApiScopes: async () => new Promise((resolve) => {
    resolve(['graphql', 'meta', 'data', 'jobs']);
  }),
  driverFactory: ({ dataSource }) => {
    if (dataSource === 'ksql') {
      return {
        type: 'ksql',
        url: process.env.KSQL_URL,
        kafkaHost: process.env.KSQL_KAFKA_HOST,
        kafkaUseSsl: false,
      };
    }

    return { type: 'postgres' };
  }
};
