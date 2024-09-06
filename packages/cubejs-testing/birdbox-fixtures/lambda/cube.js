const PostgresDriver = require("@cubejs-backend/postgres-driver");
const KsqlDriver = require("@cubejs-backend/ksql-driver");

module.exports = {
  orchestratorOptions: {
    preAggregationsOptions: {
      externalRefresh: false,
    },
  },
  contextToApiScopes: async () => new Promise((resolve) => {
    resolve(['graphql', 'meta', 'data', 'jobs']);
  }),
  dbType: ({ dataSource }) => {
    if (dataSource === 'default') {
      return 'postgres';
    }

    return dataSource || 'postgres';
  },
  driverFactory: async ({ dataSource }) => {
    if (dataSource === "ksql") {
      return new KsqlDriver({
        url: process.env.KSQL_URL,
        kafkaHost: process.env.KSQL_KAFKA_HOST,
        kafkaUseSsl: false,
      });
    }

    return new PostgresDriver();
  }
};
