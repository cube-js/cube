// const PostgresDriver = require('@cubejs-backend/postgres-driver');
module.exports = {
  // driverFactory: async () => new PostgresDriver({
  //   user: 'postgres',
  //   password: '123-PgSql-PassworD-321',
  // }),
  contextToAppId: ({ securityContext }) => (
    `CUBEJS_APP_${securityContext.tenant}`
  ),
  orchestratorOptions: {
    // Query cache options for DB queries:
    // queryCacheOptions: {},

    // Query cache options for pre-aggregations:
    preAggregationsOptions: {
      maxPartitions: 36,
      queueOptions: {
        concurrency: 1,
      },
    }
  }
};
