const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  // Provides distinct identifiers for each tenant which are used as caching keys
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.env}`,

  // Selects the database connection configuration based on the tenant name
  driverFactory: ({ securityContext }) => {
    if (!securityContext.env) {
      throw new Error('No env found in Security Context!')
    } else {
      return new PostgresDriver({
        database: 'localDB',
        host: 'postgres',
        user: 'postgres',
        password: 'example',
        port: '5432',
      });
    }
  },
};
