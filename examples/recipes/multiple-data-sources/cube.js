const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  // Provides distinct identifiers for each tenant which are used as caching keys
  // for the data model compilation results
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  // Caching key for database connections, execution queues and pre-aggregation
  // table caches. It must be tenant-specific whenever driverFactory selects a
  // connection based on the security context, otherwise every tenant shares the
  // driver resolved for whichever tenant queried first.
  contextToOrchestratorId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  // Selects the database connection configuration based on the tenant name
  driverFactory: ({ securityContext }) => {

    if (!securityContext.tenant) {
      throw new Error('No tenant found in Security Context!')
    }

    if (securityContext.tenant === 'Avocado Inc') {
      return new PostgresDriver({
        database: 'localDB',
        host: 'postgres',
        user: 'postgres',
        password: 'example',
        port: '5432',
      });
    }

    if (securityContext.tenant === 'Mango Inc') {
      return new PostgresDriver({
        database: 'ecom',
        host: 'demo-db.cube.dev',
        user: 'cube',
        password: '12345',
        port: '5432',
      });
    } 
    
    throw new Error('Unknown tenant in Security Context')
  },
};
