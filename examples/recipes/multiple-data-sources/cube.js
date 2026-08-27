const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  // Provides a distinct identifier for each tenant's compiled data model
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  // Provides a distinct identifier for each tenant's query orchestrator, so
  // that every tenant gets its own database connections and query queues.
  // Without this, all tenants would share the driver created for whichever
  // tenant connected first.
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
