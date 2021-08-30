const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
    contextToAppId: ({ securityContext }) => 
      `CUBEJS_APP_${securityContext.tenant}`,

    driverFactory: ({ securityContext } = {}) => {
      if (securityContext.tenant === 'cubeDev') {
        return new PostgresDriver({
          database: 'cubeDev',
          host: 'postgres',
          user: 'postgres',
          password: 'example',
          port: '5432',
        });
      } else {
        return new PostgresDriver({
          database: 'ecom',
          host: 'demo-db.cube.dev',
          user: 'cube',
          password: '12345',
          port: '5432',
        });
      }
    },
  };
  