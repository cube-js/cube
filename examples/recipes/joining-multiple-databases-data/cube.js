const BigQueryDriver = require('@cubejs-backend/bigquery-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

function bigquery() {
  // CUBEJS_DB_BQ_PROJECT_ID
  // CUBEJS_DB_EXPORT_BUCKET
  // CUBEJS_DB_BQ_CREDENTIALS
  return new BigQueryDriver({})
}

function postgres() {
  return new PostgresDriver({
    database: 'ecom',
    host: 'localhost',
    user: 'test',
    password: 'test',
    port: '5432',
  })
}

module.exports = {
  dbType: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers': return 'postgres';
      case 'products': return 'bigquery';
      default: return 'postgres';
    }
  },

  driverFactory: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers': return postgres();
      case 'products': return bigquery();
      default: throw new Error(`driverFactory: Invalid dataSource '${dataSource}'`);
    }
  },
};
