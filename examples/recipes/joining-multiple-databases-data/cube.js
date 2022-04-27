const BigQueryDriver = require('@cubejs-backend/bigquery-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

function bigquery() {
  // CUBEJS_DB_BQ_PROJECT_ID
  // CUBEJS_DB_EXPORT_BUCKET
  // CUBEJS_DB_BQ_CREDENTIALS
  return new BigQueryDriver({})
}

function postgres() {
  // CUBEJS_DB_HOST
  // CUBEJS_DB_NAME
  // CUBEJS_DB_USER
  // CUBEJS_DB_PASS
  return new PostgresDriver({})
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
