const BigQueryDriver = require('@cubejs-backend/bigquery-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

function bigquery() {
  return new BigQueryDriver({
    projectId: 'cube-cloud-staging',
    exportBucket: 'cube-cloud-staging-export-bucket',
    keyFilename: '/Users/cristipp/.gcloud.key',
  })
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
      case 'default': return 'postgres';
      case 'Suppliers': return 'postgres';
      case 'Products': return 'bigquery';
      default: throw new Error('dataSource is undefined');
    }
  },
  driverFactory: ({ dataSource }) => {
    switch (dataSource) {
      case 'Suppliers': return postgres();
      case 'Products': return bigquery();
      default: throw new Error('dataSource is undefined');
    }
  },
};
