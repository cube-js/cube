module.exports = {
  dbType: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers': return 'postgres';
      case 'products': return 'bigquery';
      default: return 'postgres';
    }
  },

  driverFactory: ({ dataSource }) => {
    if (dataSource === 'suppliers') {
      return {
        type: 'postgres',
        host: process.env.CUBEJS_DS_SUPPLIERS_DB_HOST,
        database: process.env.CUBEJS_DS_SUPPLIERS_DB_NAME,
        user: process.env.CUBEJS_DS_SUPPLIERS_DB_USER,
        password: process.env.CUBEJS_DS_SUPPLIERS_DB_PASS,
      };
    }
    if (dataSource === 'products') {
      return {
        type: 'bigquery',
        projectId: process.env.CUBEJS_DS_PRODUCTS_BQ_PROJECT_ID,
        credentials: process.env.CUBEJS_DS_PRODUCTS_BQ_CREDENTIALS,
        exportBucket: process.env.CUBEJS_DS_PRODUCTS_EXPORT_BUCKET,
      };
    }
    throw new Error(`driverFactory: Invalid dataSource '${dataSource}'`);
  },
};
