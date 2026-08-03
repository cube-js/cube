module.exports = {
  driverFactory: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers':
        return { type: 'postgres' };
      case 'products':
        return {
          type: 'mysql',
          host: process.env.CUBEJS_DB_HOST2,
          port: process.env.CUBEJS_DB_PORT2,
          database: process.env.CUBEJS_DB_NAME2,
          user: process.env.CUBEJS_DB_USER2,
          password: process.env.CUBEJS_DB_PASS2,
        };
      case 'default':
        return { type: 'postgres' };
      default:
        throw new Error(`driverFactory: Invalid dataSource '${dataSource}'`);
    }
  },
};
