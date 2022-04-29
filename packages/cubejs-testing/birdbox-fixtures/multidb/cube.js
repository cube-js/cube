const PostgresDriver = require('@cubejs-backend/postgres-driver');
const MySqlDriver = require("@cubejs-backend/mysql-driver");

module.exports = {
  dbType: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers': return 'postgres';
      case 'products': return 'mysql';
      default: return 'postgres';
    }
  },

  driverFactory: ({ dataSource }) => {
    switch (dataSource) {
      case 'suppliers': return new PostgresDriver({});
      case 'products': return new MySqlDriver({
        host: process.env.CUBEJS_DB_HOST2,
        port: process.env.CUBEJS_DB_PORT2,
        database: process.env.CUBEJS_DB_NAME2,
        user: process.env.CUBEJS_DB_USER2,
        password: process.env.CUBEJS_DB_PASS2,
      });
      default: throw new Error(`driverFactory: Invalid dataSource '${dataSource}'`);
    }
  },
};
