const MySQLDriver = require('@cubejs-backend/mysql-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {

  dbType: ({ dataSource } = {}) => {
    if (dataSource === 'mysql') {
      return 'mysql';
    } else {
      return 'postgres';
    }
  },

    driverFactory: ({ dataSource } = {}) => {
      if (dataSource === 'mysql') {
        return new MySQLDriver({
          database: 'ecom-mysql',
          host: 'mysql',
          user: 'root',
          password: 'example',
          port: '3306',
        });
      } else {
        return new PostgresDriver({
          database: 'ecom-local',
          host: 'host.docker.internal',
          user: 'antonrychkov',
          port: '5432',
        });
      }
    },
  };
  