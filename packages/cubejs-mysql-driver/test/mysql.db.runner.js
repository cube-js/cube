const { GenericContainer } = require('testcontainers');
const MySqlDriver = require('../driver/MySqlDriver');

const version = process.env.TEST_MYSQL_VERSION || '5.7';

const startContainer = async () => {
  const builder = new GenericContainer(`mysql:${version}`)
    .withEnv('MYSQL_ROOT_PASSWORD', process.env.TEST_DB_PASSWORD || 'Test1test')
    .withExposedPorts(3306);

  if (version.split('.')[0] === '8') {
    /**
     * workaround for MySQL 8 and unsupported auth in mysql package
     * @link https://github.com/mysqljs/mysql/pull/2233
     */
    builder.withCmd('--default-authentication-plugin=mysql_native_password');
  }

  return builder.start();
};

const createDriver = (c) => new MySqlDriver({
  host: c.getHost(),
  user: 'root',
  password: process.env.TEST_DB_PASSWORD || 'Test1test',
  port: c.getMappedPort(3306),
  database: 'mysql',
});

module.exports = {
  startContainer,
  createDriver,
};
