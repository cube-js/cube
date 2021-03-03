const ClickhouseDriver = require('@cubejs-backend/clickhouse-driver');

// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
  driverFactory: () => {
    return new ClickhouseDriver({
      auth: 'default:',
    });
  },
};
