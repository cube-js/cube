// Cube.js configuration options: https://cube.dev/docs/config

// NOTE: third-party dependencies and the use of require(...) are disabled for
// CubeCloud users by default.  Please contact support if you need them
// enabled for your account.  You are still allowed to require
// @cubejs-backend/*-driver packages.

/**
 * Demo 1
 * Multi-tenancy with Data Sources
*/
// const clickhouseHost     = process.env.CUBEJS_DB_HOST;
// const clickhousePort     = process.env.CUBEJS_DB_PORT;
// const clickhouseDatabase = process.env.CUBEJS_DB_NAME;
// const clickhouseUser     = process.env.CUBEJS_DB_USER;
// const clickhousePassword = process.env.CUBEJS_DB_PASS;
// const mysqlHost          = '<host>';
// const mysqlPort          = '3306';
// const mysqlDatabase      = '<db>';
// const mysqlUser          = '<user>';
// const mysqlPassword      = '<pass>';

module.exports = {
  /**
   * Demo 1
   * Multi-tenancy with Data Sources
   */
  // driverFactory: ({ dataSource }) => {
  //   if (dataSource === 'mysql') {
  //     return {
  //       type: 'mysql',
  //       database: mysqlDatabase,
  //       host: mysqlHost,
  //       user: mysqlUser,
  //       password: mysqlPassword,
  //       port: mysqlPort,
  //     };
  //   } else {
  //     return {
  //       type: 'clickhouse',
  //       database: clickhouseDatabase,
  //       host: clickhouseHost,
  //       user: clickhouseUser,
  //       password: clickhousePassword,
  //       port: clickhousePort,
  //     };
  //   }
  // },


  /**
   * Demo 2
   * Multi-tenancy with Security Context
   */
  // // Provides distinct identifiers for each datasource which are used as caching keys
  // contextToAppId: ({ securityContext }) =>
  //   `CUBEJS_APP_${securityContext.dataSource}`,
  
  // contextToOrchestratorId: ({ securityContext }) =>
  //   `CUBEJS_APP_${securityContext.dataSource}`,

  // // Selects the database connection configuration based on the datasource name
  // driverFactory: ({ securityContext }) => {
  //   if (!securityContext.dataSource) {
  //     throw new Error('No dataSource found in Security Context!');
  //   } 

  //   if (securityContext.dataSource === 'mysql') {
  //     return {
  //       type: 'mysql',
  //       database: mysqlDatabase,
  //       host: mysqlHost,
  //       user: mysqlUser,
  //       password: mysqlPassword,
  //       port: mysqlPort,
  //     };
  //   } else if (securityContext.dataSource === 'clickhouse') {
  //     return {
  //       type: 'clickhouse',
  //       database: clickhouseDatabase,
  //       host: clickhouseHost,
  //       user: clickhouseUser,
  //       password: clickhousePassword,
  //       port: clickhousePort,
  //     };
  //   }

  //   throw new Error('Unknown dataSource in Security Context!');
  // },
};
