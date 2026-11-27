// Labels the statements in Snowflake QUERY_HISTORY, so test load can be told apart in usage stats.
const driverOptions = {
  snowflake: { queryTag: { app: 'cube-ci', suite: 'testing-drivers' } },
};

const dbType = process.env.CUBEJS_DB_TYPE;

module.exports = {
  contextToApiScopes: async (
    securityContext,
    defaultPermissions,
  ) => {
    defaultPermissions.push('jobs');
    return defaultPermissions;
  },
  ...(driverOptions[dbType] ? {
    driverFactory: () => ({ type: dbType, ...driverOptions[dbType] }),
  } : {}),
};
