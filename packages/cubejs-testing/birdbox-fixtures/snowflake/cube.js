module.exports = {
  // Labels the statements in Snowflake QUERY_HISTORY, so test load can be told apart in usage stats.
  driverFactory: () => ({
    type: 'snowflake',
    queryTag: { app: 'cube-ci', suite: 'smoke-snowflake' },
  }),
};
