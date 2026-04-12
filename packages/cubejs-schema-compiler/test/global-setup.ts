export default () => {
  process.env.TZ = 'UTC';
  process.env.CUBEJS_DB_MYSQL_USE_NAMED_TIMEZONES = 'true';
  process.env.CUBEJS_DB_MSSQL_USE_NAMED_TIMEZONES = 'true';
};
