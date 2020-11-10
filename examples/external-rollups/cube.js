module.exports = {
  preAggregationsSchema: () => process.env.CUBEJS_PREAGGREGATIONS_SCHEMA || 'stb_pre_aggregations',
    externalDbType: 'mysql',
  externalDriverFactory: () => new MySQLDriver({
  host: process.env.CUBEJS_EXT_DB_HOST,
  database: process.env.CUBEJS_EXT_DB_NAME,
  user: process.env.CUBEJS_EXT_DB_USER,
  password: process.env.CUBEJS_EXT_DB_PASS.toString()
})};
