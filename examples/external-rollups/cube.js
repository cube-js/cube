const BigqueryDriver = require('@cubejs-backend/bigquery-driver');
module.exports = {
  driverFactory: () => {
    const driver = new BigqueryDriver();
    driver.readOnly = () => true;
    return driver;
  },
  preAggregationsSchema: process.env.PRE_AGGREGATIONS_SCHEMA || 'stb_pre_aggregations',
};
