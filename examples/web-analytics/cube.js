// Server options go here: https://cube.dev/docs/config#options-reference
module.exports = {
  preAggregationsSchema: process.env.PRE_AGGREGATIONS_SCHEMA || 'wa_pre_aggregations',
  orchestratorOptions: {
    rollupOnlyMode: !process.env.CUBEJS_SCHEDULED_REFRESH_TIMER
  }
};
