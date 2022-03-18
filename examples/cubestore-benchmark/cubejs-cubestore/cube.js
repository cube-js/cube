module.exports = {
  orchestratorOptions: {
    rollupOnlyMode: !process.env.CUBEJS_SCHEDULED_REFRESH_TIMER,
    queryCacheOptions: {
      externalQueueOptions: {
        concurrency: 20
      }
    },
    preAggregationsOptions: {
      externalRefresh: process.env.CUBEJS_SCHEDULED_REFRESH_TIMER !== 'true'
    }
  }
}