module.exports = {
  orchestratorOptions: {
    queryCacheOptions: {
      externalQueueOptions: {
        concurrency: 20
      }
    },

    preAggregationsOptions: {
      externalRefresh: false,
    },
  }
}