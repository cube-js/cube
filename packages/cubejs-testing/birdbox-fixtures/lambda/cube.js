module.exports = {
  orchestratorOptions: {
    preAggregationsOptions: {
      externalRefresh: false,
    },
  },
  contextToApiScopes: async () => new Promise((resolve) => {
    resolve(['liveliness', 'graphql', 'meta', 'data', 'jobs']);
  }),
};
