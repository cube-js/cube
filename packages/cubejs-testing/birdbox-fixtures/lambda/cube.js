module.exports = {
  orchestratorOptions: {
    preAggregationsOptions: {
      externalRefresh: false,
    },
  },
  contextToApiScopes: async () => new Promise((resolve) => {
    resolve(['graphql', 'meta', 'data', 'jobs']);
  }),
};
