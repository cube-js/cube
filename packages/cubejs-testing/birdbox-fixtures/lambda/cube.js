module.exports = {
  orchestratorOptions: {
    preAggregationsOptions: {
      externalRefresh: false,
    },
  },
  contextToPermissions: async () => new Promise((resolve) => {
    resolve(['liveliness', 'graphql', 'meta', 'data', 'jobs']);
  }),
};
