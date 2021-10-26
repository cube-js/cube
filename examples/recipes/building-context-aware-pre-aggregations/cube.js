module.exports = {
  // Provides distinct identifiers for each tenant which are used as caching keys
  contextToAppId: ({ securityContext }) => `CUBEJS_APP_${securityContext.env}`,

  // Defines contexts for scheduled pre-aggregation updates
  scheduledRefreshContexts: async () => [
    {
      securityContext: {
        env: 'testing',
      },
    },
    {
      securityContext: {
        env: 'staging',
      },
    },
    {
      securityContext: {
        env: 'production',
      },
    },
  ]
};
