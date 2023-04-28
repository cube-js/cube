module.exports = {
  contextToApiScopes: async (
    securityContext,
    defaultPermissions,
  ) => {
    defaultPermissions.push('jobs');
    return defaultPermissions;
  },
};
