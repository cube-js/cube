module.exports = {
  contextToPermissions: async (
    securityContext,
    defaultPermissions,
  ) => {
    defaultPermissions.push('jobs');
    return defaultPermissions;
  },
};
