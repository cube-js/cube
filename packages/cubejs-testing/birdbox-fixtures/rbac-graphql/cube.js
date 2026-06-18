module.exports = {
  contextToRoles: ({ securityContext }) => securityContext?.auth?.roles || ['*'],

  // Same app ID for all tenants so they share a CompilerApi instance
  contextToAppId: () => 'CUBEJS_APP_shared',
};
