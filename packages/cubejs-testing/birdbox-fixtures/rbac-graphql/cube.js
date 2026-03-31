// Cube.js configuration for testing GraphQL schema caching with different security contexts
module.exports = {
  // Map security context to RBAC roles
  contextToRoles: ({ securityContext }) => securityContext?.auth?.roles || ['*'],

  // SAME app ID for all tenants - this forces them to share a CompilerApi instance
  contextToAppId: () => 'CUBEJS_APP_shared',
};
