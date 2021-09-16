const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  repositoryFactory: ({ securityContext }) =>
    new FileRepository(`schema/${securityContext.tenant}`),
};
