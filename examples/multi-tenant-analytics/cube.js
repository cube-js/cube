// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
  queryTransformer: (query, { securityContext }) => {
    const { [process.env.CUBEJS_JWT_CLAIMS_NAMESPACE]: { role } } = securityContext;
    if (role === 'admin') {
      console.log(`User with role "${role}" executed: ${JSON.stringify(query)}`);
    }
    return query;
  },
};