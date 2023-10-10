// Cube.js configuration options: https://cube.dev/docs/config

module.exports = {
  contextToAppId: ({ securityContext }) => securityContext.scope || `default`,

  // checkSqlAuth: async (req, username) => {
  //   if (username === 'igor') {
  //     return {
  //       password: 'mypassword',
  //       securityContext: {
  //         scope: 'cube'
  //       },
  //     };
  //   }

  //   throw new Error('Incorrect user name or password');
  // },
};
