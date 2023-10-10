const jwtDecode = require('jwt-decode');

// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
  contextToAppId: ({ securityContext }) => {
    return `CUBEJS_APP_${securityContext.company}`;
  },
  extendContext: (req) => {
    const { department } = jwtDecode(req.headers['authorization']);
    return {
      permissions: {
        finance: department === 'finance',
      },
    };
  },
};
