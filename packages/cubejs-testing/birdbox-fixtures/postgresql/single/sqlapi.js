// Cube.js configuration options: https://cube.dev/docs/config
// It's a special configuration file for SQL API smoke's testing
module.exports = {
  queryRewrite: (query, { securityContext }) => {
    if (!securityContext.user) {
      throw new Error('Property user does not exist in in Security Context!');
    }

    console.log('queryRewrite', {
      securityContext
    });

    return query;
  },
  checkSqlAuth: async (req, user) => {
    if (user === 'admin') {
      return {
        password: 'admin_password',
        superuser: true,
        securityContext: {
          user: 'admin'
        },
      };
    }

    if (user === 'moderator') {
      return {
        password: 'moderator_password',
        securityContext: {
          user: 'moderator'
        },
      };
    }

    if (user === 'usr1') {
      return {
        password: 'user1_password',
        securityContext: {
          user: 'usr1'
        },
      };
    }

    if (user === 'usr2') {
      return {
        password: 'ignore password',
        securityContext: {
          user: 'usr2'
        },
      };
    }

    throw new Error(`User "${user}" doesn't exist`);
  },
  // ADMIN is allowed to access with superuser: true
  // moderator is allowed to access -> user1/usr2
  // usr1/usr2 are not allowed to change
  canSwitchSqlUser: async (current, user) => {
    await new Promise((resolve) => {
      setTimeout(resolve, 1000);
    });

    if (current === 'moderator') {
      return user === 'usr1';
    }

    return false;
  }
};
