module.exports = {
  contextToRoles: async (context) => context.securityContext.auth?.roles || [],
  checkSqlAuth: async (req, user, password) => {
    if (user === 'admin') {
      if (password && password !== 'admin_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: true,
        securityContext: {
          auth: {
            username: 'admin',
            userAttributes: {
              region: 'CA',
              city: 'Fresno',
              canHaveAdmin: true,
              minDefaultId: 10000,
            },
            roles: ['admin', 'ownder', 'hr'],
          },
        },
      };
    }
    throw new Error(`User "${user}" doesn't exist`);
  }
};
