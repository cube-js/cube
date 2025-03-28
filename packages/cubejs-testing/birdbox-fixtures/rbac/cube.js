module.exports = {
  contextToRoles: async (context) => context.securityContext.auth?.roles || [],
  canSwitchSqlUser: async () => true,
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
    if (user === 'manager') {
      if (password && password !== 'manager_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'manager',
            userAttributes: {
              region: 'CA',
              city: 'Fresno',
              canHaveAdmin: false,
              minDefaultId: 10000,
            },
            roles: ['manager'],
          },
        },
      };
    }
    if (user === 'default') {
      if (password && password !== 'default_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'default',
            userAttributes: {
              region: 'CA',
              city: 'San Francisco',
              canHaveAdmin: false,
              minDefaultId: 20000,
            },
            roles: [],
          },
        },
      };
    }
    if (user === 'restricted') {
      if (password && password !== 'restricted_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'default',
            userAttributes: {
              region: 'CA',
              city: 'San Francisco',
              canHaveAdmin: true,
              minDefaultId: 20000,
            },
            roles: ['restricted'],
          },
        },
      };
    }
    throw new Error(`User "${user}" doesn't exist`);
  }
};
