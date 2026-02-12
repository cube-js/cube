module.exports = {
  contextToRoles: async (context) => context.securityContext.auth?.roles || [],
  contextToGroups: async (context) => context.securityContext.auth?.groups || [],
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
            groups: ['leadership', 'hr'],
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
            groups: ['management'],
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
            groups: ['general'],
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
            groups: ['restricted'],
          },
        },
      };
    }
    // Developer user for testing overlapping policies scenario
    // where group "*" has empty member includes and "developer" has row filter
    if (user === 'developer') {
      if (password && password !== 'developer_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'developer',
            userAttributes: {
              region: 'CA',
              allowedCities: ['Los Angeles', 'New York'],
            },
            roles: [],
            groups: ['developer'],
          },
        },
      };
    }
    // User for testing two-dimensional policy overlap (matches diagram in CompilerApi.ts)
    // Has policy2_role, so both Policy 1 (*) and Policy 2 (policy2_role) apply
    if (user === 'policy_test') {
      if (password && password !== 'policy_test_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'policy_test',
            userAttributes: {},
            roles: ['policy2_role'],
            groups: [],
          },
        },
      };
    }
    throw new Error(`User "${user}" doesn't exist`);
  }
};
