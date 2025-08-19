// Example configuration using group-based access control
module.exports = {
  // Use contextToGroups for group-based access control
  // Note: Cannot be used together with contextToRoles
  contextToGroups: async (context) => context.securityContext.auth?.groups || [],
  
  canSwitchSqlUser: async () => true,
  
  checkSqlAuth: async (req, user, password) => {
    if (user === 'analyst') {
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'analyst',
            groups: ['analytics', 'reporting'],
          },
        },
      };
    }
    if (user === 'manager') {
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'manager',
            groups: ['management', 'hr'],
          },
        },
      };
    }
    if (user === 'finance') {
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'finance',
            groups: ['finance', 'accounting'],
          },
        },
      };
    }
    throw new Error(`User "${user}" doesn't exist`);
  }
};