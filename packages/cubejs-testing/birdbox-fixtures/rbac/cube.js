module.exports = {
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
            groups: ['leadership', 'hr', 'admin'],
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
            groups: ['management', 'manager'],
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
            groups: ['developer'],
          },
        },
      };
    }
    // User for testing two-dimensional policy overlap (matches diagram in CompilerApi.ts)
    // Has policy2_group, so both Policy 1 (*) and Policy 2 (policy2_group) apply
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
            groups: ['policy2_group'],
          },
        },
      };
    }
    // User for masking tests - no special groups, sees only masked values
    if (user === 'masking_viewer') {
      if (password && password !== 'masking_viewer_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'masking_viewer',
            userAttributes: {},
            groups: [],
          },
        },
      };
    }
    // User for masking tests - has full access group
    if (user === 'masking_full') {
      if (password && password !== 'masking_full_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'masking_full',
            userAttributes: {},
            groups: ['masking_full_access'],
          },
        },
      };
    }
    // User for masking tests - has partial access + masking
    if (user === 'masking_partial') {
      if (password && password !== 'masking_partial_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'masking_partial',
            userAttributes: {},
            groups: ['masking_partial'],
          },
        },
      };
    }
    if (user === 'region_user') {
      if (password && password !== 'region_user_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'region_user',
            userAttributes: {
              allowedProductIds: [1, 2],
            },
            groups: ['user_group', 'region_group'],
          },
        },
      };
    }
    if (user === 'region_user_no_filter') {
      if (password && password !== 'region_user_no_filter_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'region_user_no_filter',
            userAttributes: {},
            groups: ['user_group'],
          },
        },
      };
    }
    if (user === 'sc_test') {
      if (password && password !== 'sc_test_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          cubeCloud: {
            userAttributes: {
              tenantId: '1',
            },
            groups: ['1', '2'],
          },
          auth: {
            username: 'sc_test',
            userAttributes: {},
            groups: [],
          },
        },
      };
    }
    if (user === 'conditional_mask_user') {
      if (password && password !== 'conditional_mask_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'conditional_mask_user',
            userAttributes: {},
            groups: ['conditional_mask_group'],
          },
        },
      };
    }
    if (user === 'conditional_mask_multi_user') {
      if (password && password !== 'conditional_mask_multi_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'conditional_mask_multi_user',
            userAttributes: {},
            groups: ['conditional_mask_group', 'conditional_mask_group_extra'],
          },
        },
      };
    }
    // User matching only the full-access policy (with a row filter) on a cube
    // that also has a separate, group-scoped masking policy the user is NOT in.
    if (user === 'single_policy_measure_user') {
      if (password && password !== 'single_policy_measure_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'single_policy_measure_user',
            userAttributes: {},
            groups: ['spm_full_group'],
          },
        },
      };
    }
    // User belonging to two groups whose access policies grant different
    // members (member-level union across groups, no row_level filters).
    if (user === 'multi_group_user') {
      if (password && password !== 'multi_group_password') {
        throw new Error(`Password doesn't match for ${user}`);
      }
      return {
        password,
        superuser: false,
        securityContext: {
          auth: {
            username: 'multi_group_user',
            userAttributes: {},
            groups: ['mg_group_a', 'mg_group_c'],
          },
        },
      };
    }
    throw new Error(`User "${user}" doesn't exist`);
  }
};
