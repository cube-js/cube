import {
  ScaffoldingTemplate,
  SchemaFormat,
} from '../../src/scaffolding/ScaffoldingTemplate';

const driver = {
  quoteIdentifier: (name) => `"${name}"`,
};

const mySqlDriver = {
  quoteIdentifier: (name) => `\`${name}\``,
};

const bigQueryDriver = {
  quoteIdentifier(identifier) {
    const nestedFields = identifier.split('.');
    return nestedFields
      .map((name) => {
        if (name.match(/^[a-z0-9_]+$/)) {
          return name;
        }
        return `\`${identifier}\``;
      })
      .join('.');
  },
};

const dbSchema = {
  public: {
    orders: [
      {
        name: 'id',
        type: 'integer',
        attributes: [],
      },
      {
        name: 'amount',
        type: 'integer',
        attributes: [],
      },
      {
        name: 'customerId',
        type: 'integer',
        attributes: [],
      },
    ],
    customers: [
      {
        name: 'id',
        type: 'integer',
        attributes: [],
      },
      {
        name: 'visit_count',
        type: 'integer',
        attributes: [],
      },
      {
        name: 'name',
        type: 'character varying',
        attributes: [],
      },
      {
        name: 'account_id',
        type: 'integer',
        attributes: [],
      },
    ],
    accounts: [
      {
        name: 'id',
        type: 'integer',
        attributes: [],
      },
      {
        name: 'username',
        type: 'character varying',
        attributes: [],
      },
      {
        name: 'password',
        type: 'character varying',
        attributes: [],
      },
      {
        name: 'failure_count',
        type: 'integer',
        attributes: [],
      },
    ],
  },
};

describe('ScaffoldingTemplate', () => {
  describe('JavaScript formatter', () => {
    it('template', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver);

      expect(
        template.generateFilesByTableNames([
          'public.orders',
          ['public', 'customers'],
          'public.accounts',
        ])
      ).toEqual([
        {
          fileName: 'Orders.js',
          content: `cube(\`Orders\`, {
  sql: \`SELECT * FROM public.orders\`,
  
  joins: {
    Customers: {
      sql: \`\${CUBE}."customerId" = \${Customers}.id\`,
      relationship: \`belongsTo\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    amount: {
      sql: \`amount\`,
      type: \`sum\`
    }
  },
  
  preAggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
        {
          fileName: 'Customers.js',
          content: `cube(\`Customers\`, {
  sql: \`SELECT * FROM public.customers\`,
  
  joins: {
    Accounts: {
      sql: \`\${CUBE}.account_id = \${Accounts}.id\`,
      relationship: \`belongsTo\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    name: {
      sql: \`name\`,
      type: \`string\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    visitCount: {
      sql: \`visit_count\`,
      type: \`sum\`
    }
  },
  
  preAggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
        {
          fileName: 'Accounts.js',
          content: `cube(\`Accounts\`, {
  sql: \`SELECT * FROM public.accounts\`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    username: {
      sql: \`username\`,
      type: \`string\`
    },
    
    password: {
      sql: \`password\`,
      type: \`string\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    failureCount: {
      sql: \`failure_count\`,
      type: \`sum\`
    }
  },
  
  preAggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
      ]);
    });

    it('template with snake case', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        snakeCase: true,
      });

      expect(
        template.generateFilesByTableNames([
          'public.orders',
          ['public', 'customers'],
          'public.accounts',
        ])
      ).toEqual([
        {
          fileName: 'orders.js',
          content: `cube(\`orders\`, {
  sql_table: \`public.orders\`,
  
  joins: {
    customers: {
      sql: \`\${CUBE}."customerId" = \${customers}.id\`,
      relationship: \`many_to_one\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    amount: {
      sql: \`amount\`,
      type: \`sum\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
        {
          fileName: 'customers.js',
          content: `cube(\`customers\`, {
  sql_table: \`public.customers\`,
  
  joins: {
    accounts: {
      sql: \`\${CUBE}.account_id = \${accounts}.id\`,
      relationship: \`many_to_one\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    },
    
    name: {
      sql: \`name\`,
      type: \`string\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    visit_count: {
      sql: \`visit_count\`,
      type: \`sum\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
        {
          fileName: 'accounts.js',
          content: `cube(\`accounts\`, {
  sql_table: \`public.accounts\`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    },
    
    username: {
      sql: \`username\`,
      type: \`string\`
    },
    
    password: {
      sql: \`password\`,
      type: \`string\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    failure_count: {
      sql: \`failure_count\`,
      type: \`sum\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
      ]);
    });

    it('escaping back tick', () => {
      const template = new ScaffoldingTemplate(
        {
          public: {
            someOrders: [
              {
                name: 'id',
                type: 'integer',
                attributes: [],
              },
              {
                name: 'amount',
                type: 'integer',
                attributes: [],
              },
              {
                name: 'someDimension',
                type: 'string',
                attributes: [],
              },
            ],
          },
        },
        mySqlDriver,
        {
          snakeCase: true
        }
      );

      expect(template.generateFilesByTableNames(['public.someOrders'])).toEqual(
        [
          {
            fileName: 'some_orders.js',
            content: `cube(\`some_orders\`, {
  sql_table: \`public.\\\`someOrders\\\`\`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    },
    
    somedimension: {
      sql: \`\${CUBE}.\\\`someDimension\\\`\`,
      type: \`string\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    amount: {
      sql: \`amount\`,
      type: \`sum\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
          },
        ]
      );
    });

    it('big query nested fields', () => {
      const template = new ScaffoldingTemplate(
        {
          public: {
            orders: [
              {
                name: 'id',
                type: 'integer',
                attributes: [],
              },
              {
                name: 'some.dimension.inside',
                type: 'string',
                attributes: [],
              },
            ],
          },
        },
        bigQueryDriver,
        {
          snakeCase: true
        }
      );
      expect(template.generateFilesByTableNames(['public.orders'])).toEqual([
        {
          fileName: 'orders.js',
          content: `cube(\`orders\`, {
  sql_table: \`public.orders\`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    },
    
    some_dimension_inside: {
      sql: \`\${CUBE}.some.dimension.inside\`,
      type: \`string\`,
      title: \`Some.dimension.inside\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
      ]);
    });

    it('should add options if passed', () => {
      const schemaContext = {
        dataSource: 'testDataSource',
      };

      const template = new ScaffoldingTemplate(
        {
          public: {
            orders: [
              {
                name: 'id',
                type: 'integer',
                attributes: [],
              },
              {
                name: 'some.dimension.inside',
                type: 'string',
                attributes: [],
              },
            ],
          },
        },
        bigQueryDriver,
        {
          snakeCase: true
        }
      );

      expect(
        template.generateFilesByTableNames(['public.orders'], schemaContext)
      ).toEqual([
        {
          fileName: 'orders.js',
          content: `cube(\`orders\`, {
  sql_table: \`public.orders\`,
  
  data_source: \`testDataSource\`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    },
    
    some_dimension_inside: {
      sql: \`\${CUBE}.some.dimension.inside\`,
      type: \`string\`,
      title: \`Some.dimension.inside\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
`,
        },
      ]);
    });
  });

  describe('Yaml formatter', () => {
    it('generates schema for base driver', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true
      });

      expect(
        template.generateFilesByTableNames([
          'public.orders',
          ['public', 'customers'],
          'public.accounts',
        ])
      ).toEqual([
        {
          fileName: 'orders.yml',
          content: `cubes:
  - name: orders
    sql_table: public.orders

    joins:
      - name: customers
        sql: "{CUBE}.\\"customerId\\" = {customers}.id"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

    measures:
      - name: count
        type: count

      - name: amount
        sql: amount
        type: sum

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started

`,
        },
        {
          fileName: 'customers.yml',
          content: `cubes:
  - name: customers
    sql_table: public.customers

    joins:
      - name: accounts
        sql: "{CUBE}.account_id = {accounts}.id"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: name
        sql: name
        type: string

    measures:
      - name: count
        type: count

      - name: visit_count
        sql: visit_count
        type: sum

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started

`,
        },
        {
          fileName: 'accounts.yml',
          content: `cubes:
  - name: accounts
    sql_table: public.accounts

    joins: []

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: username
        sql: username
        type: string

      - name: password
        sql: password
        type: string

    measures:
      - name: count
        type: count

      - name: failure_count
        sql: failure_count
        type: sum

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started

`,
        },
      ]);
    });

    it('generates schema for MySQL driver', () => {
      const template = new ScaffoldingTemplate(
        {
          public: {
            accounts: dbSchema.public.accounts,
          },
        },
        mySqlDriver,
        {
          format: SchemaFormat.Yaml,
          snakeCase: true
        }
      );

      expect(template.generateFilesByTableNames(['public.accounts'])).toEqual([
        {
          fileName: 'accounts.yml',
          content: `cubes:
  - name: accounts
    sql_table: public.accounts

    joins: []

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: username
        sql: username
        type: string

      - name: password
        sql: password
        type: string

    measures:
      - name: count
        type: count

      - name: failure_count
        sql: failure_count
        type: sum

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started

`,
        },
      ]);
    });
    
    it('generates schema with a catalog', () => {
      const template = new ScaffoldingTemplate(
        {
          public: {
            accounts: dbSchema.public.accounts,
          },
        },
        driver,
        {
          format: SchemaFormat.Yaml,
          snakeCase: true,
          catalog: 'hello_catalog'
        }
      );

      expect(template.generateFilesByTableNames(['public.accounts'])).toEqual([
        {
          fileName: 'accounts.yml',
          content: `cubes:
  - name: accounts
    sql_table: hello_catalog.public.accounts

    joins: []

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: username
        sql: username
        type: string

      - name: password
        sql: password
        type: string

    measures:
      - name: count
        type: count

      - name: failure_count
        sql: failure_count
        type: sum

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started

`,
        },
      ]);
    });
  });
});
