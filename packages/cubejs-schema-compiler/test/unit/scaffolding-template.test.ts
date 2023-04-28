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
        name: 'accountId',
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
        name: 'failureCount',
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
  
  preAggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    Customers: {
      sql: \`\${CUBE}."customerId" = \${Customers}.id\`,
      relationship: \`belongsTo\`
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
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    }
  }
});
`,
        },
        {
          fileName: 'Customers.js',
          content: `cube(\`Customers\`, {
  sql: \`SELECT * FROM public.customers\`,
  
  preAggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    Accounts: {
      sql: \`\${CUBE}."accountId" = \${Accounts}.id\`,
      relationship: \`belongsTo\`
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
  }
});
`,
        },
        {
          fileName: 'Accounts.js',
          content: `cube(\`Accounts\`, {
  sql: \`SELECT * FROM public.accounts\`,
  
  preAggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    failurecount: {
      sql: \`\${CUBE}."failureCount"\`,
      type: \`sum\`
    }
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
  
  pre_aggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    customers: {
      sql: \`\${CUBE}."customerId" = \${customers}.id\`,
      relationship: \`many_to_one\`
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
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true
    }
  }
});
`,
        },
        {
          fileName: 'customers.js',
          content: `cube(\`customers\`, {
  sql_table: \`public.customers\`,
  
  pre_aggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    accounts: {
      sql: \`\${CUBE}."accountId" = \${accounts}.id\`,
      relationship: \`many_to_one\`
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
  }
});
`,
        },
        {
          fileName: 'accounts.js',
          content: `cube(\`accounts\`, {
  sql_table: \`public.accounts\`,
  
  pre_aggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`
    },
    
    failurecount: {
      sql: \`\${CUBE}."failureCount"\`,
      type: \`sum\`
    }
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
  
  pre_aggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
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
                nestedName: ['some', 'dimension', 'inside'],
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
  
  pre_aggregations: {
    // Pre-aggregation definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`
    }
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
  }
});
`,
        },
      ]);
    });

    it('should add options if passed', () => {
      const schemaContext = {
        dataSource: 'testDataSource',
        pre_aggregations: {
          main: {
            type: 'originalSql',
          },
        },
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
                nestedName: ['some', 'dimension', 'inside'],
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
  
  pre_aggregations: {
    main: {
      type: \`originalSql\`
    }
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`
    }
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

    pre_aggregations:
      # Pre-aggregation definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started

    joins:
      - name: customers
        sql: "{CUBE}.\\"customerId\\" = {customers}.id"
        relationship: many_to_one

    measures:
      - name: count
        type: count

      - name: amount
        sql: amount
        type: sum

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

`,
        },
        {
          fileName: 'customers.yml',
          content: `cubes:
  - name: customers
    sql_table: public.customers

    pre_aggregations:
      # Pre-aggregation definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started

    joins:
      - name: accounts
        sql: "{CUBE}.\\"accountId\\" = {accounts}.id"
        relationship: many_to_one

    measures:
      - name: count
        type: count

      - name: visit_count
        sql: visit_count
        type: sum

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: name
        sql: name
        type: string

`,
        },
        {
          fileName: 'accounts.yml',
          content: `cubes:
  - name: accounts
    sql_table: public.accounts

    pre_aggregations:
      # Pre-aggregation definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started

    joins: []

    measures:
      - name: count
        type: count

      - name: failurecount
        sql: "{CUBE}.\\"failureCount\\""
        type: sum

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

    pre_aggregations:
      # Pre-aggregation definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started

    joins: []

    measures:
      - name: count
        type: count

      - name: failurecount
        sql: "{CUBE}.\`failureCount\`"
        type: sum

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

`,
        },
      ]);
    });
  });
});
