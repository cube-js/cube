import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';

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
    // Pre-Aggregations definitions go here
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
      type: \`count\`,
      drillMembers: [id]
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
    // Pre-Aggregations definitions go here
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
      type: \`count\`,
      drillMembers: [id, name]
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
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, username]
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
        mySqlDriver
      );

      expect(template.generateFilesByTableNames(['public.someOrders'])).toEqual(
        [
          {
            fileName: 'SomeOrders.js',
            content: `cube(\`SomeOrders\`, {
  sql: \`SELECT * FROM public.\\\`someOrders\\\`\`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id]
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
        bigQueryDriver
      );
      expect(template.generateFilesByTableNames(['public.orders'])).toEqual([
        {
          fileName: 'Orders.js',
          content: `cube(\`Orders\`, {
  sql: \`SELECT * FROM public.orders\`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, someDimensionInside]
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    someDimensionInside: {
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
        preAggregations: {
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
        bigQueryDriver
      );

      expect(
        template.generateFilesByTableNames(['public.orders'], schemaContext)
      ).toEqual([
        {
          fileName: 'Orders.js',
          content: `cube(\`Orders\`, {
  sql: \`SELECT * FROM public.orders\`,
  
  preAggregations: {
    main: {
      type: \`originalSql\`
    }
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, someDimensionInside]
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    someDimensionInside: {
      sql: \`\${CUBE}.some.dimension.inside\`,
      type: \`string\`,
      title: \`Some.dimension.inside\`
    }
  },
  
  dataSource: \`testDataSource\`
});
`,
        },
      ]);
    });
  });

  describe('Yaml formatter', () => {
    it('generates schema for base driver', () => {
      const template = new ScaffoldingTemplate(
        dbSchema,
        driver,
        SchemaFormat.Yaml
      );

      expect(
        template.generateFilesByTableNames([
          'public.orders',
          ['public', 'customers'],
          'public.accounts',
        ])
      ).toEqual([
        {
          fileName: 'Orders.yaml',
          content: `cubes:
  - name: Orders
    sql: SELECT * FROM public.orders
      # preAggregations:
      # Pre-Aggregations definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
    joins:
      - name: Customers
        sql: "{CUBE}.\\"customerId\\" = {Customers}.id"
        relationship: belongsTo
    measures:
      - name: count
        type: count
        drillMembers: [id]
      - name: amount
        sql: amount
        type: sum
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
`,
        },
        {
          fileName: 'Customers.yaml',
          content: `cubes:
  - name: Customers
    sql: SELECT * FROM public.customers
      # preAggregations:
      # Pre-Aggregations definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
    joins:
      - name: Accounts
        sql: "{CUBE}.\\"accountId\\" = {Accounts}.id"
        relationship: belongsTo
    measures:
      - name: count
        type: count
        drillMembers: [id, name]
      - name: visitCount
        sql: visit_count
        type: sum
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
      - name: name
        sql: name
        type: string
`,
        },
        {
          fileName: 'Accounts.yaml',
          content: `cubes:
  - name: Accounts
    sql: SELECT * FROM public.accounts
      # preAggregations:
      # Pre-Aggregations definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
    joins: []
    measures:
      - name: count
        type: count
        drillMembers: [id, username]
      - name: failurecount
        sql: "{CUBE}.\\"failureCount\\""
        type: sum
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
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
        SchemaFormat.Yaml
      );
      
      expect(
        template.generateFilesByTableNames([
          'public.accounts',
        ])
      ).toEqual([
        {
          fileName: 'Accounts.yaml',
          content: `cubes:
  - name: Accounts
    sql: SELECT * FROM public.accounts
      # preAggregations:
      # Pre-Aggregations definitions go here
      # Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
    joins: []
    measures:
      - name: count
        type: count
        drillMembers: [id, username]
      - name: failurecount
        sql: "{CUBE}.\`failureCount\`"
        type: sum
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
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
