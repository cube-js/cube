import { ScaffoldingSchema } from '../../src/scaffolding/ScaffoldingSchema';

describe('ScaffoldingSchema', () => {
  const schemas = {
    public: {
      orders: [{
        name: 'id',
        type: 'integer',
        attributes: []
      }, {
        name: 'amount',
        type: 'integer',
        attributes: []
      }, {
        name: 'customer_id',
        type: 'integer',
        attributes: []
      }],
      customers: [{
        name: 'id',
        type: 'integer',
        attributes: []
      }, {
        name: 'name',
        type: 'character varying',
        attributes: []
      }, {
        name: 'account_id',
        type: 'integer',
        attributes: []
      }],
      accounts: [{
        name: 'id',
        type: 'integer',
        attributes: []
      }, {
        name: 'username',
        type: 'character varying',
        attributes: []
      }, {
        name: 'password',
        type: 'character varying',
        attributes: []
      }, {
        name: 'failure_count',
        type: 'integer',
        attributes: []
      }, {
        name: 'account_status',
        type: 'character varying',
        attributes: []
      }],
    }
  };

  const schemasWithPrimaryAndForeignKeys = {
    public: {
      orders: [
        {
          name: 'test',
          type: 'integer',
          attributes: ['primaryKey']
        },
        {
          name: 'id',
          type: 'integer',
          attributes: []
        },
        {
          name: 'amount',
          type: 'integer',
          attributes: []
        },
        {
          name: 'customerkey',
          type: 'integer',
          attributes: [],
          foreign_keys: [
            {
              target_table: 'customers',
              target_column: 'id'
            }
          ]
        }
      ],
      customers: [
        {
          name: 'id',
          type: 'integer',
          attributes: []
        },
        {
          name: 'name',
          type: 'character varying',
          attributes: []
        },
        {
          name: 'account_id',
          type: 'integer',
          attributes: []
        }
      ],
      accounts: [
        {
          name: 'id',
          type: 'integer',
          attributes: []
        },
        {
          name: 'username',
          type: 'character varying',
          attributes: []
        },
        {
          name: 'password',
          type: 'character varying',
          attributes: ['primaryKey']
        },
        {
          name: 'failure_count',
          type: 'integer',
          attributes: []
        },
        {
          name: 'account_status',
          type: 'character varying',
          attributes: []
        }
      ],
    }
  };

  it('respects primary and foreign keys', () => {
    const schema = new ScaffoldingSchema(schemasWithPrimaryAndForeignKeys);
    const schemaForTables = schema.generateForTables(['public.orders', 'public.customers', 'public.accounts']);

    expect(schemaForTables).toEqual([
      {
        cube: 'Orders',
        schema: 'public',
        table: 'orders',
        tableName: 'public.orders',
        measures: [
          {
            name: 'amount',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Amount'
          }
        ],
        dimensions: [
          {
            name: 'test',
            types: [
              'number'
            ],
            title: 'Test',
            isPrimaryKey: true
          },
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
        ],
        joins: [
          {
            thisTableColumn: 'customerkey',
            tableName: 'public.customers',
            cubeToJoin: 'Customers',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'Customers',
        schema: 'public',
        table: 'customers',
        tableName: 'public.customers',
        measures: [],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'name',
            types: [
              'string'
            ],
            title: 'Name',
            isPrimaryKey: false
          }
        ],
        joins: [
          {
            thisTableColumn: 'account_id',
            tableName: 'public.accounts',
            cubeToJoin: 'Accounts',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'Accounts',
        schema: 'public',
        table: 'accounts',
        tableName: 'public.accounts',
        measures: [
          {
            name: 'failure_count',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Failure Count'
          }
        ],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'username',
            types: [
              'string'
            ],
            title: 'Username',
            isPrimaryKey: false
          },
          {
            name: 'password',
            types: [
              'string'
            ],
            title: 'Password',
            isPrimaryKey: true
          },
          {
            name: 'account_status',
            types: [
              'string'
            ],
            title: 'Account Status',
            isPrimaryKey: false
          }
        ],

        joins: []
      }
    ]);
  });

  it('schema', () => {
    const schema = new ScaffoldingSchema(schemas);
    const schemaForTables = schema.generateForTables(['public.orders', 'public.customers', 'public.accounts']);

    expect(schemaForTables).toEqual([
      {
        cube: 'Orders',
        schema: 'public',
        table: 'orders',
        tableName: 'public.orders',
        measures: [
          {
            name: 'amount',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Amount'
          }
        ],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          }
        ],
        joins: [
          {
            thisTableColumn: 'customer_id',
            tableName: 'public.customers',
            cubeToJoin: 'Customers',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'Customers',
        schema: 'public',
        table: 'customers',
        tableName: 'public.customers',
        measures: [],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'name',
            types: [
              'string'
            ],
            title: 'Name',
            isPrimaryKey: false
          }
        ],
        joins: [
          {
            thisTableColumn: 'account_id',
            tableName: 'public.accounts',
            cubeToJoin: 'Accounts',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'Accounts',
        schema: 'public',
        table: 'accounts',
        tableName: 'public.accounts',
        measures: [
          {
            name: 'failure_count',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Failure Count'
          }
        ],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'username',
            types: [
              'string'
            ],
            title: 'Username',
            isPrimaryKey: false
          },
          {
            name: 'password',
            types: [
              'string'
            ],
            title: 'Password',
            isPrimaryKey: false
          },
          {
            name: 'account_status',
            types: [
              'string'
            ],
            title: 'Account Status',
            isPrimaryKey: false
          }
        ],

        joins: []
      }
    ]);
  });

  it('schema', () => {
    const schema = new ScaffoldingSchema(schemas, { snakeCase: true });
    const schemaForTables = schema.generateForTables(['public.orders', 'public.customers', 'public.accounts']);
    expect(schemaForTables).toEqual([
      {
        cube: 'orders',
        schema: 'public',
        table: 'orders',
        tableName: 'public.orders',
        measures: [
          {
            name: 'amount',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Amount'
          }
        ],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          }
        ],
        joins: [
          {
            thisTableColumn: 'customer_id',
            tableName: 'public.customers',
            cubeToJoin: 'customers',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'customers',
        schema: 'public',
        table: 'customers',
        tableName: 'public.customers',
        measures: [],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'name',
            types: [
              'string'
            ],
            title: 'Name',
            isPrimaryKey: false
          }
        ],
        joins: [
          {
            thisTableColumn: 'account_id',
            tableName: 'public.accounts',
            cubeToJoin: 'accounts',
            columnToJoin: 'id',
            relationship: 'belongsTo'
          }
        ]
      },
      {
        cube: 'accounts',
        schema: 'public',
        table: 'accounts',
        tableName: 'public.accounts',
        measures: [
          {
            name: 'failure_count',
            types: [
              'sum',
              'avg',
              'min',
              'max'
            ],
            title: 'Failure Count'
          }
        ],
        dimensions: [
          {
            name: 'id',
            types: [
              'number'
            ],
            title: 'Id',
            isPrimaryKey: true
          },
          {
            name: 'username',
            types: [
              'string'
            ],
            title: 'Username',
            isPrimaryKey: false
          },
          {
            name: 'password',
            types: [
              'string'
            ],
            title: 'Password',
            isPrimaryKey: false
          },
          {
            name: 'account_status',
            types: [
              'string'
            ],
            title: 'Account Status',
            isPrimaryKey: false
          }
        ],
        joins: []
      }
    ]);
  });
});
