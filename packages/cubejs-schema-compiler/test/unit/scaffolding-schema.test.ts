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
      }, {
        name: 'bool_value',
        type: 'boolean',
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
          },
          {
            isPrimaryKey: false,
            name: 'bool_value',
            title: 'Bool Value',
            types: [
              'boolean'
            ],
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
          },
          {
            isPrimaryKey: false,
            name: 'bool_value',
            title: 'Bool Value',
            types: [
              'boolean'
            ],
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

  describe('columnType mapping for numeric types', () => {
    it('should map FLOAT types to number', () => {
      const floatSchemas = {
        public: {
          test: [
            { name: 'float_col', type: 'FLOAT', attributes: [] },
            { name: 'float4_col', type: 'FLOAT4', attributes: [] },
            { name: 'float8_col', type: 'FLOAT8', attributes: [] },
            { name: 'float32_col', type: 'FLOAT32', attributes: [] },
            { name: 'float64_col', type: 'FLOAT64', attributes: [] },
          ]
        }
      };
      const schema = new ScaffoldingSchema(floatSchemas);
      floatSchemas.public.test.forEach(col => {
        expect((schema as any).columnType(col)).toBe('number');
      });
    });

    it('should map REAL type to number', () => {
      const realSchemas = {
        public: {
          test: [{ name: 'real_col', type: 'REAL', attributes: [] }]
        }
      };
      const schema = new ScaffoldingSchema(realSchemas);
      expect((schema as any).columnType(realSchemas.public.test[0])).toBe('number');
    });

    it('should map SERIAL types to number', () => {
      const serialSchemas = {
        public: {
          test: [
            { name: 'serial_col', type: 'SERIAL', attributes: [] },
            { name: 'bigserial_col', type: 'BIGSERIAL', attributes: [] },
            { name: 'smallserial_col', type: 'SMALLSERIAL', attributes: [] },
          ]
        }
      };
      const schema = new ScaffoldingSchema(serialSchemas);
      serialSchemas.public.test.forEach(col => {
        expect((schema as any).columnType(col)).toBe('number');
      });
    });

    it('should map MONEY types to number', () => {
      const moneySchemas = {
        public: {
          test: [
            { name: 'money_col', type: 'MONEY', attributes: [] },
            { name: 'smallmoney_col', type: 'SMALLMONEY', attributes: [] },
          ]
        }
      };
      const schema = new ScaffoldingSchema(moneySchemas);
      moneySchemas.public.test.forEach(col => {
        expect((schema as any).columnType(col)).toBe('number');
      });
    });

    it('should map various integer types to number (covered by int keyword)', () => {
      const intSchemas = {
        public: {
          test: [
            // Standard integer types
            { name: 'tinyint_col', type: 'TINYINT', attributes: [] },
            { name: 'mediumint_col', type: 'MEDIUMINT', attributes: [] },
            { name: 'hugeint_col', type: 'HUGEINT', attributes: [] },
            // Unsigned integer types
            { name: 'uint8_col', type: 'UINT8', attributes: [] },
            { name: 'uint32_col', type: 'UINT32', attributes: [] },
            { name: 'uinteger_col', type: 'UINTEGER', attributes: [] },
            { name: 'ubigint_col', type: 'UBIGINT', attributes: [] },
            // Other variants
            { name: 'byteint_col', type: 'BYTEINT', attributes: [] },
          ]
        }
      };
      const schema = new ScaffoldingSchema(intSchemas);
      intSchemas.public.test.forEach(col => {
        expect((schema as any).columnType(col)).toBe('number');
      });
    });

    it('should be case insensitive for type matching', () => {
      const caseSchemas = {
        public: {
          test: [
            { name: 'float_lower', type: 'float', attributes: [] },
            { name: 'float_upper', type: 'FLOAT', attributes: [] },
            { name: 'float_mixed', type: 'Float', attributes: [] },
          ]
        }
      };
      const schema = new ScaffoldingSchema(caseSchemas);
      caseSchemas.public.test.forEach(col => {
        expect((schema as any).columnType(col)).toBe('number');
      });
    });
  });
});
