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
        name: 'customerKey',
        type: 'string',
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
        type: 'string',
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

describe('ScaffoldingTemplate', () => {
  describe('JavaScript formatter', () => {
    it('template', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver);

      template.generateFilesByTableNames([
        'public.orders',
        ['public', 'customers'],
        'public.accounts',
      ]).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
    });

    it('template with snake case', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        snakeCase: true,
      });

      template.generateFilesByTableNames([
        'public.orders',
        ['public', 'customers'],
        'public.accounts',
      ]).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
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

      template.generateFilesByTableNames(['public.someOrders']).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
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

      template.generateFilesByTableNames(['public.orders'])
        .forEach((it) => expect(it.content).toMatchSnapshot(it.fileName));
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

      template.generateFilesByTableNames(['public.orders'], schemaContext).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
    });

    it('uses dimension refs instead of table columns for join sql', () => {
      const template = new ScaffoldingTemplate(
        schemasWithPrimaryAndForeignKeys,
        driver,
        {
          format: SchemaFormat.JavaScript,
          snakeCase: true,
        }
      );

      template.generateFilesByTableNames(['public.orders', 'public.customers']).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
    });
  });

  describe('Yaml formatter', () => {
    it('generates schema for base driver', () => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true
      });

      template.generateFilesByTableNames([
        'public.orders',
        ['public', 'customers'],
        'public.accounts',
      ]).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
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

      template.generateFilesByTableNames(['public.accounts']).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
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

      template.generateFilesByTableNames(['public.accounts']).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
    });

    it('uses dimension refs instead of table columns for join sql', () => {
      const template = new ScaffoldingTemplate(
        schemasWithPrimaryAndForeignKeys,
        driver,
        {
          format: SchemaFormat.Yaml,
          snakeCase: true,
        }
      );

      template.generateFilesByTableNames(['public.orders', 'public.customers']).forEach((it) => {
        expect(it.content).toMatchSnapshot(it.fileName);
      });
    });
  });
});
