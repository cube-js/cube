import {
  ScaffoldingTemplate,
  SchemaFormat,
} from '../../src/scaffolding/ScaffoldingTemplate';
import { MemberType } from '../../src/scaffolding/ScaffoldingSchema';

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

  describe('drill members', () => {
    const drillSchema = {
      public: {
        orders: [
          { name: 'id', type: 'integer', attributes: ['primaryKey'] },
          { name: 'order_status', type: 'character varying', attributes: [] },
          { name: 'notes', type: 'character varying', attributes: [] },
          { name: 'amount', type: 'integer', attributes: [] },
          { name: 'updated_at', type: 'timestamp', attributes: [] },
          { name: 'created_at', type: 'timestamp', attributes: [] },
        ],
      },
    };

    function generate(format: SchemaFormat, options = {}) {
      return new ScaffoldingTemplate(drillSchema, driver, {
        format,
        snakeCase: true,
        ...options,
      }).generateFilesByTableNames(['public.orders'])[0].content;
    }

    it('lists the primary key first, then attributes, then the main time dimension', () => {
      // created_at, not updated_at: ScaffoldingSchema sorts time columns by timeColumnIndex.
      expect(generate(SchemaFormat.Yaml)).toContain(
        'drill_members: [id, order_status, created_at]'
      );
    });

    it('leaves out dimensions that do not identify a row', () => {
      const content = generate(SchemaFormat.Yaml);

      // `notes` is still a dimension — it just isn't worth drilling into.
      expect(content).toContain('- name: notes');
      expect(content).not.toMatch(/drill_members: \[[^\]]*notes/);
    });

    it('renders drill members on every measure, including the generated count', () => {
      const content = generate(SchemaFormat.Yaml);
      const drillMembers = content.match(/drill_members:/g);

      // count + amount
      expect(drillMembers).toHaveLength(2);
      expect(content).toMatch(/- name: count\n\s+type: count\n\s+drill_members:/);
    });

    it('uses the camelCase key and member names when snakeCase is off', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            orders: [
              { name: 'ID', type: 'integer', attributes: ['primaryKey'] },
              { name: 'ORDER_STATUS', type: 'character varying', attributes: [] },
              { name: 'amount', type: 'integer', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.JavaScript, snakeCase: false }
      ).generateFilesByTableNames(['public.orders'])[0];

      // References the rendered member names, not the raw column names.
      expect(content).toContain('drillMembers: [id, orderStatus]');
    });

    it('omits the key entirely when nothing identifies a row', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            events: [{ name: 'amount', type: 'integer', attributes: [] }],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.events'])[0];

      expect(content).not.toContain('drill_members');
    });

    it('never references a dimension the caller deselected', () => {
      const template = new ScaffoldingTemplate(drillSchema, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true,
      });

      // The onboarding flow hands back an edited member list; a drill member the cube
      // no longer defines would dead-end at click time.
      const { content } = template.generateFilesByCubeDescriptors([
        {
          cube: 'orders',
          tableName: 'public.orders',
          table: 'orders',
          schema: 'public',
          joins: [],
          members: [
            {
              name: 'id',
              title: 'Id',
              memberType: MemberType.Dimension,
              types: ['number'],
              isPrimaryKey: true,
            },
            {
              name: 'created_at',
              title: 'Created At',
              memberType: MemberType.Dimension,
              types: ['time'],
            },
          ],
        },
      ])[0];

      expect(content).toContain('drill_members: [id, created_at]');
      expect(content).not.toContain('order_status');
    });

    it('keeps every describing attribute on a wide table', () => {
      const wide = {
        public: {
          things: [
            { name: 'id', type: 'integer', attributes: ['primaryKey'] },
            { name: 'name', type: 'character varying', attributes: [] },
            { name: 'title', type: 'character varying', attributes: [] },
            { name: 'status', type: 'character varying', attributes: [] },
            { name: 'category', type: 'character varying', attributes: [] },
            { name: 'type', type: 'character varying', attributes: [] },
            { name: 'code', type: 'character varying', attributes: [] },
            { name: 'created_at', type: 'timestamp', attributes: [] },
          ],
        },
      };

      const { content } = new ScaffoldingTemplate(wide, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true,
      }).generateFilesByTableNames(['public.things'])[0];

      // Nothing is truncated: primary key, every dictionary attribute, then the timestamp.
      expect(content).toContain(
        'drill_members: [id, name, title, status, category, type, code, created_at]'
      );
    });

    it('lists a member once when two columns render to the same name', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            things: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'user_name', type: 'character varying', attributes: [] },
              { name: 'user name', type: 'character varying', attributes: [] },
              { name: 'amount', type: 'integer', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.things'])[0];

      expect(content).toContain('drill_members: [id, user_name]');
    });

    it('collapses a name collision without dropping later members', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            things: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'user_name', type: 'character varying', attributes: [] },
              { name: 'user name', type: 'character varying', attributes: [] },
              { name: 'title', type: 'character varying', attributes: [] },
              { name: 'status', type: 'character varying', attributes: [] },
              { name: 'category', type: 'character varying', attributes: [] },
              { name: 'created_at', type: 'timestamp', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.things'])[0];

      // `user name` and `user_name` render to one member, so it appears once — and
      // collapsing it costs none of the attributes that follow.
      expect(content).toContain(
        'drill_members: [id, user_name, title, status, category, created_at]'
      );
    });

    it('keeps a composite primary key whole, with the timestamp after it', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            things: [
              ...['k1', 'k2', 'k3', 'k4', 'k5', 'k6'].map((name) => ({
                name,
                type: 'character varying',
                attributes: ['primaryKey'],
              })),
              { name: 'name', type: 'character varying', attributes: [] },
              { name: 'created_at', type: 'timestamp', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.things'])[0];

      // A 6-column key no longer crowds out the attribute or the timestamp. The key
      // columns follow the order `dimensions` renders them in, whatever that is — the
      // point here is that all of them survive, plus what comes after.
      expect(content).toContain(
        'drill_members: [k6, k5, k4, k3, k2, k1, name, created_at]'
      );
    });
  });
});
