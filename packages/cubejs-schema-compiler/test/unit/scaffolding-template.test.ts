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

    it('lists the primary key first, then the rest, then the time dimensions', () => {
      expect(generate(SchemaFormat.Yaml)).toContain(
        'drill_members: [id, order_status, notes, created_at, updated_at]'
      );
    });

    it('keeps every time dimension rather than ranking them by name', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            things: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'deleted_at', type: 'timestamp', attributes: [] },
              { name: 'created_at', type: 'timestamp', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.things'])[0];

      // Which timestamp is the row's "when" isn't derivable from its name, so none is
      // dropped. (They render created-first because ScaffoldingSchema sorts time columns
      // that way — an ordering detail, not a decision about which one matters.)
      expect(content).toContain('drill_members: [id, created_at, deleted_at]');
    });

    it('does not repeat a primary key that is also a time column', () => {
      // Only reachable via descriptors: ScaffoldingSchema never marks a time column as a
      // primary key, but a caller can. Without the guard it would be listed twice — once
      // as the key, once among the timestamps.
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            snapshots: [
              { name: 'captured_at', type: 'timestamp', attributes: [] },
              { name: 'label', type: 'character varying', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByCubeDescriptors([
        {
          cube: 'snapshots',
          tableName: 'public.snapshots',
          table: 'snapshots',
          schema: 'public',
          joins: [],
          members: [
            {
              name: 'captured_at',
              title: 'Captured At',
              memberType: MemberType.Dimension,
              types: ['time'],
              isPrimaryKey: true,
            },
            {
              name: 'label',
              title: 'Label',
              memberType: MemberType.Dimension,
              types: ['string'],
            },
          ],
        },
      ])[0];

      expect(content).toContain('drill_members: [captured_at, label]');
    });

    it('drills into every dimension the cube defines, and only those', () => {
      const content = generate(SchemaFormat.Yaml);

      const dimensions = [
        ...content
          .split('dimensions:')[1]
          .split('measures:')[0]
          .matchAll(/- name: (\w+)/g),
      ].map((m) => m[1]);
      const drillMembers = content
        .match(/drill_members: \[([^\]]+)\]/)?.[1]
        .split(', ');

      // Exactly the dimension set — `notes` included, though no dictionary would have
      // called it meaningful. Ordering differs, so compare as sets.
      expect(dimensions).toContain('notes');
      expect([...(drillMembers ?? [])].sort()).toEqual([...dimensions].sort());
    });

    it('renders drill members on every measure, including the generated count', () => {
      const content = generate(SchemaFormat.Yaml);
      const drillMembers = content.match(/drill_members:/g);

      // count + amount
      expect(drillMembers).toHaveLength(2);
      expect(content).toMatch(/- name: count\n\s+type: count\n\s+drill_members:/);
    });

    it('keeps a blank line after an empty joins block, like a non-empty one', () => {
      // `drill_members` and an empty `joins` share the inline-array branch, so a
      // separator added there for one silently doubles up for the other.
      const content = generate(SchemaFormat.Yaml);

      expect(content).toMatch(/\n\s+joins: \[\]\n\n\s+dimensions:/);
    });

    it('renders an array-valued context prop without padding its elements', () => {
      // `SchemaContext` only declares `dataSource`, so this shape can't arrive
      // through the typed API today — the cast is the point. Drill members are all
      // MemberReferences and return early; a plain scalar reaches the value branch,
      // where a missing parent reads as "value after a key" and earns a leading
      // space: `[ alpha,  beta]`. Pinned so widening the type can't reintroduce it.
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            orders: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'amount', type: 'integer', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.orders'], {
        tags: ['alpha', 'beta'],
      } as any)[0];

      expect(content).toContain('tags: [alpha, beta]');
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

    it('keeps the caller\'s members and order on the descriptor path', () => {
      const template = new ScaffoldingTemplate(drillSchema, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true,
      });

      // ScaffoldingSchema's type filter and time-column sort don't reach this path — the
      // descriptor's members are passed through as sent, numeric dimensions included.
      // They're dimensions of the cube, so they drill.
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
              name: 'lat',
              title: 'Lat',
              memberType: MemberType.Dimension,
              types: ['number'],
            },
            {
              name: 'deleted_at',
              title: 'Deleted At',
              memberType: MemberType.Dimension,
              types: ['time'],
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

      expect(content).toContain('drill_members: [id, lat, deleted_at, created_at]');
    });

    it('keeps every dimension on a wide table, whatever its columns are called', () => {
      const wide = {
        public: {
          things: [
            { name: 'id', type: 'integer', attributes: ['primaryKey'] },
            { name: 'name', type: 'character varying', attributes: [] },
            // Deliberately opaque names: selection must not depend on recognising them.
            { name: 'zzz_top', type: 'character varying', attributes: [] },
            { name: 'qux', type: 'character varying', attributes: [] },
            { name: 'blorb', type: 'character varying', attributes: [] },
            { name: 'is_active', type: 'boolean', attributes: [] },
            { name: 'created_at', type: 'timestamp', attributes: [] },
          ],
        },
      };

      const { content } = new ScaffoldingTemplate(wide, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true,
      }).generateFilesByTableNames(['public.things'])[0];

      // Nothing is truncated and nothing is judged by its name: primary key, every other
      // dimension in rendered order, then the timestamp.
      expect(content).toContain(
        'drill_members: [id, name, zzz_top, qux, blorb, is_active, created_at]'
      );
    });

    it('drills into non-scalar columns, which the type filter treats as strings', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            events: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'payload', type: 'jsonb', attributes: [] },
              { name: 'blob_data', type: 'bytea', attributes: [] },
              { name: 'tags', type: 'text[]', attributes: [] },
              { name: 'created_at', type: 'timestamp', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.events'])[0];

      // `columnType` recognises numbers, booleans and times and calls everything else a
      // string, so JSON and binary columns are dimensions — and therefore drill members.
      // Documented rather than special-cased: excluding them means deciding a JSON column
      // can't be worth drilling into, which is the same guess as reading its name.
      expect(content).toContain(
        'drill_members: [id, payload, blob_data, tags, created_at]'
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
      // collapsing it costs none of the members that follow.
      expect(content).toContain(
        'drill_members: [id, user_name, title, status, category, created_at]'
      );
    });

    it('classifies a collapsed member off the definition the cube keeps', () => {
      const { content } = new ScaffoldingTemplate(
        {
          public: {
            things: [
              { name: 'id', type: 'integer', attributes: ['primaryKey'] },
              { name: 'user_name', type: 'character varying', attributes: [] },
              // Same member name, different type. The rendered `dimensions` object is a
              // spread-reduce, so the cube defines `user_name` as the *later* one — time.
              { name: 'USER NAME', type: 'timestamp', attributes: [] },
              // A plain attribute after the collision, so the two dedupe strategies are
              // told apart by where `user_name` lands relative to it.
              { name: 'zebra', type: 'character varying', attributes: [] },
              { name: 'created_at', type: 'timestamp', attributes: [] },
              { name: 'amount', type: 'integer', attributes: [] },
            ],
          },
        },
        driver,
        { format: SchemaFormat.Yaml, snakeCase: true }
      ).generateFilesByTableNames(['public.things'])[0];

      // The cube defines it as time, so the drill set must classify it as time too —
      // grouped after `zebra` with the other timestamps. Reading the discarded varchar
      // definition would file it as an attribute, putting it ahead of `zebra` instead.
      expect(content).toMatch(/- name: user_name\n\s+sql: .*USER NAME.*\n\s+type: time/);
      expect(content).toContain('drill_members: [id, zebra, user_name, created_at]');
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
      // columns' relative order isn't a contract — `dimensions.sort((a) => …)` ignores
      // its second argument, so the permutation is implementation-defined — assert the
      // set and what follows it, not the order within the key.
      const drillMembers = content
        .match(/drill_members: \[([^\]]+)\]/)?.[1]
        .split(', ');

      expect(drillMembers?.slice(0, 6).sort()).toEqual(['k1', 'k2', 'k3', 'k4', 'k5', 'k6']);
      expect(drillMembers?.slice(6)).toEqual(['name', 'created_at']);
    });
  });
});
