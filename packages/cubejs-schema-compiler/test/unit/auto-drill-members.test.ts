import { prepareYamlCompiler, prepareJsCompiler } from './PrepareCompiler';

const modelContent = `
cubes:
  - name: orders
    sql: SELECT * FROM orders
    measures:
      - name: count
        sql: id
        type: count
      - name: total
        sql: amount
        type: sum
      - name: declared
        sql: amount
        type: sum
        drill_members:
          - status
      - name: declared_empty
        sql: amount
        type: sum
        drill_members: []
      - name: big_orders
        sql: amount
        type: sum
        filters:
          - sql: "{CUBE}.amount > 10000"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: city
        sql: city
        type: string
        links:
          - name: city_page
            label: Open the city page
            url: "{city}"
      - name: secret
        sql: secret
        type: string
        public: false
      - name: created_at
        sql: created_at
        type: time
      - name: line_item_count
        sql: "{line_items.count}"
        type: number
        sub_query: true

  - name: line_items
    sql: SELECT * FROM line_items
    measures:
      - name: count
        sql: id
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: sku
        sql: sku
        type: string

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - count
          - total
          - declared
          - status
          - line_item_count
          - name: city
            alias: renamed_city
`;

const legacyModelContent = `
cube('orders', {
  sql: 'SELECT * FROM orders',
  measures: {
    legacy: { type: 'count', sql: 'id', drillMemberReferences: [status] },
    bare: { type: 'count', sql: 'id', drillMembers: status },
    plain: { type: 'count', sql: 'id' }
  },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    status: { sql: 'status', type: 'string' }
  }
});
`;

const measure = (metaTransformer: any, cubeName: string, measureName: string) => metaTransformer.cubes
  .find((it: any) => it.config.name === cubeName)
  ?.config.measures.find((it: any) => it.name === `${cubeName}.${measureName}`);

const withEnv = async (env: Record<string, string>, fn: (metaTransformer: any) => void | Promise<void>) => {
  const originals: Record<string, string | undefined> = {};
  Object.keys(env).forEach((key) => {
    originals[key] = process.env[key];
    process.env[key] = env[key];
  });

  try {
    // The compiler must be prepared *after* the env is set — meta is computed
    // during compile() and cached on the instance.
    const { compiler, metaTransformer } = prepareYamlCompiler(modelContent);
    await compiler.compile();
    await fn(metaTransformer);
  } finally {
    Object.keys(env).forEach((key) => {
      if (originals[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = originals[key];
      }
    });
  }
};

describe('Auto drill members', () => {
  describe('flag off (default)', () => {
    let metaTransformer: any;

    beforeAll(async () => {
      delete process.env.CUBEJS_AUTO_DRILL_MEMBERS;
      delete process.env.CUBEJS_AUTO_DRILL_MEMBERS_LIMIT;
      const prepared = prepareYamlCompiler(modelContent);
      metaTransformer = prepared.metaTransformer;
      await prepared.compiler.compile();
    });

    it('leaves undeclared measures with no drill members', () => {
      expect(measure(metaTransformer, 'orders', 'count').drillMembers).toEqual([]);
      expect(measure(metaTransformer, 'orders', 'total').drillMembers).toEqual([]);
    });

    it('emits an empty grouped set, not a missing one', () => {
      expect(measure(metaTransformer, 'orders', 'count').drillMembersGrouped).toEqual({
        measures: [],
        dimensions: [],
      });
    });

    it('still resolves declared drill members', () => {
      expect(measure(metaTransformer, 'orders', 'declared').drillMembers).toEqual(['orders.status']);
    });

    it('leaves view measures with no drill members', () => {
      expect(measure(metaTransformer, 'orders_view', 'count').drillMembers).toEqual([]);
    });
  });

  describe('flag on', () => {
    it('gives an undeclared measure the cube dimensions, primary key first', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'count').drillMembers).toEqual([
          'orders.id',
          'orders.status',
          'orders.city',
          'orders.created_at',
        ]);
      });
    });

    it('populates drillMembersGrouped from the computed set', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'total').drillMembersGrouped).toEqual({
          measures: [],
          dimensions: ['orders.id', 'orders.status', 'orders.city', 'orders.created_at'],
        });
      });
    });

    it('excludes non-public dimensions but keeps the primary key', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        const { drillMembers } = measure(metaTransformer, 'orders', 'count');
        expect(drillMembers).not.toContain('orders.secret');
        // The primary key is hidden by default, yet it identifies the row.
        expect(drillMembers[0]).toBe('orders.id');
      });
    });

    it('excludes sub-query dimensions', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'count').drillMembers)
          .not.toContain('orders.line_item_count');
      });
    });

    it('applies to every undeclared measure on the cube', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'line_items', 'count').drillMembers).toEqual([
          'line_items.id',
          'line_items.sku',
        ]);
      });
    });

    it('excludes generated link dimensions', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        // `city` declares a link, so a public `synthetic` dimension is minted
        // alongside it. It is a URL helper, not an attribute worth a cap slot.
        expect(measure(metaTransformer, 'orders', 'count').drillMembers)
          .not.toContain('orders.city___link_city_page_url');
      });
    });

    it('hands each measure its own copy of the computed set', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        const count = measure(metaTransformer, 'orders', 'count');
        const total = measure(metaTransformer, 'orders', 'total');

        // The set is computed once per cube and meta is cached, so sharing the
        // instance would let one consumer's in-place sort reorder every other
        // measure's list for every later request.
        expect(count.drillMembers).not.toBe(total.drillMembers);
        count.drillMembers.reverse();
        expect(total.drillMembers[0]).toBe('orders.id');
      });
    });
  });

  describe('primary key shapes', () => {
    const compoundPkModel = `
cubes:
  - name: shipments
    sql: SELECT * FROM shipments
    measures:
      - name: count
        sql: id
        type: count
    dimensions:
      - name: order_id
        sql: order_id
        type: number
        primary_key: true
      - name: line_no
        sql: line_no
        type: number
        primary_key: true
      - name: carrier
        sql: carrier
        type: string

  - name: events
    sql: SELECT * FROM events
    measures:
      - name: count
        sql: id
        type: count
    dimensions:
      - name: name
        sql: name
        type: string
      - name: happened_at
        sql: happened_at
        type: time
`;

    const withModel = async (model: string, env: Record<string, string>, fn: (m: any) => void) => {
      const originals: Record<string, string | undefined> = {};
      Object.keys(env).forEach((key) => {
        originals[key] = process.env[key];
        process.env[key] = env[key];
      });

      try {
        const { compiler, metaTransformer } = prepareYamlCompiler(model);
        await compiler.compile();
        fn(metaTransformer);
      } finally {
        Object.keys(env).forEach((key) => {
          if (originals[key] === undefined) {
            delete process.env[key];
          } else {
            process.env[key] = originals[key];
          }
        });
      }
    };

    it('puts every part of a compound primary key ahead of the rest', async () => {
      await withModel(compoundPkModel, { CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'shipments', 'count').drillMembers).toEqual([
          'shipments.order_id',
          'shipments.line_no',
          'shipments.carrier',
        ]);
      });
    });

    it('counts the compound key against the cap', async () => {
      await withModel(
        compoundPkModel,
        { CUBEJS_AUTO_DRILL_MEMBERS: 'true', CUBEJS_AUTO_DRILL_MEMBERS_LIMIT: '2' },
        (metaTransformer) => {
          expect(measure(metaTransformer, 'shipments', 'count').drillMembers).toEqual([
            'shipments.order_id',
            'shipments.line_no',
          ]);
        }
      );
    });

    it('falls back to the public dimensions when a cube has no primary key', async () => {
      await withModel(compoundPkModel, { CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'events', 'count').drillMembers).toEqual([
          'events.name',
          'events.happened_at',
        ]);
      });
    });
  });

  describe('declared always wins', () => {
    it('does not touch a measure that declares drill members', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'declared').drillMembers).toEqual(['orders.status']);
      });
    });

    it('respects an empty declaration as an opt-out', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'declared_empty').drillMembers).toEqual([]);
      });
    });
  });

  describe('the cap', () => {
    it('truncates the set to the configured limit', async () => {
      await withEnv(
        { CUBEJS_AUTO_DRILL_MEMBERS: 'true', CUBEJS_AUTO_DRILL_MEMBERS_LIMIT: '2' },
        (metaTransformer) => {
          expect(measure(metaTransformer, 'orders', 'count').drillMembers).toEqual([
            'orders.id',
            'orders.status',
          ]);
        }
      );
    });

    it('yields nothing at all for a zero limit', async () => {
      await withEnv(
        { CUBEJS_AUTO_DRILL_MEMBERS: 'true', CUBEJS_AUTO_DRILL_MEMBERS_LIMIT: '0' },
        (metaTransformer) => {
          expect(measure(metaTransformer, 'orders', 'count').drillMembers).toEqual([]);
        }
      );
    });
  });

  describe('views', () => {
    it('names the view\'s own members, using the view\'s alias', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        const { drillMembers } = measure(metaTransformer, 'orders_view', 'count');

        // renamed_city, not city — proving these are the view's member keys
        // rather than source names re-prefixed.
        expect(drillMembers).toEqual(['orders_view.status', 'orders_view.renamed_city']);
      });
    });

    it('names only members the view actually includes', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        const { drillMembers } = measure(metaTransformer, 'orders_view', 'total');

        // created_at and secret are not included by the view, so a drill on it
        // cannot dead-end on them.
        expect(drillMembers).not.toContain('orders_view.created_at');
        expect(drillMembers).not.toContain('orders_view.secret');
        expect(drillMembers).not.toContain('orders_view.city');
      });
    });

    it('excludes generated link dimensions reached through the view', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        // A view's included members do carry `synthetic`, and the link helper
        // for an included dimension is auto-included alongside it — so an
        // unfiltered view set would pick URL helpers up here too.
        const { drillMembers } = measure(metaTransformer, 'orders_view', 'count');
        expect(drillMembers.filter((m: string) => m.includes('___link_'))).toEqual([]);
      });
    });

    it('excludes a sub-query dimension reached through the view', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        // A view's included members don't carry `sub_query`, so this only holds
        // if the dimension is resolved back to its source definition.
        expect(measure(metaTransformer, 'orders_view', 'count').drillMembers)
          .not.toContain('orders_view.line_item_count');
      });
    });

    it('carries a declared set through the view untouched', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders_view', 'declared').drillMembers).toEqual([
          'orders_view.status',
        ]);
      });
    });
  });

  describe('legacy and irregular declaration shapes', () => {
    const withLegacyEnv = async (env: Record<string, string>, fn: (m: any) => void) => {
      const originals: Record<string, string | undefined> = {};
      Object.keys(env).forEach((key) => {
        originals[key] = process.env[key];
        process.env[key] = env[key];
      });

      try {
        const { compiler, metaTransformer } = prepareJsCompiler(legacyModelContent);
        await compiler.compile();
        fn(metaTransformer);
      } finally {
        Object.keys(env).forEach((key) => {
          if (originals[key] === undefined) {
            delete process.env[key];
          } else {
            process.env[key] = originals[key];
          }
        });
      }
    };

    it('treats the legacy drillMemberReferences key as a declaration', async () => {
      await withLegacyEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'legacy').drillMembers).toEqual(['orders.status']);
      });
    });

    it('leaves a bare (non-array) declaration exactly as it has always been emitted', async () => {
      await withLegacyEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        // Passed through unchanged rather than normalized: this field has always
        // emitted the bare string for this shape, and the automatic set must not
        // alter what a declaring model already produces.
        expect(measure(metaTransformer, 'orders', 'bare').drillMembers).toBe('orders.status');
      });
    });

    it('still fills in an undeclared measure alongside them', async () => {
      await withLegacyEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        expect(measure(metaTransformer, 'orders', 'plain').drillMembers).toEqual([
          'orders.id',
          'orders.status',
        ]);
      });
    });
  });

  describe('the limit is not read unless the feature is on', () => {
    it('compiles with a malformed limit while the flag is off', async () => {
      const original = process.env.CUBEJS_AUTO_DRILL_MEMBERS_LIMIT;
      process.env.CUBEJS_AUTO_DRILL_MEMBERS_LIMIT = 'not-a-number';
      delete process.env.CUBEJS_AUTO_DRILL_MEMBERS;

      try {
        const { compiler, metaTransformer } = prepareYamlCompiler(modelContent);
        await compiler.compile();
        expect(measure(metaTransformer, 'orders', 'count').drillMembers).toEqual([]);
      } finally {
        if (original === undefined) {
          delete process.env.CUBEJS_AUTO_DRILL_MEMBERS_LIMIT;
        } else {
          process.env.CUBEJS_AUTO_DRILL_MEMBERS_LIMIT = original;
        }
      }
    });
  });

  describe('measure filters', () => {
    it('leaves a filtered measure\'s own filters in place alongside the computed set', async () => {
      await withEnv({ CUBEJS_AUTO_DRILL_MEMBERS: 'true' }, (metaTransformer) => {
        const bigOrders = measure(metaTransformer, 'orders', 'big_orders');

        // The drill set is computed, and the measure's filters are untouched by
        // it — they reach the drill query through the `measureFilter` operator,
        // which keys on the measure rather than on how its members were derived.
        expect(bigOrders.drillMembers).toEqual([
          'orders.id',
          'orders.status',
          'orders.city',
          'orders.created_at',
        ]);
      });
    });
  });
});
