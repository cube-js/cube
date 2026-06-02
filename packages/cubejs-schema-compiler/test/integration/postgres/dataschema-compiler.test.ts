import { CompileError } from '../../../src/compiler/CompileError';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { prepareCompiler as originalPrepareCompiler } from '../../../src/compiler/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('DataSchemaCompiler', () => {
  jest.setTimeout(200000);

  it('gutter', async () => {
    const { compiler } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id',
          drillMembers: [source, created_at]
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount',
          drillMemberReferences: [source, created_at]
        }
      },

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `);
    return compiler.compile();
  });

  it('error', async () => {
    const { compiler } = prepareJsCompiler(`
    cube({}, {
      measures: {}
    })
    `);
    return compiler.compile()
      .then(() => {
        compiler.throwIfAnyErrors();
        throw new Error();
      })
      .catch((error) => {
        expect(error).toBeInstanceOf(CompileError);
      });
  });

  it('duplicate member', () => {
    const { compiler } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count',
          sql: 'id'
        },
        id: {
          type: 'sum',
          sql: 'id'
        }
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        }
      }
    })
    `);
    return compiler.compile().then(() => {
      compiler.throwIfAnyErrors();
      throw new Error();
    }).catch((error) => {
      expect(error).toBeInstanceOf(CompileError);
    });
  });

  describe('Test duplicate properties', () => {
    const invalidSchema = `
      cube('visitors', {
        sql: 'select * from visitors',
        measures: {
          count: {
            type: 'count',
            sql: 'id'
          },
          count: {
            type: 'count',
            sql: 'id'
          }
        },
        dimensions: {
          date: {
            type: 'string',
            sql: 'date'
          }
        }
      })
    `;

    const validSchema = `
      cube('visitors', {
        sql: 'select * from visitors',
        measures: {
          count: {
            type: 'count',
            sql: 'id'
          }
        },
        dimensions: {
          date: {
            type: 'string',
            sql: 'date'
          }
        }
      })
    `;

    it('Should compile without error, allowJsDuplicatePropsInSchema = false, valid schema', () => {
      const { compiler } = prepareJsCompiler(validSchema, { allowJsDuplicatePropsInSchema: false });
      return compiler.compile().then(() => {
        compiler.throwIfAnyErrors();
      });
    });

    it('Should throw error, allowJsDuplicatePropsInSchema = false, invalid schema', () => {
      const { compiler } = prepareJsCompiler(invalidSchema, { allowJsDuplicatePropsInSchema: false });
      return compiler.compile().then(() => {
        compiler.throwIfAnyErrors();
        throw new Error();
      }).catch((error) => {
        expect(error).toBeInstanceOf(CompileError);
        expect(error.message).toMatch(/Duplicate property parsing count/);
      });
    });

    it('Should compile without error, allowJsDuplicatePropsInSchema = true, invalid schema', () => {
      const { compiler } = prepareJsCompiler(invalidSchema, { allowJsDuplicatePropsInSchema: true });
      return compiler.compile().then(() => {
        compiler.throwIfAnyErrors();
      });
    });
  });

  it('calculated metrics', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id'
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount'
        },
        per_visitor_revenue: {
          type: 'number',
          sql: visitor_revenue + "/" + visitor_count
        }
      },

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        },
        updated_at: {
          type: 'time',
          sql: 'updated_at'
        }
      }
    })
    `);

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.visitor_count'],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      filters: [{
        dimension: 'visitors.updated_at',
        operator: 'in_date_range',
        values: ['2017-01-01', '2017-01-30']
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__visitor_count: '1' },
      { visitors__created_at_day: '2017-01-04T00:00:00.000Z', visitors__visitor_count: '1' },
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__visitor_count: '1' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__visitor_count: '2' }
    ]);
  });

  it('static dimension case', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id'
        }
      },

      dimensions: {
        status: {
          type: 'string',
          case: {
            when: [{
              sql: \`\${CUBE}.status = 1\`,
              label: 'Approved'
            }, {
              sql: \`\${CUBE}.status = 2\`,
              label: 'Canceled'
            }],
            else: { label: 'Unknown' }
          }
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.visitor_count'],
      dimensions: ['visitors.status'],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.status'
      }]
    });

    console.log(query.buildSqlAndParams());

    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { visitors__status: 'Approved', visitors__visitor_count: '2' },
      { visitors__status: 'Canceled', visitors__visitor_count: '4' }
    ]);
  });

  it('filtered dates', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        },
        updated_at: {
          type: 'time',
          sql: 'updated_at'
        }
      }
    })
    `);
    const responses = [
      [{ visitors__created_at: '2017-01-03T00:00:00.000Z' }],
      [
        { visitors__created_at: '2016-09-07T00:00:00.000Z' },
        { visitors__created_at: '2017-01-05T00:00:00.000Z' },
        { visitors__created_at: '2017-01-06T00:00:00.000Z' },
        { visitors__created_at: '2017-01-07T00:00:00.000Z' }
      ],
      [{ visitors__created_at: '2017-01-07T00:00:00.000Z' }],
      [
        { visitors__created_at: '2016-09-07T00:00:00.000Z' },
        { visitors__created_at: '2017-01-03T00:00:00.000Z' },
        { visitors__created_at: '2017-01-05T00:00:00.000Z' },
        { visitors__created_at: '2017-01-06T00:00:00.000Z' }
      ],
      [{ visitors__created_at: '2017-01-07T00:00:00.000Z' }]
    ];

    await compiler.compile();

    const queries = ['in_date_range', 'not_in_date_range', 'on_the_date', 'before_date', 'after_or_on_date'].map((operator, index) => {
      const filterValues = index < 2 ? ['2017-01-01', '2017-01-03'] : ['2017-01-06', '2017-01-06'];
      return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        dimensions: ['visitors.created_at'],
        timeDimensions: [],
        filters: [{
          operator,
          dimension: 'visitors.created_at',
          values: filterValues
        }],
        order: [{
          id: 'visitors.created_at',
          desc: false
        }],
        timezone: 'America/Los_Angeles'
      });
    });

    return Promise.all(queries.map(async (query, index) => {
      console.log(query.buildSqlAndParams());
      const res = await dbRunner.testQuery(query.buildSqlAndParams());

      expect(res).toEqual(responses[index]);
    }));
  });

  it('export import', async () => {
    const { compiler, cubeEvaluator, joinGraph } = originalPrepareCompiler({
      localPath: () => '',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'main.js',
          content: `
          const fooTable = require('./some.js').foo;
          cube('Main', {
            sql: \`select * from \${fooTable}\`,
            measures: {
              count: {
                sql: 'count(*)',
                type: 'number'
              }
            }
          })
          `
        }, {
          fileName: 'some.js',
          content: `
          export const foo = 'bar';
          `
        }
      ])
    }, { adapter: 'postgres' });
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Main.count'],
      dimensions: [],
      timeDimensions: [],
      order: [],
      timezone: 'America/Los_Angeles'
    });
    console.log(query.buildSqlAndParams());
    expect(query.buildSqlAndParams()[0]).toMatch(/bar/);
  });

  it('contexts', async () => {
    const { compiler, contextEvaluator } = prepareJsCompiler(`
      cube('Visitors', {
        sql: \`
        select * from visitors
        \`,

        measures: {
          visitor_count: {
            type: 'count',
            sql: 'id'
          },
        },

        dimensions: {
          source: {
            type: 'string',
            sql: 'source'
          },
        }
      })

      context('Marketing', {
        contextMembers: [Visitors]
      });
    `);
    await compiler.compile();

    expect(contextEvaluator.contextList).toEqual(
      ['Marketing']
    );
  });

  it('views should not contain own members', () => {
    const { compiler } = prepareJsCompiler(`
    view('Visitors', {
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
        }
      }
    })
    `);
    return compiler.compile().then(() => {
      compiler.throwIfAnyErrors();
      throw new Error();
    }).catch((error) => {
      console.log(error);
      expect(error).toBeInstanceOf(CompileError);
    });
  });

  it('foreign cubes', () => {
    const { compiler } = prepareJsCompiler(`
    cube('Visitors', {
      sql: 'select * from visitors',

      dimensions: {
        foo: {
          type: 'number',
          sql: \`$\{Foreign}.bar\`,
        }
      }
    });

    cube('Foreign', {
      sql: 'select * from foreign',

      dimensions: {
        bar: {
          type: 'number',
          sql: 'id',
        }
      }
    })
    `);
    return compiler.compile().then(() => {
      compiler.throwIfAnyErrors();
      throw new Error();
    }).catch((error) => {
      console.log(error);
      expect(error).toBeInstanceOf(CompileError);
    });
  });

  it('view_groups defined via standalone view_group()', async () => {
    const { compiler, metaTransformer, viewGroupEvaluator } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      cube('Customers', {
        sql: \`select * from customers\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view('customers_view', {
        cubes: [{
          joinPath: Customers,
          includes: '*'
        }]
      })

      view_group('sales', {
        title: 'Sales',
        description: 'Sales related views',
        views: [revenue, customers_view]
      });
    `);
    await compiler.compile();

    expect(viewGroupEvaluator.viewGroupList).toEqual(['sales']);
    expect(viewGroupEvaluator.compiledViewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      description: 'Sales related views',
      views: ['revenue', 'customers_view'],
      includes: ['revenue', 'customers_view'],
    }]);

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      description: 'Sales related views',
      views: ['revenue', 'customers_view'],
      includes: ['revenue', 'customers_view'],
    }]);

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);

    const customersView = metaTransformer.cubes.find(c => c.config.name === 'customers_view');
    expect(customersView?.config.viewGroups).toEqual(['sales']);
  });

  it('view_group with string references', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view_group('sales', {
        title: 'Sales',
        views: ['revenue']
      });
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      views: ['revenue'],
      includes: ['revenue'],
    }]);

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);
  });

  it('view_group defined via view property', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        viewGroup: 'sales',
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view_group('sales', {});
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      views: ['revenue'],
      includes: ['revenue'],
    }]);

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);
  });

  it('plural viewGroups property on view', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        viewGroups: ['sales', 'finance'],
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view_group('sales', {
        title: 'Sales',
      });

      view_group('finance', {
        title: 'Finance',
      });
    `);
    await compiler.compile();

    const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
    expect(salesGroup?.views).toContain('revenue');
    expect(salesGroup?.title).toBe('Sales');

    const financeGroup = metaTransformer.viewGroups.find(g => g.name === 'finance');
    expect(financeGroup?.views).toContain('revenue');
    expect(financeGroup?.title).toBe('Finance');

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales', 'finance']);
  });

  it('singular viewGroup and plural viewGroups are merged', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        viewGroup: 'sales',
        viewGroups: ['finance'],
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view_group('sales', {});
      view_group('finance', {});
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toHaveLength(2);
    expect(metaTransformer.viewGroups.find(g => g.name === 'sales')?.views).toContain('revenue');
    expect(metaTransformer.viewGroups.find(g => g.name === 'finance')?.views).toContain('revenue');

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales', 'finance']);
  });

  it('view-level viewGroup as bare reference to view_group', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view_group('sales', {
        title: 'Sales',
      });

      view('revenue', {
        viewGroup: sales,
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups.find(g => g.name === 'sales')?.views).toContain('revenue');

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);
  });

  it('view-level viewGroups as bare references to view_group', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view_group('sales', { title: 'Sales' });
      view_group('finance', { title: 'Finance' });

      view('revenue', {
        viewGroups: [sales, finance],
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })
    `);
    await compiler.compile();

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales', 'finance']);
  });

  it('view_group merges standalone and view-level definitions', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      cube('Customers', {
        sql: \`select * from customers\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        viewGroup: 'sales',
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })

      view('customers_view', {
        cubes: [{
          joinPath: Customers,
          includes: '*'
        }]
      })

      view_group('sales', {
        title: 'Sales',
        description: 'Sales related views',
        views: ['customers_view']
      });
    `);
    await compiler.compile();

    const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
    expect(salesGroup).toBeDefined();
    expect(salesGroup?.title).toBe('Sales');
    expect(salesGroup?.description).toBe('Sales related views');
    expect(salesGroup?.views).toContain('customers_view');
    expect(salesGroup?.views).toContain('revenue');
  });

  it('view_groups in YAML format', async () => {
    const { compiler, metaTransformer, viewGroupEvaluator } = prepareYamlCompiler(`
cubes:
  - name: Orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: Orders
        includes: '*'

view_groups:
  - name: sales
    title: Sales
    description: Sales related views
    views:
      - revenue
    `);
    await compiler.compile();

    expect(viewGroupEvaluator.viewGroupList).toEqual(['sales']);
    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      description: 'Sales related views',
      views: ['revenue'],
      includes: ['revenue'],
    }]);

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);
  });

  it('view_group via view property in YAML', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: Orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true

views:
  - name: revenue
    view_group: sales
    cubes:
      - join_path: Orders
        includes: '*'

view_groups:
  - name: sales
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      views: ['revenue'],
      includes: ['revenue'],
    }]);

    const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenueView?.config.viewGroups).toEqual(['sales']);
  });

  it('fails when view references non-existent view group', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { type: 'number', sql: 'id', primaryKey: true },
        }
      })

      view('revenue', {
        viewGroup: 'nonexistent',
        cubes: [{
          joinPath: Orders,
          includes: '*'
        }]
      })
    `);

    try {
      await compiler.compile();
      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('View "revenue" references view group "nonexistent" which is not defined');
    }
  });

  it('view_group with includes (string references)', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      cube('Customers', {
        sql: \`select * from customers\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('customers_view', { cubes: [{ joinPath: Customers, includes: '*' }] })

      view_group('sales', {
        title: 'Sales',
        includes: ['revenue', 'customers_view']
      });
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      views: ['revenue', 'customers_view'],
      includes: ['revenue', 'customers_view'],
    }]);

    expect(metaTransformer.cubes.find(c => c.config.name === 'revenue')?.config.viewGroups).toEqual(['sales']);
    expect(metaTransformer.cubes.find(c => c.config.name === 'customers_view')?.config.viewGroups).toEqual(['sales']);
  });

  it('view_group with includes (bare and mixed references)', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      cube('Customers', {
        sql: \`select * from customers\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('customers_view', { cubes: [{ joinPath: Customers, includes: '*' }] })

      view_group('sales', {
        title: 'Sales',
        includes: [revenue, 'customers_view']
      });
    `);
    await compiler.compile();

    const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
    expect(salesGroup?.views).toEqual(['revenue', 'customers_view']);
    expect(salesGroup?.includes).toEqual(['revenue', 'customers_view']);
  });

  it('view_group with nested view groups (includes)', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('enterprise_deals', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        title: 'Sales',
        description: 'Sales related views',
        includes: [
          revenue,
          {
            name: 'ent_sales',
            title: 'Enterprise Sales',
            description: 'Enterprise deals',
            includes: [enterprise_deals]
          }
        ]
      });
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      description: 'Sales related views',
      views: ['revenue'],
      includes: [
        'revenue',
        {
          name: 'ent_sales',
          title: 'Enterprise Sales',
          description: 'Enterprise deals',
          views: ['enterprise_deals'],
          includes: ['enterprise_deals'],
        },
      ],
    }]);

    // Most-specific membership: a view in a nested group maps to the nested group only.
    expect(metaTransformer.cubes.find(c => c.config.name === 'revenue')?.config.viewGroups).toEqual(['sales']);
    expect(metaTransformer.cubes.find(c => c.config.name === 'enterprise_deals')?.config.viewGroups).toEqual(['ent_sales']);
  });

  it('view_group with deeply nested view groups', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('a', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('b', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('c', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('root', {
        includes: [
          a,
          {
            name: 'mid',
            includes: [
              b,
              { name: 'leaf', includes: [c] }
            ]
          }
        ]
      });
    `);
    await compiler.compile();

    const root = metaTransformer.viewGroups.find(g => g.name === 'root');
    expect(root?.views).toEqual(['a']);
    const mid = root?.includes.find((i: any) => typeof i !== 'string' && i.name === 'mid') as any;
    expect(mid.views).toEqual(['b']);
    const leaf = mid.includes.find((i: any) => typeof i !== 'string' && i.name === 'leaf') as any;
    expect(leaf.views).toEqual(['c']);
    expect(leaf.includes).toEqual(['c']);

    expect(metaTransformer.cubes.find(c => c.config.name === 'a')?.config.viewGroups).toEqual(['root']);
    expect(metaTransformer.cubes.find(c => c.config.name === 'b')?.config.viewGroups).toEqual(['mid']);
    expect(metaTransformer.cubes.find(c => c.config.name === 'c')?.config.viewGroups).toEqual(['leaf']);
  });

  it('view can belong to two sibling top-level view groups via includes', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', { title: 'Sales', includes: [revenue] });
      view_group('finance', { title: 'Finance', includes: [revenue] });
    `);
    await compiler.compile();

    const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
    expect(salesGroup?.views).toEqual(['revenue']);
    expect(salesGroup?.includes).toEqual(['revenue']);

    const financeGroup = metaTransformer.viewGroups.find(g => g.name === 'finance');
    expect(financeGroup?.views).toEqual(['revenue']);
    expect(financeGroup?.includes).toEqual(['revenue']);

    expect(metaTransformer.cubes.find(c => c.config.name === 'revenue')?.config.viewGroups).toEqual(['sales', 'finance']);
  });

  it('view can belong to both a view group and its nested child group', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        title: 'Sales',
        includes: [
          revenue,
          { name: 'ent_sales', title: 'Enterprise Sales', includes: [revenue] }
        ]
      });
    `);
    await compiler.compile();

    const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
    expect(salesGroup?.views).toEqual(['revenue']);
    expect(salesGroup?.includes).toEqual([
      'revenue',
      {
        name: 'ent_sales',
        title: 'Enterprise Sales',
        views: ['revenue'],
        includes: ['revenue'],
      },
    ]);

    // The view is a direct member of both the parent and the nested child.
    expect(metaTransformer.cubes.find(c => c.config.name === 'revenue')?.config.viewGroups).toEqual(['sales', 'ent_sales']);
  });

  it('fails when view group uses both views and includes', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })
      view('orders_overview', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        views: ['revenue'],
        includes: ['orders_overview']
      });
    `);

    try {
      await compiler.compile();
      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('View group must use either "views" or "includes", but not both');
    }
  });

  it('view_group with nested view groups in YAML', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: Orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: Orders
        includes: '*'
  - name: enterprise_deals
    cubes:
      - join_path: Orders
        includes: '*'

view_groups:
  - name: sales
    title: Sales
    includes:
      - revenue
      - name: ent_sales
        title: Enterprise Sales
        description: Enterprise deals
        includes:
          - enterprise_deals
    `);
    await compiler.compile();

    expect(metaTransformer.viewGroups).toEqual([{
      name: 'sales',
      title: 'Sales',
      views: ['revenue'],
      includes: [
        'revenue',
        {
          name: 'ent_sales',
          title: 'Enterprise Sales',
          description: 'Enterprise deals',
          views: ['enterprise_deals'],
          includes: ['enterprise_deals'],
        },
      ],
    }]);

    expect(metaTransformer.cubes.find(c => c.config.name === 'enterprise_deals')?.config.viewGroups).toEqual(['ent_sales']);
  });

  it('fails when view group includes a non-existent view', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        includes: ['revenue', 'ghost']
      });
    `);

    try {
      await compiler.compile();
      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('View group "sales" includes "ghost" which is not a defined view');
    }
  });

  it('fails on duplicate nested view group name', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        includes: [
          { name: 'dup', includes: [revenue] },
          { name: 'dup', includes: [revenue] }
        ]
      });
    `);

    try {
      await compiler.compile();
      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('View group "dup" already exists');
    }
  });

  it('fails on malformed nested view group (missing name)', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('Orders', {
        sql: \`select * from orders\`,
        measures: { count: { type: 'count' } },
        dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } }
      })

      view('revenue', { cubes: [{ joinPath: Orders, includes: '*' }] })

      view_group('sales', {
        includes: [
          { title: 'No name', includes: [revenue] }
        ]
      });
    `);

    try {
      await compiler.compile();
      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('view group');
    }
  });
});
