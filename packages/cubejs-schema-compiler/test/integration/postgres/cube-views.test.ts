import { getEnv } from '@cubejs-backend/shared';
import { BaseQuery, PostgresQuery } from '../../../src/adapter';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Cube Views', () => {
  jest.setTimeout(200000);

  // language=JavaScript
  const { compiler, joinGraph, cubeEvaluator, metaTransformer } = prepareJsCompiler(`
cube(\`Orders\`, {
  sql: \`
  SELECT 1 as id, 1 as product_id, 'completed' as status, '2022-01-01T00:00:00.000Z'::timestamptz as created_at
  UNION ALL
  SELECT 2 as id, 2 as product_id, 'completed' as status, '2022-01-02T00:00:00.000Z'::timestamptz as created_at
  \`,

  shown: false,

  refreshKey: {
    sql: \`SELECT MAX(created_at) FROM \${Orders.sql()} orders WHERE \${FILTER_PARAMS.Orders.createdAt.filter('created_at')}\`
  },

  preAggregations: {
    countByProductName: {
      measures: [CUBE.count],
      dimensions: [Products.name],
      timeDimension: CUBE.createdAt,
      granularity: \`day\`,
      partitionGranularity: \`month\`,
      buildRangeStart: { sql: \`SELECT '2022-01-01'\` },
      buildRangeEnd: { sql: \`SELECT '2022-03-01'\` },
    }
  },

  joins: {
    Products: {
      sql: \`\${CUBE}.product_id = \${Products}.id\`,
      relationship: \`belongsTo\`
    },
    ProductsAlt: {
      sql: \`\${CUBE}.product_id = \${ProductsAlt}.id\`,
      relationship: \`belongsTo\`
    }
  },

  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, createdAt, Products.ProductCategories.name]
    },

    runningTotal: {
      type: \`count\`,
      rollingWindow: {
        trailing: \`unbounded\`
      },
    },
  },

  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },

    status: {
      sql: \`status\`,
      type: \`string\`
    },

    statusProduct: {
      sql: \`\${CUBE}.status || '_' || \${Products.name}\`,
      type: \`string\`
    },

    createdAt: {
      sql: \`created_at\`,
      type: \`time\`
    },

    productId: {
      sql: \`product_id\`,
      type: \`number\`,
    },

    productAndCategory: {
      sql: \`\${Products.name} || '_' || \${Products.ProductCategories.name}\`,
      type: \`string\`
    },
  },

  segments: {
    potatoOnly: {
      sql: \`\${CUBE}.product_id = 2 AND \${FILTER_PARAMS.Orders.productId.filter(\`\${CUBE.productId}\`)}\`,
    },
  },

  dataSource: \`default\`
});

cube(\`Products\`, {
  sql: \`
  SELECT 1 as id, 1 as product_category_id, 'Tomato' as name
  UNION ALL
  SELECT 2 as id, 1 as product_category_id, 'Potato' as name
  \`,

  joins: {
    ProductCategories: {
      sql: \`\${CUBE}.product_category_id = \${ProductCategories}.id\`,
      relationship: \`belongsTo\`
    },
  },

  measures: {
    count: {
      type: \`count\`,
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
    },

    proxyName: {
      sql: \`\${name}\`,
      type: \`string\`,
    },
  }
});

cube(\`ProductsAlt\`, {
  sql: \`SELECT * FROM \${Products.sql()} as p WHERE id = 1\`,

  joins: {
    ProductCategories: {
      sql: \`\${CUBE}.product_category_id = \${ProductCategories}.id\`,
      relationship: \`belongsTo\`
    },
  },

  measures: {
    count: {
      type: \`count\`,
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
    },
  }
});

cube(\`ProductCategories\`, {
  sql: \`
  SELECT 1 as id, 'Groceries' as name
  UNION ALL
  SELECT 2 as id, 'Electronics' as name
  \`,

  joins: {

  },

  measures: {
    count: {
      type: \`count\`,
    },
    count2: {
      type: \`count\`,
      drillMembers: [id, name]
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
    },
  }
});

view(\`OrdersView\`, {
  cubes: [{
    join_path: Orders,
    includes: '*',
    excludes: ['createdAt']
  }, {
    join_path: Orders.Products,
    includes: '*',
    prefix: true
  }, {
    join_path: Orders.Products.ProductCategories,
    includes: '*',
    prefix: true
  }],

  measures: {
    productCategoryCount: {
      sql: \`\${Orders.ProductsAlt.ProductCategories.count}\`,
      type: \`number\`
    }
  },

  dimensions: {
    createdAt: {
      sql: \`\${Orders.createdAt}\`,
      type: \`time\`
    },

    productName: {
      sql: \`\${Products.name}\`,
      type: \`string\`
    },

    categoryName: {
      sql: \`\${Orders.ProductsAlt.ProductCategories.name}\`,
      type: \`string\`
    },

    productCategory: {
      sql: \`\${Orders.ProductsAlt.name} || '_' || \${Orders.ProductsAlt.ProductCategories.name} || '_' || \${categoryName}\`,
      type: \`string\`
    },
  }
});

view(\`OrdersView3\`, {
  cubes: [{
    join_path: Orders,
    includes: '*'
  }, {
    join_path: Orders.Products.ProductCategories,
    includes: '*',
    split: true
  }]
});

view(\`OrdersSimpleView\`, {
  cubes: [{
    join_path: Orders,
    includes: ['createdAt', 'count']
  }]
});

view(\`OrdersViewDrillMembers\`, {
  cubes: [{
    join_path: Orders,
    includes: ['createdAt', 'count']
  }, {
    join_path: Orders.Products.ProductCategories,
    includes: ['name', 'count2']
  }]
});

view(\`OrdersViewDrillMembersWithPrefix\`, {
  cubes: [{
    join_path: Orders,
    includes: ['createdAt', 'count']
  }, {
    join_path: Orders.Products.ProductCategories,
    includes: ['name', 'count2'],
    prefix: true
  }]
});
  `);

  async function runQueryTest(q: any, expectedResult: any, additionalTest?: (query: BaseQuery) => any) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, { ...q, timezone: 'UTC', preAggregationsSchema: '' });

    console.log(query.buildSqlAndParams());

    console.log(query.cacheKeyQueries());

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    console.log(JSON.stringify(res));

    if (additionalTest) {
      additionalTest(query);
    }

    expect(res).toEqual(
      expectedResult
    );
  }

  it('simple view', async () => runQueryTest({
    measures: ['OrdersView.count'],
    dimensions: [
      'OrdersView.categoryName'
    ],
    order: [{ id: 'OrdersView.categoryName' }]
  }, [{
    orders_view__category_name: 'Groceries',
    orders_view__count: '1',
  }, {
    orders_view__category_name: null,
    orders_view__count: '1',
  }]));

  it('join from two join hint paths', async () => runQueryTest({
    measures: ['OrdersView.productCategoryCount'],
    dimensions: [
      'OrdersView.categoryName'
    ],
    order: [{ id: 'OrdersView.productCategoryCount' }]
  }, [{
    orders_view__category_name: null,
    orders_view__product_category_count: '0',
  }, {
    orders_view__category_name: 'Groceries',
    orders_view__product_category_count: '1',
  }]));

  it('pre-aggregation', async () => runQueryTest({
    measures: ['OrdersView.count'],
    dimensions: [
      'OrdersView.productName'
    ],
    order: [{ id: 'OrdersView.productName' }],
  }, [{
    orders_view__product_name: 'Potato',
    orders_view__count: '1',
  }, {
    orders_view__product_name: 'Tomato',
    orders_view__count: '1',
  }], (query) => {
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/count_by_product_name/);
  }));

  it('proxy dimension', async () => runQueryTest({
    measures: ['OrdersView.count'],
    dimensions: [
      'Products.proxyName'
    ],
    order: [{ id: 'Products.proxyName' }],
  }, [{
    products__proxy_name: 'Potato',
    orders_view__count: '1',
  }, {
    products__proxy_name: 'Tomato',
    orders_view__count: '1',
  }], (query) => {
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/count_by_product_name/);
  }));

  it('compound dimension', async () => runQueryTest({
    measures: [],
    dimensions: [
      'Orders.statusProduct'
    ],
    order: [{ id: 'Orders.statusProduct' }],
  }, [{
    orders__status_product: 'completed_Potato',
  }, {
    orders__status_product: 'completed_Tomato',
  }]));

  it('compound dimension 2', async () => runQueryTest({
    measures: [],
    dimensions: [
      'Orders.productAndCategory'
    ],
    order: [{ id: 'Orders.productAndCategory' }],
  }, [{
    orders__product_and_category: 'Potato_Groceries',
  }, {
    orders__product_and_category: 'Tomato_Groceries',
  }]));

  it('view compound dimension', async () => runQueryTest({
    measures: [],
    dimensions: [
      'OrdersView.productCategory'
    ],
    order: [{ id: 'OrdersView.productCategory' }],
  }, [{
    orders_view__product_category: 'Tomato_Groceries_Groceries',
  }, {
    orders_view__product_category: null,
  }]));

  it('segment with filter params', async () => runQueryTest({
    measures: ['Orders.count'],
    segments: [
      'Orders.potatoOnly'
    ],
    filters: [{
      member: 'Orders.productId',
      operator: 'equals',
      values: ['2'],
    }]
  }, [{
    orders__count: '1',
  }]));

  it('rolling window', async () => runQueryTest({
    measures: ['OrdersView.runningTotal']
  }, [{
    orders_view__running_total: '2',
  }]));

  it('rolling window with dimension', async () => runQueryTest({
    measures: ['OrdersView.runningTotal'],
    dimensions: ['OrdersView.productName'],
    order: [{ id: 'OrdersView.productName' }],
  }, [{
    orders_view__product_name: 'Potato',
    orders_view__running_total: '1',
  }, {
    orders_view__product_name: 'Tomato',
    orders_view__running_total: '1',
  }]));

  it('check includes are exposed in meta', async () => {
    await compiler.compile();
    const cube = metaTransformer.cubes.find(c => c.config.name === 'OrdersView');
    expect(cube.config.measures.find((({ name }) => name === 'OrdersView.count')).name).toBe('OrdersView.count');
  });

  it('orders are hidden', async () => {
    await compiler.compile();
    const cube = metaTransformer.cubes.find(c => c.config.name === 'Orders');
    expect(cube.config.measures.filter((({ isVisible }) => isVisible)).length).toBe(0);
  });

  it('split views', async () => runQueryTest({
    measures: ['OrdersView3.count'],
    dimensions: ['OrdersView3_ProductCategories.name'],
    order: [{ id: 'OrdersView3_ProductCategories.name' }],
  }, [{
    orders_view3__count: '2',
    orders_view3__product_categories__name: 'Groceries',
  }]));

  it('check drillMembers are inherited in views', async () => {
    await compiler.compile();
    const cube = metaTransformer.cubes.find(c => c.config.name === 'OrdersView');
    const countMeasure = cube.config.measures.find((m) => m.name === 'OrdersView.count');
    expect(countMeasure.drillMembers).toEqual(['OrdersView.id', 'OrdersView.ProductCategories_name']);
    expect(countMeasure.drillMembersGrouped).toEqual({
      measures: [],
      dimensions: ['OrdersView.id', 'OrdersView.ProductCategories_name']
    });
  });

  it('verify drill member inheritance functionality', async () => {
    await compiler.compile();

    // Check that the source Orders cube has drill members
    const sourceOrdersCube = metaTransformer.cubes.find(c => c.config.name === 'Orders');
    const sourceCountMeasure = sourceOrdersCube.config.measures.find((m) => m.name === 'Orders.count');
    expect(sourceCountMeasure.drillMembers).toEqual(['Orders.id', 'Orders.createdAt', 'ProductCategories.name']);

    // Check that the OrdersView cube inherits these drill members with correct naming
    const viewCube = metaTransformer.cubes.find(c => c.config.name === 'OrdersView');
    const viewCountMeasure = viewCube.config.measures.find((m) => m.name === 'OrdersView.count');

    expect(viewCountMeasure.drillMembers).toBeDefined();
    expect(Array.isArray(viewCountMeasure.drillMembers)).toBe(true);
    expect(viewCountMeasure.drillMembers.length).toBeGreaterThan(0);
    expect(viewCountMeasure.drillMembers).toContain('OrdersView.id');
    expect(viewCountMeasure.drillMembersGrouped).toBeDefined();
  });

  it('check drill member inheritance with limited includes in OrdersSimpleView', async () => {
    await compiler.compile();
    const cube = metaTransformer.cubes.find(c => c.config.name === 'OrdersSimpleView');

    if (!cube) {
      throw new Error('OrdersSimpleView not found in compiled cubes');
    }

    const countMeasure = cube.config.measures.find((m) => m.name === 'OrdersSimpleView.count');

    if (!countMeasure) {
      throw new Error('OrdersSimpleView.count measure not found');
    }

    // Check what dimensions are actually available in this limited view
    const availableDimensions = cube.config.dimensions?.map(d => d.name) || [];

    // This view only includes 'createdAt' dimension and should not include id
    expect(availableDimensions).not.toContain('OrdersSimpleView.id');
    expect(availableDimensions).toContain('OrdersSimpleView.createdAt');

    // The source measure has drillMembers: ['Orders.id', 'Orders.createdAt']
    // Both should be available in this view since we explicitly included them
    expect(countMeasure.drillMembers).toBeDefined();
    // Verify drill members are inherited and correctly transformed to use View naming
    expect(countMeasure.drillMembers).toEqual(['OrdersSimpleView.createdAt']);
    expect(countMeasure.drillMembersGrouped).toEqual({
      measures: [],
      dimensions: ['OrdersSimpleView.createdAt']
    });
  });

  it('verify drill member inheritance functionality (with transitive joins)', async () => {
    await compiler.compile();

    // Check that the OrdersView cube inherits these drill members with correct naming
    const viewCube = metaTransformer.cubes.find(c => c.config.name === 'OrdersViewDrillMembers');

    const viewCountMeasure = viewCube.config.measures.find((m) => m.name === 'OrdersViewDrillMembers.count');
    expect(viewCountMeasure.drillMembers).toBeDefined();
    expect(Array.isArray(viewCountMeasure.drillMembers)).toBe(true);
    expect(viewCountMeasure.drillMembers.length).toEqual(2);
    expect(viewCountMeasure.drillMembers).toEqual(['OrdersViewDrillMembers.createdAt', 'OrdersViewDrillMembers.name']);

    const viewCount2Measure = viewCube.config.measures.find((m) => m.name === 'OrdersViewDrillMembers.count2');
    expect(viewCount2Measure.drillMembers).toBeDefined();
    expect(Array.isArray(viewCount2Measure.drillMembers)).toBe(true);
    expect(viewCount2Measure.drillMembers.length).toEqual(1);
    expect(viewCount2Measure.drillMembers).toContain('OrdersViewDrillMembers.name');
  });

  it('verify drill member inheritance functionality (with transitive joins + prefix)', async () => {
    await compiler.compile();

    // Check that the OrdersView cube inherits these drill members with correct naming
    const viewCube = metaTransformer.cubes.find(c => c.config.name === 'OrdersViewDrillMembersWithPrefix');

    const viewCountMeasure = viewCube.config.measures.find((m) => m.name === 'OrdersViewDrillMembersWithPrefix.count');
    expect(viewCountMeasure.drillMembers).toBeDefined();
    expect(Array.isArray(viewCountMeasure.drillMembers)).toBe(true);
    expect(viewCountMeasure.drillMembers.length).toEqual(2);
    expect(viewCountMeasure.drillMembers).toEqual(['OrdersViewDrillMembersWithPrefix.createdAt', 'OrdersViewDrillMembersWithPrefix.ProductCategories_name']);

    const viewCount2Measure = viewCube.config.measures.find((m) => m.name === 'OrdersViewDrillMembersWithPrefix.ProductCategories_count2');
    expect(viewCount2Measure.drillMembers).toBeDefined();
    expect(Array.isArray(viewCount2Measure.drillMembers)).toBe(true);
    expect(viewCount2Measure.drillMembers.length).toEqual(1);
    expect(viewCount2Measure.drillMembers).toContain('OrdersViewDrillMembersWithPrefix.ProductCategories_name');
  });
});
